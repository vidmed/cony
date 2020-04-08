package cony

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/pkg/errors"

	"github.com/ethereum/go-ethereum/log"

	"github.com/streadway/amqp"
)

var (
	// ErrNoConnection is an indicator that currently there is no connection
	// available
	ErrNoConnection = errors.New("no connection available")
	// ErrStopped is an indicator that client is stopped
	ErrStopped = errors.New("client stopped")
	// ErrConnected is an indicator that client already connected
	ErrConnected = errors.New("client already connected")
	// ErrDisconnectWanted is an indicator that someone called disconnect method
	ErrDisconnectWanted = errors.New("disconnect wanted")
	// ErrDisconnectTimeout is an indicator that disconnect timed out
	ErrDisconnectTimeout = errors.New("disconnect timeout")
	// ErrFrequentDisconnect is an indicator that there are frequent disconnects
	ErrFrequentDisconnect = errors.New("too frequent disconnects")
	// ErrConnectionBlocked is an indicator that connection blocked
	ErrConnectionBlocked = errors.New("connection blocked")
	// ErrConnectionClosedByClient is an indicator that connection was closed by this library
	ErrConnectionClosedByClient = errors.New("connection closed by client")
)

const disconnectTimeout = 5 * time.Second
const disconnectFrequency = 10 * time.Second

// ClientOpt is a Client's functional option type
type ClientOpt func(*Client)

// Client is a Main AMQP client wrapper
type Client struct {
	conn atomic.Value //*amqp.Connection

	lastDisconnectMu sync.RWMutex
	lastDisonnect    *time.Time

	addrs             []string
	declarations      []Declaration
	consumers         map[*Consumer]struct{}
	publishers        map[*Publisher]struct{}
	errs              chan error
	blocking          chan amqp.Blocking
	bo                Backoffer
	attempt           int32
	l                 sync.RWMutex
	config            amqp.Config
	closeCh           chan struct{}
	closeCurConnectCh chan chan struct{}

	logger Logger
}

// Declare used to declare queues/exchanges/bindings.
// Declaration is saved and will be re-run every time Client gets connection
func (c *Client) Declare(d []Declaration) error {
	if c.Closed() {
		return ErrStopped
	}
	c.l.Lock()
	c.declarations = append(c.declarations, d...)
	c.l.Unlock()
	c.logger.Debug("Declarations added", "count", len(d))

	return c.declare()
}

func (c *Client) Redeclare() error {
	return c.declare()
}

func (c *Client) declare() error {
	ch, err := c.channel()
	if err != nil {
		return errors.Wrap(err, "can not create channel in client for declarations")
	}
	defer ch.Close()

	c.l.RLock()
	defer c.l.RUnlock()

	for _, declare := range c.declarations {
		err = declare(ch)
	}

	log.Debug("Declare amqp channel closed", "err", ch.Close())
	return err
}

// Consume used to declare consumers
func (c *Client) Consume(cons *Consumer) {
	c.l.Lock()
	c.consumers[cons] = struct{}{}
	c.l.Unlock()

	go cons.serve(c.channel)
}

func (c *Client) CancelConsumer(cons *Consumer) {
	c.deleteConsumer(cons)
	cons.stop()
}

func (c *Client) deleteConsumer(cons *Consumer) {
	c.l.Lock()
	defer c.l.Unlock()
	delete(c.consumers, cons)
}

// Publish used to declare publishers
func (c *Client) Publish(pub *Publisher) {
	c.l.Lock()
	c.publishers[pub] = struct{}{}
	c.l.Unlock()

	go pub.serve(c.channel)
}

func (c *Client) CancelPublisher(pub *Publisher) {
	c.deletePublisher(pub)
	pub.stop()
}

func (c *Client) deletePublisher(pub *Publisher) {
	c.l.Lock()
	defer c.l.Unlock()
	delete(c.publishers, pub)
}

// Errors returns AMQP connection level errors. Default buffer size is 100.
// Messages will be dropped in case if receiver can't keep up
func (c *Client) Errors() <-chan error {
	return c.errs
}

// Blocking notifies the server's TCP flow control of the Connection. Default
// buffer size is 10. Messages will be dropped in case if receiver can't keep up
func (c *Client) Blocking() <-chan amqp.Blocking {
	return c.blocking
}

// CloseCh notifies that client was closed
func (c *Client) CloseCh() <-chan struct{} {
	return c.closeCh
}

// Close shutdown the client
func (c *Client) Close() {
	if !c.Closed() {
		close(c.closeCh)
	}
}

// Canceled checks if Client is cancelled
func (c *Client) Closed() bool {
	select {
	case <-c.closeCh:
		return true
	default:
		return false
	}
}

func (c *Client) closeCurrentConnection() {
	if conn, _ := c.Connection(); conn != nil {
		err := conn.Close()
		c.logger.Warn("current connection closed", "err", err)
	}
	c.conn.Store((*amqp.Connection)(nil))
}

func (c *Client) storeCurrentConnection(conn *amqp.Connection) {
	c.conn.Store(conn)
	atomic.StoreInt32(&c.attempt, 0)

}

func (c *Client) setLastDisconnectTime() {
	c.lastDisconnectMu.Lock()
	defer c.lastDisconnectMu.Unlock()

	now := time.Now()
	c.lastDisonnect = &now
}

func (c *Client) getLastDisconnectTime() *time.Time {
	c.lastDisconnectMu.RLock()
	defer c.lastDisconnectMu.RUnlock()

	return c.lastDisonnect
}

// Disconnect tries gracefully close current connection using closeCurConnectCh channel
// This did due to the reason call closeCurrentConnection() from the one place - from guard connection goroutine
// In case of  timeout or closing client - error will bw returned
func (c *Client) Disconnect() error {
	c.logger.Info("Client Disconnect method called")
	if !c.Connected() {
		return ErrNoConnection
	}

	lastConnect := c.getLastDisconnectTime()
	if lastConnect != nil {
		if time.Since(*lastConnect) <= disconnectFrequency {
			return ErrFrequentDisconnect
		}
	}

	c.setLastDisconnectTime()
	res := make(chan struct{})
	select {
	case <-c.closeCh:
		c.logger.Info("Client Disconnect method returned due to Client stopped")
		return ErrStopped
	case <-time.After(disconnectTimeout):
		c.logger.Info("Client Disconnect method returned due to Client disconnectTimeout fired")
		return ErrDisconnectTimeout
	case c.closeCurConnectCh <- res:
		// receive stopServingCh response
		select {
		case <-c.closeCh:
			c.logger.Info("Client Disconnect method returned due to Client stopped")
			return ErrStopped
		case <-res:
			c.logger.Info("Client Disconnect method successfully disconnected connection")
		}
	}
	return nil
}

// shuffleAddrs mix list of connection addresses
func (c *Client) shuffleAddrs() []string {
	if len(c.addrs) < 2 {
		return c.addrs
	}
	shuffled := make([]string, len(c.addrs))
	copy(shuffled, c.addrs)

	rand.Seed(time.Now().UnixNano())

	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled
}

// clusterDial tries to connect to random node of the cluster
// first of all it takes shuffled list of addresses and try to connect
// if connection to any address succeed - return this connection immediately
func (c *Client) clusterDial() (*amqp.Connection, error) {
	// set default Heartbeat to 10 seconds like in original amqp.Dial
	if c.config.Heartbeat == 0 {
		c.config.Heartbeat = 1 * time.Second
	}

	var errs []string
	for _, addr := range c.shuffleAddrs() {
		conn, err := amqp.DialConfig(addr, c.config)
		if err == nil {
			c.logger.Info("amqp successfully dialed", "addr", addr)
			return conn, nil
		}

		err = errors.Wrapf(err, "amqp dial address (%q) error", addr)
		c.logger.Error(err.Error())
		errs = append(errs, fmt.Sprintf("(%q)", err.Error()))
	}
	return nil, errors.Errorf("amqp dial cluster error: ", strings.Join(errs, "; "))
}

// tries to Connect to amqp
// if connection already established - return error ErrConnected
// if not - tries to connect with some Backoff policy
// also guard connection goroutine runs to handle some connection troubles
// when something wrong happens guard connection goroutine calls c.reportErr(err)
// this will cause client channel listener wake up and launch Loop() iteration
// then established new connection
func (c *Client) Connect() error {
	var (
		err error
	)

	if c.Closed() {
		return ErrStopped
	}

	conn, _ := c.Connection()
	if conn != nil {
		return ErrConnected
	}

	if c.bo != nil {
		sleep := c.bo.Backoff(int(atomic.LoadInt32(&c.attempt)))
		c.logger.Info("sleeping ", "backoff", sleep)
		sleepTimer := time.NewTimer(sleep)
		select {
		case <-sleepTimer.C:
			sleepTimer.Stop()
		case <-c.closeCh:
			sleepTimer.Stop()
			return ErrStopped
		}

		atomic.AddInt32(&c.attempt, 1)
		c.logger.Debug("woke up")
	}

	conn, err = c.clusterDial()
	if err != nil {
		return err
	}
	c.storeCurrentConnection(conn)

	// guard connection
	go func() {
		chanErr := conn.NotifyClose(make(chan *amqp.Error, 1))
		chanBlocking := conn.NotifyBlocked(make(chan amqp.Blocking, 1))

		var (
			eventType string
			err       error
		)

		c.logger.Warn("guard connection started")

		defer func() {
			c.logger.Warn("guard connection event fired", "event", eventType, "error", err)
			// todo perhaps remove this check
			if err != ErrConnectionClosedByClient {
				c.closeCurrentConnection()
				c.reportErr(err)
			}
		}()

		// loop for guarding connection
		select {
		case <-c.closeCh:
			eventType = "clientClosed"
			err = ErrStopped
			// return from routine to launch reconnect process
			return
		case reply := <-c.closeCurConnectCh:
			eventType = "closeCurConnectCh"
			err = ErrDisconnectWanted
			close(reply)
			// return from routine to launch reconnect process
			return
		case amqpErr, ok := <-chanErr:
			eventType = "NotifyClose"
			if !ok {
				err = ErrConnectionClosedByClient
			} else {
				err = errors.New(fmt.Sprintf("amqp error from chanErr: %q.", spew.Sdump(amqpErr)))
			}
			// return from routine to launch reconnect process
			return
		case blocking := <-chanBlocking:
			select {
			case c.blocking <- blocking:
			default:
			}

			eventType = "NotifyBlocked"
			err = ErrConnectionBlocked
			// return from routine to launch reconnect process
			return
		}
	}()
	return nil
}

// Loop should be run as condition for `for` with receiving from (*Client).Errors()
//
// It will manage AMQP connection, run queue and exchange declarations, consumers.
// Will start to return false once (*Client).Close() called.
func (c *Client) Loop() bool {
	err := c.Connect()
	if err == ErrStopped {
		return false
	} else if err == ErrConnected {
		return true
	}

	if c.reportErr(err) {
		// this call will cause guard connection go-routine (if exists) wake up
		c.closeCurrentConnection()
		return true
	}

	// redeclare declarations on new connection
	if err = c.declare(); err != nil {
		err = errors.Wrap(err, "can not declare")
	}
	if c.reportErr(err) {
		c.closeCurrentConnection()
		return true
	}

	// re-run all servings because connection is reestablished
	c.l.RLock()
	defer c.l.RUnlock()

	for cons := range c.consumers {
		go cons.serve(c.channel)
	}

	for pub := range c.publishers {
		go pub.serve(c.channel)
	}

	return true
}

func (c *Client) reportErr(err error) bool {
	if err != nil {
		select {
		case c.errs <- err:
		default:
		}
		return true
	}
	return false
}

func (c *Client) channel() (*amqp.Channel, error) {
	conn, err := c.Connection()
	if err != nil {
		return nil, err
	}

	return conn.Channel()
}

func (c *Client) Connection() (*amqp.Connection, error) {
	conn, _ := c.conn.Load().(*amqp.Connection)
	if conn == nil {
		return nil, ErrNoConnection
	}

	return conn, nil
}

func (c *Client) Connected() bool {
	conn, _ := c.conn.Load().(*amqp.Connection)

	return conn != nil
}

// NewClient initializes new Client
func NewClient(opts ...ClientOpt) *Client {
	c := &Client{
		closeCh:           make(chan struct{}),
		closeCurConnectCh: make(chan chan struct{}),
		declarations:      make([]Declaration, 0),
		consumers:         make(map[*Consumer]struct{}),
		publishers:        make(map[*Publisher]struct{}),
		errs:              make(chan error, 100),
		blocking:          make(chan amqp.Blocking, 10),
		logger:            noopLogger{},
	}

	for _, o := range opts {
		o(c)
	}
	return c
}

// URL is a functional option, used in `NewClient` constructor
// default URL is amqp://guest:guest@localhost/
func URLs(addrs []string) ClientOpt {
	return func(c *Client) {
		if len(addrs) == 0 {
			addrs = []string{"amqp://guest:guest@localhost/"}
		}
		c.addrs = addrs
	}
}

// Log is a functional option, used in `NewClient` constructor
// to set the logger
func Log(l Logger) ClientOpt {
	return func(c *Client) {
		if l != nil {
			c.logger = l
		}
	}
}

// Backoff is a functional option, used to define backoff policy, used in
// `NewClient` constructor
func Backoff(bo Backoffer) ClientOpt {
	return func(c *Client) {
		c.bo = bo
	}
}

// ErrorsChan is a functional option, used to initialize error reporting channel
// in client code, maintaining control over buffer size. Default buffer size is
// 100. Messages will be dropped in case if receiver can't keep up, used in
// `NewClient` constructor
func ErrorsChan(errChan chan error) ClientOpt {
	return func(c *Client) {
		c.errs = errChan
	}
}

// BlockingChan is a functional option, used to initialize blocking reporting
// channel in client code, maintaining control over buffering, used in
// `NewClient` constructor
func BlockingChan(blockingChan chan amqp.Blocking) ClientOpt {
	return func(c *Client) {
		c.blocking = blockingChan
	}
}

// Config is a functional option, used to setup extended amqp configuration
func Config(config amqp.Config) ClientOpt {
	return func(c *Client) {
		c.config = config
	}
}
