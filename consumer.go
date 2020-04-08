package cony

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/streadway/amqp"
)

var (
	// ErrConsumerStopped is an indicator that consumer is stopped
	ErrConsumerStopped         = errors.New("consumer stopped")
	ErrDeliveriesChannelClosed = errors.New("consumer deliveries serving channel closed")
	ErrConsumerServingStopped  = errors.New("consumer serving stopped")
)

const serving = 1

// ConsumerOpt is a consumer's functional option type
type ConsumerOpt func(*Consumer)

// Consumer holds definition for AMQP consumer
type Consumer struct {
	q             *Queue
	deliveries    chan *amqp.Delivery
	errs          chan error
	qos           int
	tag           string
	autoAck       bool
	exclusive     bool
	noLocal       bool
	stopCh        chan struct{}
	stopServingCh chan chan struct{} // used to just stopping serving by client
	sync          chan struct{}      // sync channel is used to synchronize important operations
	servingFlag   *uint32
	wg            sync.WaitGroup

	logger Logger
}

// Deliveries return deliveries shipped to this consumer
// this channel never closed, even on disconnects
func (c *Consumer) Deliveries() <-chan *amqp.Delivery {
	return c.deliveries
}

// Errors returns channel with AMQP channel level errors
func (c *Consumer) Errors() <-chan error {
	return c.errs
}

// StopCh returns channel which will be closed when consumer stopped
func (c *Consumer) StopCh() <-chan struct{} {
	return c.stopCh
}

func (c *Consumer) Qos() int {
	return c.qos
}

func (c *Consumer) Tag() string {
	return c.tag
}

func (c *Consumer) AutoAck() bool {
	return c.autoAck
}

func (c *Consumer) Exclusive() bool {
	return c.exclusive
}

func (c *Consumer) NoLocal() bool {
	return c.noLocal
}

// Serving returns if this consumer is serving at the moment
func (c *Consumer) Serving() bool {
	return atomic.LoadUint32(c.servingFlag) == serving
}

func (c *Consumer) setServing() {
	atomic.StoreUint32(c.servingFlag, serving)
	c.wg.Add(1)
}

func (c *Consumer) unsetServing() {
	atomic.StoreUint32(c.servingFlag, 0)
	c.wg.Done()
}

// Cancel this consumer.
//
// This will CLOSE Deliveries() channel
func (c *Consumer) stop() {
	if !c.Stopped() {
		close(c.stopCh)
		c.wg.Wait()
		close(c.deliveries)
		c.logger.Warn("Consumer stopped")
	}
}

// Canceled checks if Consumer is cancelled
func (c *Consumer) Stopped() bool {
	select {
	case <-c.stopCh:
		return true
	default:
		return false
	}
}

func (c *Consumer) reportErr(err error) bool {
	if err != nil {
		select {
		case c.errs <- err:
		default:
		}
		return true
	}
	return false
}

func (c *Consumer) serve(chConstr channelConstrucor) {
	c.logger.Info("Consumer serve called")
	defer c.logger.Info("Consumer serve returned")
	// here we try to obtain sync lock
	// if another serve method running - stop that serving
	// this is done in purpose to run just one serving per consumer
syncLoop:
	for {
		res := make(chan struct{})
		select {
		case <-c.stopCh:
			c.logger.Info("Consumer serve sync returned due to Consumer stopped")
			return
		case <-c.sync:
			c.logger.Info("Consumer serve sync obtained")
			break syncLoop
		case c.stopServingCh <- res:
			c.logger.Info("Consumer serve stopServing sent")
			// receive stopServingCh response
			select {
			case <-c.stopCh:
				c.logger.Info("Consumer serve sync returned due to Consumer stopped")
				return
			case <-res:
			}
		}
	}

	defer func() {
		c.logger.Info("Consumer serve sync released")
		c.sync <- struct{}{}
	}()

	if c.Stopped() {
		c.reportErr(ErrConsumerStopped)
		return
	}

	c.setServing()
	defer c.unsetServing()

	ch, err := chConstr()
	if err != nil {
		c.reportErr(errors.Wrap(err, "Consumer serve error while creating amqp channel"))
		return
	}
	defer ch.Close()

	err = ch.Qos(c.qos, 0, false)
	if err != nil {
		c.reportErr(errors.Wrap(err, "Consumer serve error while setting channel Qos"))
		return
	}

	deliveries, err := ch.Consume(c.q.Name,
		c.tag,       // consumer tag
		c.autoAck,   // autoAck,
		c.exclusive, // exclusive,
		c.noLocal,   // noLocal,
		false,       // noWait,
		nil,         // args Table
	)
	if err != nil {
		c.reportErr(errors.Wrap(err, "Consumer serve error begin consuming channel"))
		return
	}

	//chanErrs := ch.NotifyClose(make(chan *amqp.Error, 1))

	for {
		// if stopCh closed return as soon as possible
		select {
		case <-c.stopCh:
			c.reportErr(ErrConsumerStopped)
			return
		default:
		}

		// if stopCh closed return as soon as possible
		select {
		case <-c.stopCh:
			c.reportErr(ErrConsumerStopped)
			return
		case notify := <-c.stopServingCh:
			c.logger.Warn("stopServing signal received")
			c.reportErr(ErrConsumerServingStopped)
			close(notify)
			return
		default:
		}

		// if stopCh closed return as soon as possible
		//select {
		//case <-c.stopCh:
		//	c.reportErr(ErrConsumerStopped)
		//	return
		//case notify := <-c.stopServingCh:
		//	c.logger.Warn("stopServing signal received")
		//	c.reportErr(ErrConsumerServingStopped)
		//	close(notify)
		//	return
		//case amqpErr := <-chanErrs:
		//	c.reportErr(errors.New(spew.Sdump(amqpErr)))
		//	return
		//default:
		//}

		select {
		case <-c.stopCh:
			c.reportErr(ErrConsumerStopped)
			return
		case notify := <-c.stopServingCh:
			c.logger.Warn("stopServing signal received")
			c.reportErr(ErrConsumerServingStopped)
			close(notify)
			return
		//case amqpErr := <-chanErrs:
		//	c.reportErr(errors.New(spew.Sdump(amqpErr)))
		//	return
		case d, ok := <-deliveries: // deliveries will be closed once channel is closed (disconnected from network)
			if !ok {
				c.reportErr(ErrDeliveriesChannelClosed)
				return
			}
			c.deliveries <- &d
		}
	}
}

// NewConsumer Consumer's constructor
func NewConsumer(q *Queue, opts ...ConsumerOpt) *Consumer {
	c := &Consumer{
		q:             q,
		deliveries:    make(chan *amqp.Delivery),
		errs:          make(chan error, 100),
		sync:          make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		stopServingCh: make(chan chan struct{}),
		servingFlag:   new(uint32),
		logger:        noopLogger{},
	}
	// init sync
	c.sync <- struct{}{}

	for _, o := range opts {
		o(c)
	}
	return c
}

// ConsumerLog is a functional option, used in `NewConsumer` constructor
// to set the logger
func ConsumerLog(l Logger) ConsumerOpt {
	return func(c *Consumer) {
		if l != nil {
			c.logger = l
		}
	}
}

// Qos on channel
func Qos(count int) ConsumerOpt {
	return func(c *Consumer) {
		c.qos = count
	}
}

// Tag the consumer
func Tag(tag string) ConsumerOpt {
	return func(c *Consumer) {
		c.tag = tag
	}
}

// AutoTag set automatically generated tag like this
//	fmt.Sprintf(QueueName+"-pid-%d@%s", os.Getpid(), os.Hostname())
func AutoTag() ConsumerOpt {
	return func(c *Consumer) {
		host, _ := os.Hostname()
		tag := fmt.Sprintf(c.q.Name+"-pid-%d@%s", os.Getpid(), host)
		Tag(tag)(c)
	}
}

// AutoAck set this consumer in AutoAck mode
func AutoAck() ConsumerOpt {
	return func(c *Consumer) {
		c.autoAck = true
	}
}

// Exclusive set this consumer in exclusive mode
func Exclusive() ConsumerOpt {
	return func(c *Consumer) {
		c.exclusive = true
	}
}

// NoLocal set this consumer in NoLocal mode.
func NoLocal() ConsumerOpt {
	return func(c *Consumer) {
		c.noLocal = true
	}
}
