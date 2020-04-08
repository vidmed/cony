package cony

import (
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/davecgh/go-spew/spew"

	"github.com/streadway/amqp"
)

const defaultConfirmTimeout = 2 * time.Second

var (
	// ErrPublisherStopped is an indicator that publisher is stopped
	// could be returned from Write() and Publish() methods
	ErrPublisherStopped        = errors.New("publisher stopped")
	ErrConfirmTimeOut          = errors.New("confirm timed out")
	ErrPublishTimeOut          = errors.New("publish timed out")
	ErrPublishingNotConfirmed  = errors.New("publishing NOT confirmed")
	ErrPublisherServingStopped = errors.New("publisher serving stopped")
)

// PublisherOpt is a functional option type for Publisher
type PublisherOpt func(*Publisher)

type publishMaybeErr struct {
	pub      *amqp.Publishing
	err      chan error
	key      string
	exchange string
}

// Publisher hold definition for AMQP publishing
type Publisher struct {
	exchange       string
	key            string
	tmpl           amqp.Publishing
	pubChan        chan *publishMaybeErr
	errs           chan error
	stopCh         chan struct{}
	servingFlag    *uint32
	stopServingCh  chan chan struct{} // used to just stopping serving by client
	sync           chan struct{}      // sync channel is used to synchronize important operations
	confirmTimeout time.Duration

	logger Logger
}

// Errors returns channel with AMQP channel level errors
func (p *Publisher) Errors() <-chan error {
	return p.errs
}

// StopCh returns channel which will be closed when publisher stopped
func (p *Publisher) StopCh() <-chan struct{} {
	return p.stopCh
}

func (p *Publisher) Exchange() string {
	return p.exchange
}

func (p *Publisher) RoutingKey() string {
	return p.key
}

func (p *Publisher) ConfirmTimeout() time.Duration {
	return p.confirmTimeout
}

// Serving returns if this publisher is serving at the moment
func (p *Publisher) Serving() bool {
	return atomic.LoadUint32(p.servingFlag) == serving
}

func (p *Publisher) setServing() {
	atomic.StoreUint32(p.servingFlag, serving)
}

func (p *Publisher) unsetServing() {
	atomic.StoreUint32(p.servingFlag, 0)
}

// Cancel this publisher.
//
// This will CLOSE Deliveries() channel
func (p *Publisher) stop() {
	if !p.Stopped() {
		close(p.stopCh)
		p.logger.Warn("Publisher stopped")
	}
}

// Canceled checks if Consumer is cancelled
func (p *Publisher) Stopped() bool {
	select {
	case <-p.stopCh:
		return true
	default:
		return false
	}
}

func (p *Publisher) reportErr(err error) bool {
	if err != nil {
		select {
		case p.errs <- err:
		default:
		}
		return true
	}
	return false
}

// Template will be used, input buffer will be added as Publishing.Body.
// return int will always be len(b)
//
// Implements io.Writer
//
// WARNING: this is blocking call, it will not return until connection is
// available. The only way to stop it is to use Cancel() method.
func (p *Publisher) Write(b []byte, timeout time.Duration) (int, error) {
	pub := p.tmpl
	pub.Body = b
	return len(b), p.Publish(&pub, timeout)
}

// PublishWithRoutingKey used to publish custom amqp.Publishing and routing key
//
// WARNING: this is blocking call, it will not return until connection is
// available. The only way to stop it is to use Cancel() method.
func (p *Publisher) PublishWithParams(pub *amqp.Publishing, exchange string, key string, timeout time.Duration) error {
	reqRepl := &publishMaybeErr{
		pub:      pub,
		err:      make(chan error),
		exchange: exchange,
		key:      key,
	}

	// wait for publishing
	waitTimer := time.NewTimer(timeout)
	defer waitTimer.Stop()

	select {
	case <-p.stopCh:
		// received stop signal
		return ErrPublisherStopped
	case p.pubChan <- reqRepl:
	case <-waitTimer.C:
		// received timeout signal
		return ErrPublishTimeOut
	}
	p.logger.Debug("Publishing published to internal channel")
	err := <-reqRepl.err
	p.logger.Debug("Publishing result ", "err ", err)
	return err
}

// Publish used to publish custom amqp.Publishing
//
// WARNING: this is blocking call, it will not return until connection is
// available. The only way to stop it is to use Cancel() method.
func (p *Publisher) Publish(pub *amqp.Publishing, timeout time.Duration) error {
	return p.PublishWithParams(pub, p.exchange, p.key, timeout)
}

func (p *Publisher) serve(chConstr channelConstrucor) {
	p.logger.Info("Publisher serve called")
	defer p.logger.Info("Publisher serve returned")
	// here we try to obtain sync lock
	// if another serve method running - stop that serving
	// this is done in purpose to run just one serving per Publisher
syncLoop:
	for {
		res := make(chan struct{})
		select {
		case <-p.stopCh:
			p.logger.Info("Publisher serve sync returned due to Publisher stopped")
			return
		case <-p.sync:
			p.logger.Info("Publisher serve sync obtained")
			break syncLoop
		case p.stopServingCh <- res:
			p.logger.Info("Publisher serve stopServing sent")
			// receive stopServingCh response
			select {
			case <-p.stopCh:
				p.logger.Info("Publisher serve sync returned due to Publisher stopped")
				return
			case <-res:
			}
		}
	}

	defer func() {
		p.logger.Info("Publisher serve sync released")
		p.sync <- struct{}{}
	}()

	if p.Stopped() {
		p.reportErr(ErrPublisherStopped)
		return
	}

	p.setServing()
	defer p.unsetServing()

	ch, err := chConstr()
	if err != nil {
		p.reportErr(errors.Wrap(err, "Publisher serve error while creating amqp channel"))
		return
	}
	defer ch.Close()

	err = ch.Confirm(false)
	if err != nil {
		p.reportErr(errors.Wrap(err, "Publisher serve error while enabling amqp channel confirmation"))
		return
	}

	chanConfirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	chanErrs := ch.NotifyClose(make(chan *amqp.Error, 1))

	for {
		// to exit as soon as possible
		select {
		case <-p.stopCh:
			p.reportErr(ErrPublisherStopped)
			return
		default:
		}

		// to exit or receive stopServing as soon as possible
		select {
		case <-p.stopCh:
			p.reportErr(ErrPublisherStopped)
			return
		case notify := <-p.stopServingCh:
			p.logger.Warn("stopServing signal received")
			p.reportErr(ErrPublisherServingStopped)
			close(notify)
			return
		default:
		}

		// to exit or receive stopServing or receive NotifyClose as soon as possible
		select {
		case <-p.stopCh:
			p.reportErr(ErrPublisherStopped)
			return
		case notify := <-p.stopServingCh:
			p.logger.Warn("stopServing signal received")
			p.reportErr(ErrPublisherServingStopped)
			close(notify)
			return
		case amqpErr := <-chanErrs:
			p.reportErr(errors.New(spew.Sdump(amqpErr)))
			return
		default:

		}

		// main select
		select {
		case <-p.stopCh:
			p.reportErr(ErrPublisherStopped)
			return
		case notify := <-p.stopServingCh:
			p.logger.Warn("stopServing signal received")
			p.reportErr(ErrPublisherServingStopped)
			close(notify)
			return
		case amqpErr := <-chanErrs:
			p.reportErr(errors.New(spew.Sdump(amqpErr)))
			return
		case envelop := <-p.pubChan:
			now := time.Now()
			err := ch.Publish(
				envelop.exchange, // exchange
				envelop.key,      // key
				false,            // mandatory
				false,            // immediate
				*envelop.pub,     // msg amqp.Publishing
			)
			// if some error happened return from this serving
			if err != nil {
				err = errors.Wrap(err, "error occurred while publising")
				envelop.err <- err
				p.reportErr(err)
				return
			}
			p.logger.Info("Publishing successfully published to amqp", "(publish took)", time.Since(now))

			// wait for confirm with timeout
			waitTimer := time.NewTimer(p.confirmTimeout)
			now = time.Now()
			select {
			case <-waitTimer.C:
				p.logger.Info("Publishing confirm timed out", "(confirm took)", time.Since(now))
				waitTimer.Stop()

				err = ErrConfirmTimeOut
				envelop.err <- err
				p.reportErr(err)
				return

			case confirm := <-chanConfirms:
				waitTimer.Stop()

				if confirm.Ack {
					p.logger.Info("Publishing successfully confirmed", "(confirm took)", time.Since(now), "deliverTag", confirm.DeliveryTag)
					envelop.err <- nil // one way to continue
				} else {
					p.logger.Info("Publishing NOT confirmed", "(confirm took)", time.Since(now), "deliverTag", confirm.DeliveryTag)
					err = ErrPublishingNotConfirmed
					envelop.err <- err
					p.reportErr(err)
					return
				}
			}
		}
	}
}

// NewPublisher is a Publisher constructor
func NewPublisher(exchange string, key string, opts ...PublisherOpt) *Publisher {
	p := &Publisher{
		exchange:       exchange,
		key:            key,
		pubChan:        make(chan *publishMaybeErr),
		errs:           make(chan error, 100),
		sync:           make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		stopServingCh:  make(chan chan struct{}),
		servingFlag:    new(uint32),
		logger:         noopLogger{},
		confirmTimeout: defaultConfirmTimeout,
	}
	// init sync
	p.sync <- struct{}{}

	for _, o := range opts {
		o(p)
	}
	return p
}

// PublisherLog is a functional option, used in `NewPublisher` constructor
// to set the logger
func PublisherLog(l Logger) PublisherOpt {
	return func(p *Publisher) {
		if l != nil {
			p.logger = l
		}
	}
}

// PublisherConfirmTimeout is a functional option, used in `NewPublisher` constructor
// to set the confirm timeout
func PublisherConfirmTimeout(t time.Duration) PublisherOpt {
	return func(p *Publisher) {
		if t > 0 {
			p.confirmTimeout = t
		}
	}
}

// PublishingTemplate Publisher's functional option. Provide template
// amqp.Publishing and save typing.
func PublishingTemplate(t amqp.Publishing) PublisherOpt {
	return func(p *Publisher) {
		p.tmpl = t
	}
}
