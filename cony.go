// Package cony is a high-level wrapper around http://github.com/streadway/amqp library,
// for working declaratively with AMQP. Cony will manage AMQP
// connect/reconnect to AMQP broker, along with recovery of consumers.
package cony

import (
	"sync"

	"github.com/streadway/amqp"
)

// Queue hold definition of AMQP queue
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table

	l sync.Mutex
}

// Exchange hold definition of AMQP exchange
type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

// Binding used to declare binding between AMQP Queue and AMQP Exchange
type Binding struct {
	Queue    *Queue
	Exchange *Exchange
	Key      string
	NoWait   bool
	Args     amqp.Table
}

type channelConstrucor func() (*amqp.Channel, error)
