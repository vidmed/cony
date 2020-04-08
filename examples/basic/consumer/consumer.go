package main

import (
	"flag"
	"fmt"

	"github.com/ethereum/go-ethereum/log"

	"github.com/vidmed/cony"
)

var url = flag.String("url", "amqp://guest:guest@localhost:5672/", "amqp url")

func showUsageAndStatus() {
	fmt.Printf("Consumer is running\n\n")
	fmt.Println("Flags:")
	flag.PrintDefaults()
	fmt.Printf("\n\n")
}

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StdoutHandler))
	flag.Parse()

	showUsageAndStatus()

	// Construct new client with the flag url
	// and default backoff policy
	cli := cony.NewClient(
		cony.URL(*url),
		cony.Backoff(cony.DefaultBackoff),
		cony.Log(log.New("component", "client").(cony.Logger)),
	)
	err := cli.Connect()
	if err != nil {
		log.Crit(err.Error())
	}

	// Declarations
	// The queue name will be supplied by the AMQP server
	que := &cony.Queue{
		AutoDelete: true,
		Name:       "myQueue",
	}
	exc := &cony.Exchange{
		Name:       "myExc",
		Kind:       "topic",
		AutoDelete: true,
	}
	bnd := &cony.Binding{
		Queue:    que,
		Exchange: exc,
		Key:      "pubSub",
	}

	err = cli.Declare([]cony.Declaration{
		cony.DeclareQueue(que),
		cony.DeclareExchange(exc),
		cony.DeclareBinding(bnd),
	})
	if err != nil {
		log.Crit(err.Error())
	}

	// Declare and register a consumer
	cns := cony.NewConsumer(
		que,
		cony.AutoAck(), // Auto sign the deliveries
		cony.Qos(4),
		cony.ConsumerLog(log.New("component", "consumer").(cony.Logger)),
	)
	cli.Consume(cns)
	for cli.Loop() {
		select {
		case msg := <-cns.Deliveries():
			log.Info("Received message:", "body", string(msg.Body))
			// If when we built the consumer we didn't use
			// the "cony.AutoAck()" option this is where we'd
			// have to call the "amqp.Deliveries" methods "Ack",
			// "Nack", "Reject"
			//
			// msg.Ack(false)
			// msg.Nack(false)
			// msg.Reject(false)
		case err := <-cns.Errors():
			log.Error("Consumer error. Reconsume...", "err", err)
			if cli.Connected() {
				log.Info("Redeclare", "err", cli.Redeclare())
				cli.Consume(cns)
			}
		case err := <-cli.Errors():
			log.Error("Client error.", "err", err)
		}
	}
}
