package main

import (
	"flag"
	"fmt"

	"github.com/ethereum/go-ethereum/log"

	"time"

	"github.com/streadway/amqp"
	"github.com/vidmed/cony"
)

var url = flag.String("url", "amqp://guest:guest@localhost:5672/", "amqp url")
var body = flag.String("body", "Hello world!", "what should be sent")

func showUsageAndStatus() {
	fmt.Printf("Producer is running\n\n")
	fmt.Println("Flags:")
	flag.PrintDefaults()
	fmt.Printf("\n\n")
	fmt.Println("Publishing:")
	fmt.Printf("Body: %q\n", *body)
	fmt.Printf("\n\n")
}

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StdoutHandler))
	flag.Parse()

	showUsageAndStatus()

	// Construct new client with the flag url
	// and default backoff policy
	cli := cony.NewClient(
		cony.URLs([]string{*url}),
		cony.Backoff(cony.DefaultBackoff),
		cony.Log(log.New("component", "client").(cony.Logger)),
	)
	err := cli.Connect()
	if err != nil {
		log.Crit(err.Error())
	}

	// Declare the exchange we'll be using
	exc := &cony.Exchange{
		Name:       "myExc",
		Kind:       "topic",
		AutoDelete: true,
	}

	err = cli.Declare([]cony.Declaration{
		cony.DeclareExchange(exc),
	})
	if err != nil {
		log.Crit(err.Error())
	}

	// Declare and register a publisher
	// with the cony client
	pbl := cony.NewPublisher(
		exc.Name,
		"pubSub",
		cony.PublisherLog(log.New("component", "publisher").(cony.Logger)),
		cony.PublisherConfirmTimeout(2*time.Second),
	)
	cli.Publish(pbl)
	// Launch a go routine and publish a message.
	// "Publish" is a blocking method this is why it
	// needs to be called in its own go routine.
	//

	eb := cony.NewErrorBatch()

	//downCommand := exec.Command("ifconfig", "enp2s0", "down")
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)

		i := 0
		for {
			select {
			case <-ticker.C:
				log.Info("Client publishing...", "num", i)
				err := pbl.Publish(&amqp.Publishing{
					Body:         []byte(fmt.Sprintf("%d", i)),
					DeliveryMode: amqp.Persistent,
				}, 1*time.Second)
				if err != nil {
					eb.Add(err)
					log.Error("Client publish error", "eb", eb, "err", err)
				} else {
					log.Info(eb.String())
					eb.Reset()
					log.Info("Client published", "num", i)
				}
				//if i == 10 {
				//	if err = cli.Disconnect(); err != nil {
				//		log.Error(err.Error())
				//	}
				//	if err = cli.Disconnect(); err != nil {
				//		panic(err.Error())
				//	}
				//}
			}
			i++
		}
	}()

	go func() {
		defer log.Warn("client error listener terminated")

		for cli.Loop() {
			// try to exit ASAP
			select {
			case <-cli.CloseCh():
				log.Warn("client closed")
				return
			default:
			}

			select {
			case <-cli.CloseCh():
				log.Warn("client closed")
				return
			case err := <-cli.Errors():
				if err == cony.ErrStopped {
					log.Warn("client closed")
					return
				}
				log.Error("client error", "err", err)
			case blocked := <-cli.Blocking():
				log.Warn("client is blocked", "reason", blocked.Reason)
			}
		}
	}()

	errs := cony.NewErrorBatch()
	for !cli.Closed() {
		select {
		case err := <-pbl.Errors():
			if err == cony.ErrPublisherStopped {
				log.Warn("Publisher error stopped.")
				return
			}
			if err == cony.ErrPublisherServingStopped {
				log.Warn("Publisher error serving stopped received.", "err", err, "errSnaphot", errs)
				errs.Reset()
				continue
			}

			if cli.Connected() {
				// there are some recent errors - try to disconnect client and reset errors
				if errs.Len() > 1 {
					log.Error("Publisher error. Rabbit client connected. Recent errors > 1. Disconnect client...", "err", err, "errSnaphot", errs)
					if discErr := cli.Disconnect(); discErr != nil {
						log.Error("Publisher error. Rabbit client connected. Recent errors > 1. Disconnect client error", "discErr", discErr, "errSnaphot", errs, "err", err)
					}
					errs.Reset()
				} else { // there are NO recent errors - redeclare publisher (amqp channel recreate)
					log.Error("Publisher error. Rabbit client connected. Recent errors <= 1. Recreate publisher...", "errSnaphot", errs, "err", err)
					errs.Add(err)
					cli.Publish(pbl)
				}
			} else {
				log.Error("Publisher error. Rabbit client NOT connected. Wait for connection...", "err", err, "errSnaphot", errs)
				// if there is no connection  - new connection will be established - so reset errors
				errs.Reset()
			}
		case <-pbl.StopCh():
			log.Warn("Publisher error stopped.")
			return
		}
	}
}
