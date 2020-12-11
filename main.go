package main

import (
	"bytes"
	"context"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cheshir/go-mq"
	"github.com/makasim/amqpextra"
	"github.com/makasim/amqpextra/consumer"
	"github.com/makasim/amqpextra/publisher"
	"github.com/streadway/amqp"
)

const rabbitmqURI = "amqp://guest:guest@127.0.0.1:5672"

func main() {
	seedRand()

	lib := ""
	action := ""

	switch {
	case len(os.Args) == 2:
		lib = os.Args[1]
	case len(os.Args) == 3:
		lib = os.Args[1]
		action = os.Args[2]
	default:
		fmt.Printf("Usage: %s <amqp|amqpextra|gomq> [producer|consumer]\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	switch lib {
	case "amqp":
		streadwayamqp(action)
	case "amqpextra":
		makasimamqpextra(action)
	case "gomq":
		gomq(action)
	default:
		fmt.Printf("Usage: %s <amqp|amqpextra|gomq> [producer|consumer]\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}
}

func streadwayamqp(action string) {
	switch action {
	case "consumer":
		streadwayamqp_consume()
	default:
		go streadwayamqp_consume()
		select {}
	}
}
func makasimamqpextra(action string) {
	switch action {
	case "consumer":
		makasimamqpextra_consume()
	case "producer":
		makasimamqpextra_produce()
	default:
		go makasimamqpextra_consume()
		go makasimamqpextra_produce()
		select {}
	}
}
func gomq(action string) {
	switch action {
	case "consumer":
		gomq_consume()
	default:
		go gomq_consume()
		select {}
	}
}

func streadwayamqp_consume() {
	conn, err := amqp.Dial(rabbitmqURI)
	if err != nil {
		panic(err)
	}

	c, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	err = c.ExchangeDeclare(
		"messages", // name of the exchange
		"topic",    // type
		true,       // durable
		false,      // delete when complete
		false,      // internal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		panic(err)
	}

	d, err := buildConsumerInfra(c)

	for {
		select {
		case msg, ok := <-d:
			if !ok {
				for {
					d, err = buildConsumerInfra(c)
					if err != nil {
						fmt.Printf("buildConsumerInfra: %s\n", err)
						conn, err = amqp.Dial(rabbitmqURI)
						if err != nil {
							fmt.Printf("amqpDial: %s\n", err)
							time.Sleep(time.Second * 5)
							continue
						}
						fmt.Printf("CONNECTED\n")
						break
					}
					time.Sleep(time.Second * 5)
				}
				c, err = conn.Channel()
				if err != nil {
					panic(err)
				}
				d, err = buildConsumerInfra(c)
				if err != nil {
					panic(err)
				}
				continue
			}

			fmt.Printf("%s\n", msg.Body)
		}
	}
}

func buildConsumerInfra(c *amqp.Channel) (<-chan amqp.Delivery, error) {
	queue, err := c.QueueDeclare(
		"",    // name of the queue
		false, // durable
		true,  // delete when unused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	if err = c.QueueBind(
		queue.Name, // name of the queue
		"update.*", // bindingKey
		"messages", // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, err
	}

	d, err := c.Consume(
		queue.Name, // name
		"",         // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func makasimamqpextra_consume() {
	conn, err := amqpextra.NewDialer(amqpextra.WithURL(rabbitmqURI))
	if err != nil {
		panic(err)
	}

	h := consumer.HandlerFunc(func(ctx context.Context, msg amqp.Delivery) interface{} {
		fmt.Printf("%d: %s\n", time.Now().Unix(), msg.Body)

		if rand.Intn(100)%10 == 0 {
			msg.Reject(true)
		} else {
			msg.Ack(false)
		}

		return nil
	})

	c, _ := amqpextra.NewConsumer(
		conn.ConnectionCh(),
		consumer.WithQueue(""),
		consumer.WithExchange("messages", "update.*"),
		consumer.WithHandler(h),
	)

	defer c.Close()
	<-c.NotifyClosed()
}

func makasimamqpextra_produce() {
	conn, err := amqpextra.NewDialer(amqpextra.WithURL(rabbitmqURI))
	if err != nil {
		panic(err)
	}

	p, err := amqpextra.NewPublisher(conn.ConnectionCh())
	if err != nil {
		log.Fatal(err)
	}

	for {
		err := p.Publish(publisher.Message{
			Key:      "update.test",
			Exchange: "messages",
			Publishing: amqp.Publishing{
				Body: []byte(fmt.Sprintf("%d", time.Now().Unix())),
			},
		})
		if err != nil {
			fmt.Printf("PRODUCER: %s", err)
			break
		}

		time.Sleep(time.Second)
	}

	defer p.Close()
	<-p.NotifyClosed()
}

func gomq_consume() {
	config := mq.Config{
		DSN:            rabbitmqURI,
		ReconnectDelay: time.Second * 5,
		TestMode:       false,
		Exchanges: mq.Exchanges{mq.ExchangeConfig{
			Name:    "messages",
			Type:    "topic",
			Options: nil,
		}},
		Queues: mq.Queues{mq.QueueConfig{
			Exchange:       "messages",
			Name:           "foo",
			RoutingKey:     "update.*",
			BindingOptions: nil,
			Options: mq.Options{
				"auto_delete": true,
			},
		}},
		Producers: nil,
		Consumers: mq.Consumers{mq.ConsumerConfig{
			Name:          "f",
			Queue:         "foo",
			Workers:       0,
			Options:       nil,
			PrefetchCount: 0,
			PrefetchSize:  0,
		}},
	}
	queue, err := mq.New(config)
	if err != nil {
		panic(err)
	}

	go handleMQErrors(queue.Error())

	c, err := queue.Consumer("f")
	if err != nil {
		panic(err)
	}

	c.Consume(func(m mq.Message) {
		fmt.Printf("%s\n", m.Body())
		err := m.Ack(false)
		if err != nil {
			panic(err)
		}
	})

	select {}
}

func handleMQErrors(errors <-chan error) {
	for err := range errors {
		log.Println(err)
	}
}

func seedRand() {
	b := make([]byte, 8)

	_, err := crypto_rand.Read(b)
	if err != nil {
		panic(err)
	}
	bBuf := bytes.NewReader(b)

	var seed int64
	err = binary.Read(bBuf, binary.BigEndian, &seed)
	if err != nil {
		panic(err)
	}

	rand.Seed(seed)
}
