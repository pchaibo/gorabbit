package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	amqb "github.com/rabbitmq/amqp091-go"
)

func data_err(err error, msc string) {
	if err != nil {
		fmt.Printf("%s :%s", msc, err.Error())
	}

}
func main() {
	Send()
	//time.Sleep(10 * time.Microsecond)
	//Red()

}

func Red() {
	conent, err := amqb.Dial("amqp://guest:guest@localhost:5672/")
	data_err(err, "conet errr")
	defer conent.Close()

	ch, cherr := conent.Channel()
	data_err(cherr, "Channel errr")
	defer ch.Close()

	//
	q, qerr := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	data_err(qerr, "q errr")
	//
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	data_err(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}

func Send() {
	conent, err := amqb.Dial("amqp://guest:guest@localhost:5672/")
	data_err(err, "conet errr")
	defer conent.Close()

	ch, cherr := conent.Channel()
	data_err(cherr, "Channel errr")
	defer ch.Close()

	//
	q, qerr := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	data_err(qerr, "q errr")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Printf("binge time:%v\n", time.Now())
	//
	for i := 0; i < 10000; i++ {
		time.Sleep(2 * time.Second)
		d_time := time.Now()
		body := fmt.Sprintf("Hello World! :%v ", d_time)
		hash := md5.Sum([]byte(body))
		md5str := hex.EncodeToString(hash[:])

		err = ch.PublishWithContext(ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqb.Publishing{
				ContentType: "text/plain",
				Body:        []byte(md5str),
			})
		data_err(err, "Failed to publish a message")
		//log.Printf(" [x] Sent %s\n", body)
	}
	fmt.Printf("end time:%v\n", time.Now())

}
