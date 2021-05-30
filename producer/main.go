package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/streadway/amqp"
)

func failOnErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func generateMessage() []byte {
	id := rand.Intn(50000)
	value := rand.Int()

	return []byte(fmt.Sprintf("%d:%d", id, value))
}

func scaleRange(value, min, max float64) float64 {
	return (value - -1)/(1 - -1)*(max-min) + min
}

func sineSleep(start time.Time, period int64, min, max float64) time.Duration {
	delta := time.Since(start).Microseconds()
	value := math.Sin(float64(delta%period) / float64(period) * math.Pi * 2)

	scaledValue := 1.0 / scaleRange(value, min, max)

	return time.Duration(scaledValue * float64(time.Second))
}

func timeLogger(start time.Time, period int64, min, max float64) {
	for {
		delta := time.Since(start).Microseconds()
		value := math.Sin(float64(delta%period) / float64(period) * math.Pi * 2)
		fmt.Println(scaleRange(value, min, max), "messages per second")

		time.Sleep(5 * time.Second)
	}
}

func producer(ch *amqp.Channel, q amqp.Queue) {
	period := (10 * time.Minute).Microseconds()
	sineMin := 300.0
	sineMax := 5000.0

	start := time.Now()
	go timeLogger(start, period, sineMin, sineMax)

	for {
		s := time.Now()
		err := ch.Publish(
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         generateMessage(),
			},
		)
		failOnErr(err, "Failed to publish message")

		if true {
			time.Sleep(sineSleep(start, period, sineMin, sineMax) - time.Since(s))
		}
	}
}

func rabbitConnect() *amqp.Connection {
	host := os.Getenv("MQ_HOST")
	port := os.Getenv("MQ_PORT")

	connString := fmt.Sprintf("amqp://%s:%s", host, port)

	conn, err := amqp.Dial(connString)
	failOnErr(err, "Failed to connect to RabbitMQ")

	return conn
}

func main() {
	rand.Seed(time.Now().UnixNano())

	conn := rabbitConnect()
	defer conn.Close()

	channel, err := conn.Channel()
	failOnErr(err, "Failed to open channel")
	defer channel.Close()

	q, err := channel.QueueDeclare(
		os.Getenv("MQ_QUEUE"),
		true,
		false,
		false,
		false,
		nil,
	)
	failOnErr(err, "Failed to declare queue")

	producer(channel, q)
}
