package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/streadway/amqp"
)

func failOnErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func parseMessage(m []byte) (int, int) {
	split := strings.Split(string(m), ":")

	id, err := strconv.Atoi(split[0])
	failOnErr(err, "ID not int")
	value, err := strconv.Atoi(split[1])
	failOnErr(err, "Value not int")

	return id, value
}

func consumeMessage(pg *pgxpool.Pool, d amqp.Delivery) {
	id, value := parseMessage(d.Body)

	tag, err := pg.Exec(context.Background(), `
INSERT INTO items (
	id, value
) VALUES (
	$1, $2
)
ON CONFLICT (id)
	DO UPDATE SET
		value = EXCLUDED.value
	`, id, value)
	failOnErr(err, "Insert failed")
	if string(tag) != "INSERT 0 1" {
		fmt.Println("Different tag", tag)
	}

	d.Ack(false)
}

func consumer(pg *pgxpool.Pool, msgs <-chan amqp.Delivery) {
	for d := range msgs {
		consumeMessage(pg, d)
	}
}

func makePool() *pgxpool.Pool {
	host := os.Getenv("PG_HOST")
	port := os.Getenv("PG_PORT")
	user := os.Getenv("PG_USER")
	password := os.Getenv("PG_PASSWORD")
	dbName := os.Getenv("PG_DATABASE")
	poolMaxConns := os.Getenv("PG_POOL_MAX_CONNS")

	connString := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s pool_max_conns=%s",
		host, port, user, password, dbName, poolMaxConns,
	)

	pool, err := pgxpool.Connect(context.Background(), connString)
	failOnErr(err, "Failed to create Postgres Pool")

	return pool
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
	pool := makePool()
	defer pool.Close()

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

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnErr(err, "Failed to register a consumer")

	for i := 0; i < int(pool.Config().MaxConns); i++ {
		go consumer(pool, msgs)
	}

	for {
		fmt.Println("Current connections:", pool.Stat().AcquiredConns())
		time.Sleep(time.Second)
	}
}
