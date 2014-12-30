package main

import (
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
)

type Settings struct {
	Url      string
	Queue    string
	Filename string
}

func main() {
	fmt.Println("Carrot")
	args, err := getArgs()
	HandleError(err, getUsage())
	fmt.Println(args.Url)

	log.Println("Connecting to RabbitMQ at " + args.Url)
	conn, err := amqp.Dial(args.Url)
	HandleError(err, "Unable to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	HandleError(err, "Unable to open channel")
	defer ch.Close()

	log.Println("Loading file")
	data, err := ioutil.ReadFile(args.Filename)
	HandleError(err, "Unable to load file")

	log.Println("Sending file to queue " + args.Queue)
	err = ch.Publish("", args.Queue, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        data})
	HandleError(err, "Unable to publish to queue "+args.Queue)
	log.Println("Done")
}

func getArgs() (*Settings, error) {
	var url, queue, filename string
	flag.StringVar(&url, "url", "", "URL for RabbitMQ [amqp://user:pass@server:5672]")
	flag.StringVar(&queue, "q", "", "Queue name")
	flag.StringVar(&filename, "file", "", "File to send")
	flag.Parse()

	if url == "" {
		return &Settings{}, fmt.Errorf("AMQP URL is missing [amqp://user:pass@server:5672]")
	}

	if queue == "" {
		return &Settings{}, fmt.Errorf("Please specify the queue you want to publish to")
	}

	if filename == "" {
		return &Settings{}, fmt.Errorf("Please specify the file to publish")
	}

	args := &Settings{Url: url, Queue: queue, Filename: filename}
	return args, nil
}

func getUsage() string {
	return `
	carrot -file <filename> -q <queue name> -url <amqp://user:password@server:5672>

	`
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
