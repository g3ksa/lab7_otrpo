package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/streadway/amqp"
	"golang.org/x/net/html"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Error loading .env file")
	}
}

func extractAndPublishLinks(url string, ch *amqp.Channel, queueName string) {
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching URL %s: %s", url, err)
		return
	}
	defer resp.Body.Close()

	doc, err := html.Parse(resp.Body)
	if err != nil {
		log.Printf("Error parsing HTML for URL %s: %s", url, err)
		return
	}

	var traverse func(*html.Node)
	traverse = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					link := attr.Val
					log.Printf("Found link: %s", link)

					err := ch.Publish(
						"",
						queueName,
						false,
						false,
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        []byte(link),
						})
					if err != nil {
						log.Printf("Failed to publish link %s: %s", link, err)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			traverse(c)
		}
	}
	traverse(doc)
}

func main() {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL not set")
	}

	conn, err := amqp.Dial(rabbitURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"links",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	done := make(chan bool)

	idleTimeout := time.NewTimer(30 * time.Second)

	go func() {
		for {
			select {
			case msg := <-msgs:
				idleTimeout.Reset(30 * time.Second)
				extractAndPublishLinks(string(msg.Body), ch, q.Name)
			case <-idleTimeout.C:
				log.Println("No messages received in 30 seconds. Exiting...")
				done <- true
				return
			case <-stopChan:
				log.Println("Received shutdown signal. Exiting...")
				done <- true
				return
			}
		}
	}()

	<-done
	log.Println("Consumer stopped")
}
