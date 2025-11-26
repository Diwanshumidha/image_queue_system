package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	envutil "diwanshuMidha/common/lib/env"

	queue "diwanshuMidha/queue"
)

func main() {
	rabbitMqUri := envutil.Get("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/")
	log.Println("Starting Transcode Worker...")

	rabbit, err := queue.NewRabbitClient(rabbitMqUri)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ and declare topology:", err)
	}
	defer rabbit.Close()

	msgs, err := rabbit.Consume(queue.TranscodeQueue)
	if err != nil {
		log.Fatal("Failed to register consumer:", err)
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf(" [*] Transcode Worker is listening on queue: %s. To exit press CTRL+C", queue.TranscodeQueue)

	go func() {
		for d := range msgs {
			log.Printf(" [x] Received job. Attempting to process body size: %d bytes", len(d.Body))

			var event queue.ImageEvent

			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf(" [!] Error unmarshaling JSON (Poison message): %v. Rejecting message.", err)
				d.Reject(false)
				continue
			}

			log.Printf(" [~] Processing S3 object: s3://%s/%s", event.Bucket, event.Key)

			time.Sleep(5 * time.Second)

			if err == nil {
				d.Ack(false)
				log.Printf(" [✓] Job completed and acknowledged for %s", event.Key)
			} else {
				log.Printf(" [x] Processing failed for %s: %v. Requeuing...", event.Key, err)
				d.Nack(false, true)
			}
		}
	}()

	<-stopChan
	log.Println(" [*] Worker shutting down gracefully...")
}
