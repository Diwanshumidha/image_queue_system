package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func NewRabbitClient(amqpURI string) (*RabbitClient, error) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	client := &RabbitClient{conn: conn, ch: ch}

	// Declare the topology
	if err := client.declareTopology(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to declare RabbitMQ topology: %w", err)
	}

	log.Println("RabbitMQ connection and topology established successfully.")
	return client, nil
}

func (c *RabbitClient) declareTopology() error {
	err := c.ch.ExchangeDeclare(
		ExchangeName,
		ExchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	_, err = c.ch.QueueDeclare(
		DeadLetterQueue,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare DLQ: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    "", // Use default exchange for DLQ routing
		"x-dead-letter-routing-key": DeadLetterQueue,
	}

	_, err = c.ch.QueueDeclare(MetadataQueue, true, false, false, false, args)
	if err != nil {
		return fmt.Errorf("failed to declare metadata queue: %w", err)
	}

	_, err = c.ch.QueueDeclare(TranscodeQueue, true, false, false, false, args)
	if err != nil {
		return fmt.Errorf("failed to declare transcode queue: %w", err)
	}

	if err = c.ch.QueueBind(MetadataQueue, BindingKey, ExchangeName, false, nil); err != nil {
		return fmt.Errorf("failed to bind metadata queue: %w", err)
	}
	if err = c.ch.QueueBind(TranscodeQueue, BindingKey, ExchangeName, false, nil); err != nil {
		return fmt.Errorf("failed to bind transcode queue: %w", err)
	}

	return nil
}

func (c *RabbitClient) PublishEvent(event ImageEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal image event: %w", err)
	}

	err = c.ch.Publish(
		ExchangeName,
		RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf(" [x] Published S3 event: %s", string(body))
	return nil
}

func (c *RabbitClient) Consume(queueName string) (<-chan amqp.Delivery, error) {
	switch queueName {
	case TranscodeQueue:
		c.ch.Qos(1, 0, false)
	case MetadataQueue:
		c.ch.Qos(5, 0, false)
	default:
		return nil, fmt.Errorf("unknown queue: %s", queueName)
	}

	msgs, err := c.ch.Consume(
		queueName,
		"",    // consumer tag (empty string generates unique tag)
		false, // auto-ack must be false for reliability
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf("Consumer registered for queue: %s", queueName)
	return msgs, nil
}

func (c *RabbitClient) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
