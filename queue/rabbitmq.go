package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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

	_, err = c.ch.QueueDeclare(TranscodeQueue, true, false, false, false, args)
	if err != nil {
		return fmt.Errorf("failed to declare transcode queue: %w", err)
	}

	if err = c.ch.QueueBind(TranscodeQueue, BindingKey, ExchangeName, false, nil); err != nil {
		return fmt.Errorf("failed to bind transcode queue: %w", err)
	}

	retryQueueArgs := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": TranscodeQueue,
		"x-message-ttl":             int32(5000), // 5s delay
	}

	_, err = c.ch.QueueDeclare(
		TranscodeRetryQueue,
		true,
		false,
		false,
		false,
		retryQueueArgs,
	)

	if err != nil {
		return fmt.Errorf("failed to declare retry queue: %w", err)
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
			Headers: amqp.Table{
				"x-retries": 3,
			},
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
		if err := c.ch.Qos(1, 0, false); err != nil {
			return nil, fmt.Errorf("failed to set QoS: %w", err)
		}
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

func getRetryCount(headers amqp.Table, key string) int {
	v, ok := headers[key]
	if !ok || v == nil {
		return 0
	}

	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	case float32:
		return int(val)
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}

	return 0
}

func (c *RabbitClient) Retry(msg *amqp.Delivery) {
	headers := msg.Headers
	if headers == nil {
		headers = amqp.Table{}
	}

	retries := getRetryCount(headers, "x-retries")

	newCount := retries - 1

	if newCount <= 0 {
		log.Println("Retried it max times. Forwarding message to DLQ")
		msg.Nack(false, false)
		return
	}

	headers["x-retries"] = newCount

	err := c.ch.Publish(
		"", // default exchange
		TranscodeRetryQueue,
		false,
		false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  msg.ContentType,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         msg.Body,
		})

	if err != nil {
		log.Printf("Failed to publish to retry queue: %v. Nacking message.", err)
		msg.Nack(false, false)
		return
	}
	msg.Ack(false)
}

func (c *RabbitClient) Close() {
	if c.ch != nil {
		c.ch.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
