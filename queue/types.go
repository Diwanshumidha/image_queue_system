package queue

import amqp "github.com/rabbitmq/amqp091-go"

type ImageEvent struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type RabbitClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

type Table = amqp.Table
type Delivery = amqp.Delivery
