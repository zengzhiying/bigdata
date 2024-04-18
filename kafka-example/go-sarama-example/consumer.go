package main

import (
	"log"

	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct {
	ready chan bool
}

func (consumer *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("Consumer message channel was closed")
				return nil
			}
			// log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s",
			// 	string(message.Value), message.Timestamp, message.Topic)
			producedTopic := message.Topic
			if topic, ok := producedByConsumedTopic[message.Topic]; ok {
				producedTopic = topic
			}
			producedMessage := &sarama.ProducerMessage{
				Topic: producedTopic,
				Value: sarama.ByteEncoder(message.Value),
			}
			producer.Input() <- producedMessage

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
