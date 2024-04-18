package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

var (
	kafkaVersion    = sarama.V2_5_1_0
	consumedBrokers = []string{
		"src-host1:9092",
		"src-host2:9092",
	}
	producedBrokers = []string{
		"dst-host1:9092",
	}
	subscribedTopics = []string{
		"testTopic1",
		"testTopic2",
	}
	// 消费者组
	consumedGroup = "my-group"
	// 初始偏移量，默认: OffsetNewest
	initialOffset = sarama.OffsetOldest
)

// 消费到生产的 Topic 映射关系，如果映射关系不存在则使用相同的 Topic 名称
var producedByConsumedTopic = map[string]string{
	"testTopic2": "testTopic3",
}

var producer sarama.AsyncProducer

func main() {
	if len(consumedBrokers) == 0 || len(producedBrokers) == 0 || len(subscribedTopics) == 0 {
		return
	}

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	// Create Consumer Group
	consumedConfig := sarama.NewConfig()
	consumedConfig.Version = kafkaVersion
	consumedConfig.Consumer.Return.Errors = true
	// Default: OffsetNewest
	consumedConfig.Consumer.Offsets.Initial = initialOffset
	// roundrobin 平衡策略
	consumedConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	group, err := sarama.NewConsumerGroup(consumedBrokers, consumedGroup, consumedConfig)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	// Create AsyncProducer Client
	producedConfig := sarama.NewConfig()
	producedConfig.Version = kafkaVersion
	producedConfig.Producer.Return.Successes = true
	producedConfig.Producer.Return.Errors = true
	producer, err = sarama.NewAsyncProducer(producedBrokers, producedConfig)
	if err != nil {
		log.Panicf("Error creating async producer client: %v", err)
	}

	consumerHandler := ConsumerGroupHandler{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		successes := 0
		start := time.Now()
		for range producer.Successes() {
			successes++
			if time.Since(start).Seconds() > 30 {
				log.Printf("number of successfully produced: %d", successes)
				start = time.Now()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errors := 0
		start := time.Now()
		for range producer.Errors() {
			errors++
			if time.Since(start).Seconds() > 30 {
				log.Printf("number of failures produced: %d", errors)
				start = time.Now()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := group.Consume(ctx, subscribedTopics, &consumerHandler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// 上下文取消，则消费者停止
			if ctx.Err() != nil {
				return
			}
			consumerHandler.ready = make(chan bool)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range group.Errors() {
			fmt.Printf("Consume err: %s\n", err)
		}
	}()

	<-consumerHandler.ready
	log.Println("Sarama consumer up and running")
	isRunning := true
	isPause := false

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	for isRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			isRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			isRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(group, &isPause)
		}
	}

	cancel()
	producer.AsyncClose()
	if err := group.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
	// 等待消费者和生产者通道结束
	wg.Wait()
}

func toggleConsumptionFlow(group sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		group.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		group.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}
