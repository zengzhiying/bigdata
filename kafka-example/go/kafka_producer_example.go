package main
import "fmt"
import "github.com/Shopify/sarama"

func main() {
    producer, err := sarama.NewAsyncProducer([]string{"192.168.0.72:9092"}, nil)
    if err != nil {
        panic(err)
    }

    defer func() {
        if err := producer.Close(); err != nil {
            fmt.Println(err)
        }
    }()

    var enqueued, errors int
    for i := 0; i < 100; i++ {
        select {
        case producer.Input() <- &sarama.ProducerMessage{
            Topic: "myTopic",
            Key: nil,
            Value: sarama.StringEncoder(fmt.Sprintf("testing message: %d", i))}:
            enqueued++
        case err := <-producer.Errors():
            fmt.Println("Failed to produce message", err)
            errors++
        }
    }

    fmt.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
