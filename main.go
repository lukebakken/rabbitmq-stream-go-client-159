package main

import "fmt"
import "time"
import "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
import "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
import "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"

func main() {
	addresses := []string{
		"rabbitmq-stream://guest:guest@shostakovich:5552/%2F"}

	env, _ := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUris(addresses))

	streamName := "test-next"
	env.DeleteStream(streamName)
	env.DeclareStream(streamName, stream.NewStreamOptions().SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))

	producer, err := env.NewProducer(streamName, nil)
	count := 100
	for i := 0; i < count; i++ {
		var message message.StreamMessage
		body := fmt.Sprintf(`{"name": "item-%d", "age": %d}`, i, i)
		message = amqp.NewMessage([]byte(body))
		err = producer.Send(message)
		if err != nil {
			fmt.Println(err)
		}
	}

	time.Sleep(5 * time.Second)

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Println("message offset:", consumerContext.Consumer.GetOffset(), ",body:", string(message.GetData()))
		consumerContext.Consumer.StoreOffset()
	}

	options := stream.NewConsumerOptions().SetConsumerName("golang-client").SetOffset(stream.OffsetSpecification{}.First()).SetManualCommit()
	env.NewConsumer(streamName, handleMessages, options)

	time.Sleep(1 * time.Second)

	go func() {
		count := 100
		for i := 100; i < 100+count; i++ {
			var message message.StreamMessage
			body := fmt.Sprintf(`{"name": "item-%d", "age": %d}`, i, i)
			message = amqp.NewMessage([]byte(body))
			err = producer.Send(message)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	time.Sleep(30 * time.Second)
}
