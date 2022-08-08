func TestNext(t *testing.T) {
	addresses := []string{
		"rabbitmq-stream://gpss:gpss@10.117.190.125:5552/zhangwenkang"}

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

	time.Sleep(3 * time.Second)

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
	time.Sleep(1000 * time.Second)
}