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

// output:
// message offset: 0 ,body: {"name": "item-0", "age": 0}
// message offset: 1 ,body: {"name": "item-1", "age": 1}
// message offset: 2 ,body: {"name": "item-2", "age": 2}
// message offset: 3 ,body: {"name": "item-3", "age": 3}
// message offset: 4 ,body: {"name": "item-4", "age": 4}
// message offset: 5 ,body: {"name": "item-5", "age": 5}
// message offset: 6 ,body: {"name": "item-6", "age": 6}
// message offset: 7 ,body: {"name": "item-7", "age": 7}
// message offset: 8 ,body: {"name": "item-8", "age": 8}
// message offset: 9 ,body: {"name": "item-9", "age": 9}
// message offset: 10 ,body: {"name": "item-10", "age": 10}
// message offset: 11 ,body: {"name": "item-11", "age": 11}
// message offset: 12 ,body: {"name": "item-12", "age": 12}
// message offset: 13 ,body: {"name": "item-13", "age": 13}
// message offset: 14 ,body: {"name": "item-14", "age": 14}
// message offset: 15 ,body: {"name": "item-15", "age": 15}
// message offset: 16 ,body: {"name": "item-16", "age": 16}
// message offset: 17 ,body: {"name": "item-17", "age": 17}
// message offset: 18 ,body: {"name": "item-18", "age": 18}
// message offset: 19 ,body: {"name": "item-19", "age": 19}
// message offset: 20 ,body: {"name": "item-20", "age": 20}
// message offset: 21 ,body: {"name": "item-21", "age": 21}
// message offset: 22 ,body: {"name": "item-22", "age": 22}
// message offset: 23 ,body: {"name": "item-23", "age": 23}
// message offset: 24 ,body: {"name": "item-24", "age": 24}
// message offset: 25 ,body: {"name": "item-25", "age": 25}
// message offset: 26 ,body: {"name": "item-26", "age": 26}
// message offset: 27 ,body: {"name": "item-27", "age": 27}
// message offset: 28 ,body: {"name": "item-28", "age": 28}
// message offset: 29 ,body: {"name": "item-29", "age": 29}
// message offset: 30 ,body: {"name": "item-30", "age": 30}
// message offset: 31 ,body: {"name": "item-31", "age": 31}
// message offset: 32 ,body: {"name": "item-32", "age": 32}
// message offset: 33 ,body: {"name": "item-33", "age": 33}
// message offset: 34 ,body: {"name": "item-34", "age": 34}
// message offset: 35 ,body: {"name": "item-35", "age": 35}
// message offset: 36 ,body: {"name": "item-36", "age": 36}
// message offset: 37 ,body: {"name": "item-37", "age": 37}
// message offset: 38 ,body: {"name": "item-38", "age": 38}
// message offset: 39 ,body: {"name": "item-39", "age": 39}
// message offset: 40 ,body: {"name": "item-40", "age": 40}
// message offset: 41 ,body: {"name": "item-41", "age": 41}
// message offset: 42 ,body: {"name": "item-42", "age": 42}
// message offset: 43 ,body: {"name": "item-43", "age": 43}
// message offset: 44 ,body: {"name": "item-44", "age": 44}
// message offset: 45 ,body: {"name": "item-45", "age": 45}
// message offset: 46 ,body: {"name": "item-46", "age": 46}
// message offset: 47 ,body: {"name": "item-47", "age": 47}
// message offset: 48 ,body: {"name": "item-48", "age": 48}
// message offset: 49 ,body: {"name": "item-49", "age": 49}
// message offset: 50 ,body: {"name": "item-50", "age": 50}
// message offset: 51 ,body: {"name": "item-51", "age": 51}
// message offset: 52 ,body: {"name": "item-52", "age": 52}
// message offset: 53 ,body: {"name": "item-53", "age": 53}
// message offset: 54 ,body: {"name": "item-54", "age": 54}
// message offset: 55 ,body: {"name": "item-55", "age": 55}
// message offset: 56 ,body: {"name": "item-56", "age": 56}
// message offset: 57 ,body: {"name": "item-57", "age": 57}
// message offset: 58 ,body: {"name": "item-58", "age": 58}
// message offset: 59 ,body: {"name": "item-59", "age": 59}
// message offset: 60 ,body: {"name": "item-60", "age": 60}
// message offset: 61 ,body: {"name": "item-61", "age": 61}
// message offset: 62 ,body: {"name": "item-62", "age": 62}
// message offset: 63 ,body: {"name": "item-63", "age": 63}
// message offset: 64 ,body: {"name": "item-64", "age": 64}
// message offset: 65 ,body: {"name": "item-65", "age": 65}
// message offset: 66 ,body: {"name": "item-66", "age": 66}
// message offset: 67 ,body: {"name": "item-67", "age": 67}
// message offset: 68 ,body: {"name": "item-68", "age": 68}
// message offset: 69 ,body: {"name": "item-69", "age": 69}
// message offset: 70 ,body: {"name": "item-70", "age": 70}
// message offset: 71 ,body: {"name": "item-71", "age": 71}
// message offset: 72 ,body: {"name": "item-72", "age": 72}
// message offset: 73 ,body: {"name": "item-73", "age": 73}
// message offset: 74 ,body: {"name": "item-74", "age": 74}
// message offset: 75 ,body: {"name": "item-75", "age": 75}
// message offset: 76 ,body: {"name": "item-76", "age": 76}
// message offset: 77 ,body: {"name": "item-77", "age": 77}
// message offset: 78 ,body: {"name": "item-78", "age": 78}
// message offset: 79 ,body: {"name": "item-79", "age": 79}
// message offset: 80 ,body: {"name": "item-80", "age": 80}
// message offset: 81 ,body: {"name": "item-81", "age": 81}
// message offset: 82 ,body: {"name": "item-82", "age": 82}
// message offset: 83 ,body: {"name": "item-83", "age": 83}
// message offset: 84 ,body: {"name": "item-84", "age": 84}
// message offset: 85 ,body: {"name": "item-85", "age": 85}
// message offset: 86 ,body: {"name": "item-86", "age": 86}
// message offset: 87 ,body: {"name": "item-87", "age": 87}
// message offset: 88 ,body: {"name": "item-88", "age": 88}
// message offset: 89 ,body: {"name": "item-89", "age": 89}
// message offset: 90 ,body: {"name": "item-90", "age": 90}
// message offset: 91 ,body: {"name": "item-91", "age": 91}
// message offset: 92 ,body: {"name": "item-92", "age": 92}
// message offset: 93 ,body: {"name": "item-93", "age": 93}
// message offset: 94 ,body: {"name": "item-94", "age": 94}
// message offset: 95 ,body: {"name": "item-95", "age": 95}
// message offset: 96 ,body: {"name": "item-96", "age": 96}
// message offset: 97 ,body: {"name": "item-97", "age": 97}
// message offset: 98 ,body: {"name": "item-98", "age": 98}
// message offset: 99 ,body: {"name": "item-99", "age": 99}
// message offset: 103 ,body: {"name": "item-100", "age": 100}
// message offset: 104 ,body: {"name": "item-101", "age": 101}
// message offset: 105 ,body: {"name": "item-102", "age": 102}
// message offset: 106 ,body: {"name": "item-103", "age": 103}
// message offset: 107 ,body: {"name": "item-104", "age": 104}
// message offset: 108 ,body: {"name": "item-105", "age": 105}
// message offset: 109 ,body: {"name": "item-106", "age": 106}
// message offset: 110 ,body: {"name": "item-107", "age": 107}
// message offset: 111 ,body: {"name": "item-108", "age": 108}
// message offset: 112 ,body: {"name": "item-109", "age": 109}
// message offset: 113 ,body: {"name": "item-110", "age": 110}
// message offset: 114 ,body: {"name": "item-111", "age": 111}
// message offset: 115 ,body: {"name": "item-112", "age": 112}
// message offset: 116 ,body: {"name": "item-113", "age": 113}
// message offset: 117 ,body: {"name": "item-114", "age": 114}
// message offset: 118 ,body: {"name": "item-115", "age": 115}
// message offset: 119 ,body: {"name": "item-116", "age": 116}
// message offset: 120 ,body: {"name": "item-117", "age": 117}
// message offset: 121 ,body: {"name": "item-118", "age": 118}
// message offset: 122 ,body: {"name": "item-119", "age": 119}
// message offset: 123 ,body: {"name": "item-120", "age": 120}
// message offset: 124 ,body: {"name": "item-121", "age": 121}
// message offset: 125 ,body: {"name": "item-122", "age": 122}
// message offset: 126 ,body: {"name": "item-123", "age": 123}
// message offset: 127 ,body: {"name": "item-124", "age": 124}
// message offset: 128 ,body: {"name": "item-125", "age": 125}
// message offset: 129 ,body: {"name": "item-126", "age": 126}
// message offset: 130 ,body: {"name": "item-127", "age": 127}
// message offset: 131 ,body: {"name": "item-128", "age": 128}
// message offset: 132 ,body: {"name": "item-129", "age": 129}
// message offset: 133 ,body: {"name": "item-130", "age": 130}
// message offset: 134 ,body: {"name": "item-131", "age": 131}
// message offset: 135 ,body: {"name": "item-132", "age": 132}
// message offset: 136 ,body: {"name": "item-133", "age": 133}
// message offset: 137 ,body: {"name": "item-134", "age": 134}
// message offset: 138 ,body: {"name": "item-135", "age": 135}
// message offset: 139 ,body: {"name": "item-136", "age": 136}
// message offset: 140 ,body: {"name": "item-137", "age": 137}
// message offset: 141 ,body: {"name": "item-138", "age": 138}
// message offset: 142 ,body: {"name": "item-139", "age": 139}
// message offset: 143 ,body: {"name": "item-140", "age": 140}
// message offset: 144 ,body: {"name": "item-141", "age": 141}
// message offset: 145 ,body: {"name": "item-142", "age": 142}
// message offset: 146 ,body: {"name": "item-143", "age": 143}
// message offset: 147 ,body: {"name": "item-144", "age": 144}
// message offset: 148 ,body: {"name": "item-145", "age": 145}
// message offset: 149 ,body: {"name": "item-146", "age": 146}
// message offset: 150 ,body: {"name": "item-147", "age": 147}
// message offset: 151 ,body: {"name": "item-148", "age": 148}
// message offset: 152 ,body: {"name": "item-149", "age": 149}
// message offset: 153 ,body: {"name": "item-150", "age": 150}
// message offset: 154 ,body: {"name": "item-151", "age": 151}
// message offset: 155 ,body: {"name": "item-152", "age": 152}
// message offset: 156 ,body: {"name": "item-153", "age": 153}
// message offset: 157 ,body: {"name": "item-154", "age": 154}
// message offset: 158 ,body: {"name": "item-155", "age": 155}
// message offset: 159 ,body: {"name": "item-156", "age": 156}
// message offset: 160 ,body: {"name": "item-157", "age": 157}
// message offset: 161 ,body: {"name": "item-158", "age": 158}
// message offset: 162 ,body: {"name": "item-159", "age": 159}
// message offset: 163 ,body: {"name": "item-160", "age": 160}
// message offset: 164 ,body: {"name": "item-161", "age": 161}
// message offset: 165 ,body: {"name": "item-162", "age": 162}
// message offset: 166 ,body: {"name": "item-163", "age": 163}
// message offset: 167 ,body: {"name": "item-164", "age": 164}
// message offset: 168 ,body: {"name": "item-165", "age": 165}
// message offset: 169 ,body: {"name": "item-166", "age": 166}
// message offset: 170 ,body: {"name": "item-167", "age": 167}
// message offset: 171 ,body: {"name": "item-168", "age": 168}
// message offset: 172 ,body: {"name": "item-169", "age": 169}
// message offset: 173 ,body: {"name": "item-170", "age": 170}
// message offset: 174 ,body: {"name": "item-171", "age": 171}
// message offset: 175 ,body: {"name": "item-172", "age": 172}
// message offset: 176 ,body: {"name": "item-173", "age": 173}
// message offset: 177 ,body: {"name": "item-174", "age": 174}
// message offset: 178 ,body: {"name": "item-175", "age": 175}
// message offset: 179 ,body: {"name": "item-176", "age": 176}
// message offset: 180 ,body: {"name": "item-177", "age": 177}
// message offset: 181 ,body: {"name": "item-178", "age": 178}
// message offset: 182 ,body: {"name": "item-179", "age": 179}
// message offset: 183 ,body: {"name": "item-180", "age": 180}
// message offset: 184 ,body: {"name": "item-181", "age": 181}
// message offset: 185 ,body: {"name": "item-182", "age": 182}
// message offset: 186 ,body: {"name": "item-183", "age": 183}
// message offset: 187 ,body: {"name": "item-184", "age": 184}
// message offset: 188 ,body: {"name": "item-185", "age": 185}
// message offset: 189 ,body: {"name": "item-186", "age": 186}
// message offset: 190 ,body: {"name": "item-187", "age": 187}
// message offset: 191 ,body: {"name": "item-188", "age": 188}
// message offset: 192 ,body: {"name": "item-189", "age": 189}
// message offset: 193 ,body: {"name": "item-190", "age": 190}
// message offset: 194 ,body: {"name": "item-191", "age": 191}
// message offset: 195 ,body: {"name": "item-192", "age": 192}
// message offset: 196 ,body: {"name": "item-193", "age": 193}
// message offset: 197 ,body: {"name": "item-194", "age": 194}
// message offset: 198 ,body: {"name": "item-195", "age": 195}
// message offset: 199 ,body: {"name": "item-196", "age": 196}
// message offset: 200 ,body: {"name": "item-197", "age": 197}
// message offset: 201 ,body: {"name": "item-198", "age": 198}
// message offset: 202 ,body: {"name": "item-199", "age": 199}