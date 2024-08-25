package main

import (
	"encoding/xml"
	"fmt"
	"github.com/IBM/sarama"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

type KafkaConfig struct {
	XMLName   xml.Name `xml:"configuration"`
	Topic     string   `xml:"topic"` // 这里也可以定义成数组，便于监听多个
	Broker    string   `xml:"broker"`
	Partition int32    `xml:"partition"` // 通常分区号由消费者库自动管理
	// Replication 在消费者配置中通常不需要
	Group   string `xml:"group"`
	Version string `xml:"version"` // 这个版本可能是Kafka客户端库的版本，或你的应用版本
}

func readConfig(filePath string) (*KafkaConfig, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var cfg KafkaConfig
	err = xml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func startKafkaConsumers(cfg *KafkaConfig, numConsumers int) {
	var wg sync.WaitGroup

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			config := sarama.NewConfig()
			config.Net.SASL.Enable = false // 根据需要配置SASL
			//config.Consumer.Group.InstanceId = "unique-instance-id"
			config.Consumer.Return.Errors = true

			client, err := sarama.NewConsumer([]string{cfg.Broker}, config)
			if err != nil {
				panic(err)
			}
			defer client.Close()

			defer wg.Done()
			topic := cfg.Topic
			partitionConsumer, err := client.ConsumePartition(topic, int32(0), sarama.OffsetNewest) // 或 sarama.OffsetOldest
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to start consumer for partition %d: %s\n", 0, err)
				return
			}
			defer partitionConsumer.Close()

			for msg := range partitionConsumer.Messages() {
				fmt.Printf("Consumer %d: Received message at offset %d, partition %d, value: %s\n", consumerID, msg.Offset, msg.Partition, string(msg.Value))
			}

		}(i)
	}

	wg.Wait()
}

func main() {
	cfg, err := readConfig("config.xml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %s\n", err)
		os.Exit(1)
	}

	numConsumers, err := strconv.Atoi(os.Getenv("KAFKA_CONSUMERS")) // 或者从配置文件读取
	if err != nil {
		numConsumers = 10 // 默认启动10个消费者
	}

	startKafkaConsumers(cfg, numConsumers)
}
