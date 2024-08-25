package main

import (
	"fmt"
	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

// Config 配置
type Config struct {
	Topic      string `xml:"topic"`
	Broker     string `xml:"broker"`
	Frequency  int    `xml:"frequency"`
	MaxMessage int    `xml:"max_message"`
}

func NewConfig(topic string, broker string, Frequency int, MaxMessage int) *Config {
	return &Config{
		Topic:      topic,
		Broker:     broker,
		Frequency:  Frequency,
		MaxMessage: MaxMessage,
	}

}

type Producer struct {
	producer  sarama.AsyncProducer
	topic     string
	wg        sync.WaitGroup
	closeChan chan struct{}
}

// NewProducer 构造KafkaProducer
func NewProducer(cfg *Config) (*Producer, error) {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse                                  // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy                            // Compress messages
	config.Producer.Flush.Frequency = time.Duration(cfg.Frequency) * time.Millisecond // Flush batches every 500ms
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	p, err := sarama.NewAsyncProducer(strings.Split(cfg.Broker, ","), config)
	if err != nil {
		return nil, err
	}
	ret := &Producer{
		producer: p,
		topic:    cfg.Topic,
	}

	return ret, nil
}

// Run 运行
func (producer *Producer) Run() {
	// 定时任务
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				// 构造消息
				msg := &sarama.ProducerMessage{
					Topic: "test",
					Value: sarama.StringEncoder(fmt.Sprintf("Message %d from goroutine", i)),
				}

				// 发送消息
				producer.producer.Input() <- msg

				log.Info("Message %d sent to partition %d at Topic %d\n", i, msg.Partition, msg.Topic)
			}(i)
		}
		wg.Wait() // 等待所有goroutine完成
	}
}

func initproducer() {
	config := NewConfig("test", "localhost:9092", 3, 10000)

	p, err := NewProducer(config) // 调用mathutils包中的Square函数

	if err != nil {
		panic(err)
	}
	p.Run()

	fmt.Println(p) // 输出结果
}

// Close 关闭
func (p *Producer) Close() error {
	p.wg.Wait()
	log.Warn("[producer] quit over")

	return p.producer.Close()
}
