package main

import (
	"fmt"
	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

// TIP To run your code, right-click the code and select <b>Run</b>. Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.
func initLog() {
	// 基本的日志记录
	log.Info("This is an info level log entry")
	log.WithFields(log.Fields{
		"animal": "walrus",
	}).Info("A walrus appears")

	// 设置日志格式为 JSON
	log.SetFormatter(&log.JSONFormatter{})
	log.Info("This is a JSON formatted log entry")

}
func main() {

	//result := mathutils.Square(5) // 调用mathutils包中的Square函数
	//fmt.Println(result) // 输出结果

	initLog()

	initproducer()

	//TIP Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined or highlighted text
	// to see how GoLand suggests fixing it.
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	partitionConsumer, err := consumer.ConsumePartition("my_topic", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()

	for msg := range partitionConsumer.Messages() {
		fmt.Printf("message received: %s\n", msg.Value)
	}
}

//TIP See GoLand help at <a href="https://www.jetbrains.com/help/go/">jetbrains.com/help/go/</a>.
// Also, you can try interactive lessons for GoLand by selecting 'Help | Learn IDE Features' from the main menu.
