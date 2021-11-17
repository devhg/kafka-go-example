package offsetmanager

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"

	"github.com/devhg/kafka-go-example/conf"
)

/*
	本例展示最简单的 偏移量管理器 的手动使用（在 消费者组中 sarama 库实现了偏移量自动管理）
	增加偏移量管理后就可以记录下每次消费的位置，便于下次接着消费，避免 sarama.OffsetOldest 的重复消费
	或者 sarama.OffsetNewest 漏掉部分消息
	NOTE: 相比普通consumer增加了OffsetManager，调用 MarkOffset 手动记录了当前消费的 offset，
	最后调用 commit 提交到 kafka。
	sarama 库的自动提交就相当于 offsetManager.Commit() 操作，还是需要手动调用 MarkOffset。
*/

func OffsetManager(topic string) {
	config := sarama.NewConfig()
	// 配置开启自动提交 offset，这样 samara 库会定时帮我们把最新的 offset 信息提交给 kafka
	config.Consumer.Offsets.AutoCommit.Enable = true              // 开启自动 commit offset
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Minute // 每 1 分钟提交一次 offset

	client, err := sarama.NewClient([]string{conf.HOST}, config)
	if err != nil {
		log.Fatal("NewClient err: ", err)
	}
	defer client.Close()

	// offsetManager 用于管理每个consumerGroup的 offset
	// 根据 groupID 来区分不同的 consumer，注意: 每次提交的 offset 信息也是和 groupID 关联的
	offsetManager, err := sarama.NewOffsetManagerFromClient("myGroupID", client)
	if err != nil {
		log.Fatal("NewOffsetManagerFromClient err: ", err)
	}
	defer offsetManager.Close()

	// 每个分区的 offset 也是分别管理的，这里使用 0 分区，因为该 topic 只有 1 个分区
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, conf.DefaultPartition)
	if err != nil {
		log.Fatal("ManagePartition err: ", err)
	}
	defer partitionOffsetManager.Close()
	defer offsetManager.Commit() // defer 在程序结束后在 commit 一次，防止自动提交间隔之间的信息被丢掉

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal("NewConsumerFromClient err: ", err)
	}

	// 根据 kafka 中记录的上次消费的 offset 开始+1的位置接着消费
	nextOffset, _ := partitionOffsetManager.NextOffset()
	fmt.Println("nextOffset:", nextOffset)

	// nextOffset = 500  // 手动设置下次消费的 offset

	pc, err := consumer.ConsumePartition(topic, conf.DefaultPartition, nextOffset)
	if err != nil {
		log.Fatal("ConsumePartition err: ", err)
	}
	defer pc.AsyncClose()

	// 开始消费
	for msg := range pc.Messages() {
		log.Printf("[Consumer] partitionID: %d; offset:%d, value: %s\n",
			msg.Partition, msg.Offset, string(msg.Value))
		// 每次消费后都更新一次 offset, 这里更新的只是程序内存中的值，需要 commit 之后才能提交到 kafka
		partitionOffsetManager.MarkOffset(msg.Offset+1, "modified metadata")
	}
}
