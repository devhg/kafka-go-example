package main

import (
	"github.com/devhg/kafka-go-example/conf"
	"github.com/devhg/kafka-go-example/helloworld/producer"
)

func main() {
	producer.Produce(conf.Topic, 1000)
}
