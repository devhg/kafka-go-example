package main

import (
	"github.com/devhg/kafka-go-example/conf"
	"github.com/devhg/kafka-go-example/helloworld/consumer"
)

func main() {
	consumer.Consume(conf.Topic)
}
