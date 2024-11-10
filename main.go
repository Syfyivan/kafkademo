package main

import (
	"context"
	"kafkademo/customer"
	"kafkademo/producer"
)

func main() {
	ctx := context.Background()

	go consumer.ListenSignal()
	go producer.WriteMessages(ctx)
	consumer.ReadMessages(ctx)
}
