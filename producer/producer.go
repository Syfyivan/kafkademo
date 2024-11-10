package producer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

var (
	topic = "user_click"
)

// 生产消息
func WriteMessages(ctx context.Context) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		WriteTimeout:           1 * time.Second,
		RequiredAcks:           kafka.RequireNone,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()
	for i := 0; i < 3; i++ {
		if err := writer.WriteMessages(
			ctx,
			kafka.Message{Key: []byte("1"), Value: []byte("万幸")},
			kafka.Message{Key: []byte("2"), Value: []byte("得以")},
			kafka.Message{Key: []byte("3"), Value: []byte("存活")},
		); err != nil {
			if err == kafka.LeaderNotAvailable {
				time.Sleep(10 * time.Second)
				continue
			} else {
				fmt.Println("批量写kafka失败:", err)
			}
		} else {
			fmt.Println("写入消息成功")
		}
	}
}
