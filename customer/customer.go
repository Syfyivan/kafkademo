// consumer/consumer.go
package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	topic = "user_click"
)

// 消费消息
func ReadMessages(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          topic,
		CommitInterval: 1 * time.Second,
		GroupID:        "rec_team",
		StartOffset:    kafka.FirstOffset, // earliest or latest
	})

	for {
		if message, err := reader.ReadMessage(ctx); err != nil {
			fmt.Println("读取消息失败:", err)
			break
		} else {
			fmt.Println("读取消息成功:", message.Value)
			fmt.Println("topic=%s, partition=%d, offset=%d, key=%s, value=%s", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
		}
	}
}

// 监听信息2和15，当收到信号的时候关闭reader
func ListenSignal() {
	//注册信号
	c := make(chan os.Signal, 1)                      //创建信号管道，容量为1
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM) //监听信号2和15，当收到这两个信号的时候放到c管道里
	sig := <-c                                        //从管道里读出这个信号，如果没有收到信号，这行读管道的代码会堵塞
	fmt.Println("收到信号", sig.String())
	os.Exit(0) //退出程序
}
