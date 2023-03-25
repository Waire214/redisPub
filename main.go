package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v8"
)

// type SubscriptionHandler func(event platform.Event)

// type EventStore interface {
// 	Publish(topic string, message []byte) error
// 	PublishRaw(topic string, message ...interface{}) error
// 	Subscribe(topic string, handler SubscriptionHandler) error
// 	Run(ctx context.Context, handlers ...EventHandler)
// }

func publishToRedis(ctx context.Context, client *redis.Client, pubChannel string) {
	fmt.Println("start publishing to " + pubChannel)
	y := struct {
		Yes string
	}{
		Yes: "yes",
	}
	x, _ := json.Marshal(y)
	err := client.Publish(ctx, pubChannel, x).Err()
	if err != nil {
		panic(err)
	}
	fmt.Println("end publishing to " + pubChannel)
}

func clientRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	return client
}

func publishTicketReceivedEvent(ctx context.Context, client *redis.Client, stream string) error {
	log.Println("Publishing event to Redis")
	err := client.XAdd(ctx, &redis.XAddArgs{
		Stream:       stream,
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "",
		Values: map[string]interface{}{
			"whatHappened": string("ticket received"),
			"ticketID":     int(rand.Intn(100000000)),
			"ticketData":   string("some ticket data"),
		},
	}).Err()
	return err
}

func main() {

	ctx := context.Background()
	client := clientRedis()
	defer client.Close()

	go publishToRedis(ctx, client, "lip")

	time.Sleep(1 * time.Second)

		for i := 0; i < 1; i++ {
			err := publishTicketReceivedEvent(ctx, client, "dilo")
			if err != nil {
				log.Fatal(err)
			}
		}

}
