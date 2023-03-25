package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

func subscribeToRedis(ctx context.Context, client *redis.Client, subChannel string) {
	pubsub := client.PSubscribe(context.Background(), subChannel) // Pattern-matching subscription

	_, _ = pubsub.Receive(context.Background()) // XXX: error checking omitted for brevity

	// s.pubsub = pubsub

	go func() {
		for msg := range pubsub.Channel() {
			switch msg.Channel {
			case "lip":
				// ...
				fmt.Println(msg.Payload)
			case "tasks.event.deleted":
				fmt.Println("no")
			}
		}

	}()
}

func createConsumerGroup(ctx context.Context, client *redis.Client, subject string, consumersGroup string) (string, error) {
	client.XGroupCreate(ctx, subject, consumersGroup, "0")

	return consumersGroup, nil
}

func handleNewTicket(ticketID string, ticketData string) error {
	log.Printf("Handling new ticket id : %s data %s\n", ticketID, ticketData)
	// time.Sleep(100 * time.Millisecond)
	return nil
}

func getAllTickets(ctx context.Context, client *redis.Client, subject, consumersGroup string) {
	uniqueID := uuid.New().String()
	// for {
		entries, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consumersGroup,
			Consumer: uniqueID,
			Streams:  []string{subject, ">"},
			Count:    2,
			Block:    0,
			NoAck:    false,
		}).Result()
		if err != nil {
			log.Fatal(err)
		}
		client.XAck(ctx, subject, consumersGroup, entries[0].Messages[0].ID)
		for i := 0; i < len(entries[0].Messages); i++ {
			messageID := entries[0].Messages[i].ID
			values := entries[0].Messages[i].Values
			eventDescription := fmt.Sprintf("%v", values["whatHappened"])
			ticketID := fmt.Sprintf("%v", values["ticketID"])
			ticketData := fmt.Sprintf("%v", values["ticketData"])
			if eventDescription == "ticket received" {
				err := handleNewTicket(ticketID, ticketData)
				if err != nil {
					log.Fatal(err)
				}
				client.XAck(ctx, subject, consumersGroup, messageID)
			}
		}
	// }
}

func main() {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	go subscribeToRedis(ctx, client, "lip")

	go subscribeToRedis(ctx, client, "lip")

	time.Sleep(8 * time.Second)

	var consumerGroup string
		consumerGroup, _ = createConsumerGroup(ctx, client, "dilo", "tickets-consumer")

	getAllTickets(ctx, client, "dilo", consumerGroup)

}
