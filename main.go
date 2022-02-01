package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type User struct {
	UserReference string `json:"user_reference"`
}

type Otp struct {
	Reference     string    `json:"reference" bson:"reference"`
	UserReference string    `json:"user_reference" bson:"user_reference"`
	Code          int       `json:"code" bson:"code"`
	CreatedOn     string    `json:"created_on" bson:"created_on"`
	ExpiresAt     time.Time `json:"expires_at" bson:"expires_at"`
}

var (
	ctx    = context.TODO()
	client1 = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	client2 = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	client3 = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
// 	MESSAGE for the decagon guys
// 	instead of creating 3 clients. I think it should be one but at the time, one wasn't working. It should be one because the value is a pointer
	// 	so you can say client = redis.NewClient(&redis.Options{
// 		Addr: "localhost:6379",
// 	})
// 	then defer client.Close() --- not tested yet, but you can test it. I will test soon.
	user     = User{UserReference: "1111dshjureuj3284837hewbdsg"}
	checkOtp = Otp{Reference: "868762e1-9a2b-411a-9b6c-8d518fcded91", UserReference: "1111dshjureuj3284837hewbdsg", Code: 620671}
	identityMessage []interface{}
	checkOtpMessage []interface{}
)

type Redis struct {
	Reference string
	Channel   string
	Subject   string
	Message   []interface{}
}


func subscribeToRedis(client *redis.Client, subChannel string) Redis {
	fmt.Println("start subscription to " + subChannel)
	receiveMessage := Redis{}
	sub := client.Subscribe(ctx, subChannel)

	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		fmt.Println(err)
	}
	if err := json.Unmarshal([]byte(msg.Payload), &receiveMessage); err != nil {
		panic(err)
	}
	fmt.Println(receiveMessage)
	fmt.Println("end subscription to  " + subChannel)
	return receiveMessage
}

func publishToRedis(client *redis.Client, pubChannel string, sendMessage Redis) {
	fmt.Println("start publishing to " + pubChannel)
	payload, err := json.Marshal(sendMessage)
	if err != nil {
		panic(err)
	}
	fmt.Println(sendMessage)
	if err := client.Publish(ctx, pubChannel, payload).Err(); err != nil {
		panic(err)
	}
	fmt.Println("end publishing to " + pubChannel)
}

func main() {
	sendIdentity := Redis{
		Reference: uuid.New().String(),
		Channel:   "identity",
		Subject:   "Publish user identity to the otp-service for identification and authorization",
		Message:   append(identityMessage, user.UserReference),
	}
	sendOtp := Redis{
		Reference: "868762e1-9a2b-411a-9b6c-8d518fcded91",
		Channel: "notification",
		Subject: "Send Otp to the otp-service for validation",
		Message: append(checkOtpMessage, "1111dshjureuj3284837hewbdsg", 620671),
	}
	publishToRedis(client1, "identity", sendIdentity)
	subscribeToRedis(client2, "otp")
	publishToRedis(client3, "notification", sendOtp)

}
