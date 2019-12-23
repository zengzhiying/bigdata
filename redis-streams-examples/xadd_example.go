package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

var redisClient *redis.Client

func main() {
	redisClient = redis.NewClient(&redis.Options{
		Network:  "tcp",
		Addr: "127.0.0.1:6379",
		Password: "",
		DB: 0,
	})
	_, err := redisClient.Ping().Result()
	if err != nil {
		fmt.Println("Ping redis err: ", err)
		return
	}
	for i := 0; i < 1000; i++ {
		xaddStream := redis.XAddArgs{
			Stream: "mystream",
			MaxLenApprox: 100000,
			ID: "*",
			Values: map[string]interface{}{
				"i": i + 1,
			},
		}
		result, err := redisClient.XAdd(&xaddStream).Result()
		if err != nil {
			fmt.Println("Send stream message err: ", err)
		} else {
			fmt.Println("Send stream message ok! ", result)
		}
		time.Sleep(10*time.Millisecond)
	}
	err = redisClient.Close()
	if err != nil {
		fmt.Println("Close redis client error! ", err)
	}
	fmt.Println("done.")
}
