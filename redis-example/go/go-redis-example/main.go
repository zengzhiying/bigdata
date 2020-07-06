package main

import (
	"fmt"
	"time"
	"strconv"
	"context"

	"github.com/go-redis/redis/v8"
)

// redis sock push: 17.7s  pop: 17.9s
// redis tcp(127.0.0.1) push: 23.3s  pop: 23.7s  pipeline push: 1.7s  pipeline pop: 1.4s
// redis tcp(172.16.0.2) push: 51.6s pop: 52.2s  pipeline push: 3.0s  pipeline pop: 2.2s

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		// Network: "unix",
		Network: "tcp",
		Addr: "127.0.0.1:6379",
		// Addr: "/tmp/redis.sock",
		Password: "",
		DB: 1,
	})

	defer rdb.Close()

	t1 := time.Now().UnixNano()

	for i := 0; i < 1000000; i++ {
		length, err := rdb.LPush(ctx, "msg", strconv.Itoa(i)).Result()
		if err != nil {
			fmt.Println("redis lpush error!", err)
		} else if length % 100000 == 0 {
			fmt.Println("list length:", length)
		}
	}
	t2 := time.Now().UnixNano()
	pushTime := float64(t2 - t1) / 1.0e9
	fmt.Println("push time:", pushTime)

	t1 = time.Now().UnixNano()
	popCount := 0
	for {
		value, err := rdb.RPop(ctx, "msg").Result()
		if err == redis.Nil {
			fmt.Printf("数据已经取空! %s, count: %d\n", value, popCount)
			break
		} else if err != nil {
			fmt.Printf("RPOP error! %s\n", err)
			break
		}

		if value == "" {
			fmt.Println("value is empty!")
		} else {
			popCount++
			if popCount % 100000 == 0 {
				fmt.Printf("pop count: %d, value: %s\n", popCount, value)
			}
		}
	}

	t2 = time.Now().UnixNano()
	popTime := float64(t2 - t1) / 1.0e9
	fmt.Println("pop time:", popTime)

	pipe := rdb.Pipeline()
	defer pipe.Close()
	t1 = time.Now().UnixNano()
	batch, n := 100, 0
	for i := 0; i < 1000000; i++ {
		length := pipe.LPush(ctx, "msg", strconv.Itoa(i))
		n++
		if n >= batch {
			_, err := pipe.Exec(ctx)
			if err != nil {
				fmt.Println("pipe exec error!", err)
			} else {
				l := length.Val()
				if l % 100000 == 0 {
					fmt.Println("pipe exec num:", l)
				}
			}
			n = 0
		}
	}
	t2 = time.Now().UnixNano()
	pipePushTime := float64(t2 - t1) / 1.0e9
	fmt.Println("pipe push time:", pipePushTime)

	t1 = time.Now().UnixNano()
	popCount = 0
	for {
		// var value *redis.StringCmd
		values := make([]*redis.StringCmd, 100)
		for i := 0; i < 100; i++ {
			value := pipe.RPop(ctx, "msg")
			// if value.Err() != nil {
			// 	fmt.Println("pipe pop error", value.Err())
			// }
			values[i] = value
		}
		popCount += 100
		_, err := pipe.Exec(ctx)
		// 返回第一个失败命令的错误
		if err == redis.Nil {
			fmt.Println("pipe rpop exec 为空!", err)
			// break
		} else if err != nil {
			fmt.Println("pipe exec error!", err)
			break
		}

		isDone := false

		for _, value := range values {
			if value.Err() == redis.Nil {
				fmt.Println("pipeline 数据已经取空!", value.Val())
				isDone = true
				break
			}
			if value.Err() != nil {
				fmt.Println("pipeline rpop error!", value.Err())
			}
			if popCount % 100000 == 0 {
				fmt.Printf("pop count: %d, value: %s\n", popCount, value.Val())
				break
			}
		}

		if isDone {
			break
		}
	}
	t2 = time.Now().UnixNano()
	pipePopTime := float64(t2 - t1) / 1.0e9
	fmt.Println("pipe push time:", pipePopTime)
}
