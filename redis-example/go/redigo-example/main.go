package main

import (
	"fmt"
	"time"
	"strconv"

	"github.com/gomodule/redigo/redis"
)

// redis tcp(127.0.0.1) push: 21.4s  pop: 21.4s, pipeline push: 1.4s, pipeline pop: 1.5s
// redis tcp(172.16.0.2) push: 52.7s pop: 51.5s, pipeline push: 2.2s, pipeline pop: 2.5s

func main() {
	c, err := redis.Dial("tcp", "127.0.0.1:6379",
		redis.DialDatabase(1))
	if err != nil {
		fmt.Println("redis connect error!", err)
	}
	defer c.Close()

    t1 := time.Now().UnixNano()
	for i := 0; i < 1000000; i++ {
		l, err := c.Do("LPUSH", "msg", strconv.Itoa(i))
		length, err := redis.Int(l, err)
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
		value, err := redis.String(c.Do("RPOP", "msg"))
		if err == redis.ErrNil {
			fmt.Printf("数据已经取空! %s, count: %d\n", value, popCount)
			break
		} else if err != nil {
			fmt.Printf("RPOP error! %s\n", err)
			break
		}
		if value == "" {
			fmt.Println("value is empty! ")
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

	t1 = time.Now().UnixNano()
	batch := 100
	n := 0
	for i := 0; i < 1000000; i++ {
		c.Send("LPUSH", "msg", strconv.Itoa(i))
		n += 1
		if n >= batch {
			err := c.Flush()
			if err != nil {
				fmt.Println("Flush error!", err)
			}
			for ; n > 0; n-- {
				c.Receive()
			}
			// n = 0
		}
	}
	//c.Flush()
	t2 = time.Now().UnixNano()
	pipePushTime := float64(t2 - t1) / 1.0e9
	fmt.Println("pipe push time:", pipePushTime)

	popCount = 0
	t1 = time.Now().UnixNano()
	for {
		c.Send("MULTI")
		for i := 0; i < 100; i++ {
			c.Send("RPOP", "msg")
		}
		r, err := c.Do("EXEC")
		// r: []interface{}
		if err != nil {
			fmt.Println("pipe pop error!", err)
			break
		}
		popCount += 100
		// fmt.Println(r)
		// fmt.Printf("%T\n", r)
		r1 := r.([]interface{})
		is_done := false
		// fmt.Println(r1)
		
		for _, num := range r1 {
			if num == nil {
				is_done = true
				break
			}
			n, err := strconv.Atoi(string(num.([]uint8)))
			if err != nil {
				fmt.Println("pipe exec convert error!", err)
			} else if popCount % 100000 == 0 {
				fmt.Printf("pipe pop count: %d, value: %d\n", popCount, n)
				break
			}
		}
		if is_done {
			fmt.Println("数据已经取空!")
			break
		}
	}
	t2 = time.Now().UnixNano()
	pipePopTime := float64(t2 - t1) / 1.0e9
	fmt.Println("pipe pop time:", pipePopTime)
}

