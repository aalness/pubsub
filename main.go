package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/time/rate"
)

var R = flag.Int("r", 0, "rate to produce messages (per second)")
var N = flag.Int("n", 0, "number of topics")
var P = flag.Int("p", 0, "number of publishers")

var total int
var startTime time.Time
var stop bool

func usage() {
	fmt.Printf("Usage:\n\n")
	fmt.Printf("  publish messages at rate R randomly across N different topics:\n")
	fmt.Printf("    -r <R> -n <N> publish\n\n")
	fmt.Printf("  subscribe to N different topics and expect P publishers:\n")
	fmt.Printf("    -n <N> -p <P> subscribe\n\n")
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		usage()
		return
	}

	address := os.Getenv("REDIS_HOST_ADDRESS")
	if len(address) == 0 {
		fmt.Println("REDIS_HOST_ADDRESS must be set!")
		return
	}

	conn, err := redis.Dial("tcp", address)
	if err != nil {
		panic(err)
	}

	switch flag.Arg(0) {
	case "publish":
		if *N == 0 || *R == 0 {
			usage()
			return
		}
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			<-c
			stop = true
		}()
		publish(conn, *R, *N)
		// send exit
		if err := conn.Send("PUBLISH", "0", "exit"); err != nil {
			panic(err)
		}
		if err := conn.Flush(); err != nil {
			panic(err)
		}
	case "subscribe":
		if *N == 0 {
			usage()
			return
		}
		subscribe(conn, *N, *P)
	default:
		usage()
		return
	}
	rate := float64(total) / time.Now().Sub(startTime).Seconds()
	fmt.Printf("total msgs processed: %d, achieved rate: %.2f msgs/sec\n", total, rate)
}

func publish(conn redis.Conn, r, n int) {
	limiter := rate.NewLimiter(rate.Limit(float64(r)), 100)
	startTime = time.Now()
	for !stop {
		r := limiter.ReserveN(time.Now(), 1)
		if r.OK() {
			topic := strconv.Itoa(rand.Intn(n))
			if err := conn.Send("PUBLISH", topic, "update"); err != nil {
				panic(err)
			}
			if err := conn.Flush(); err != nil {
				panic(err)
			}
			total++
		}
		time.Sleep(r.Delay())
	}
}

func subscribe(conn redis.Conn, n, p int) {
	pubsub := &redis.PubSubConn{conn}
	exit := 0
	for i := 0; i < n; i++ {
		topic := strconv.Itoa(i)
		if err := pubsub.Subscribe(topic); err != nil {
			panic(err)
		}
		switch msg := pubsub.Receive().(type) {
		case error:
			panic(msg)
		}
	}
	fmt.Printf("subscribed to %d topics\n", n)
	for {
		switch msg := pubsub.Receive().(type) {
		case error:
			panic(msg)
		case redis.Message:
			if string(msg.Data) == "exit" {
				if exit++; exit == p {
					return
				}
			}
		}
		if total == 0 {
			startTime = time.Now()
		}
		total++
	}
}
