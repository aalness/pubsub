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

var total int
var startTime time.Time
var stop bool

func usage() {
	fmt.Printf("Usage:\n\n")
	fmt.Printf("  publish messages at rate R randomly across N different topics:\n")
	fmt.Printf("    -r <R> -n <N> publish\n\n")
	fmt.Printf("  subscribe to N different topics:\n")
	fmt.Printf("    -n <N> subscribe\n\n")
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

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		stop = true
		rate := float64(total) / time.Now().Sub(startTime).Seconds()
		fmt.Printf("total msgs processed: %d, achieved rate: %.2f msgs/sec\n", total, rate)
		os.Exit(1)
	}()

	switch flag.Arg(0) {
	case "publish":
		if *N == 0 || *R == 0 {
			usage()
			return
		}
		publish(conn, *R, *N)
		rate := float64(total) / time.Now().Sub(startTime).Seconds()
		fmt.Printf("total msgs processed: %d, achieved rate: %.2f msgs/sec\n", total, rate)
	case "subscribe":
		if *N == 0 {
			usage()
			return
		}
		subscribe(conn, *N)
	default:
		usage()
	}
}

func publish(conn redis.Conn, r, n int) {
	limiter := rate.NewLimiter(rate.Limit(float64(r)), 100)
	startTime = time.Now()
	for !stop {
		r := limiter.ReserveN(time.Now(), 1)
		if r.OK() {
			topic := strconv.Itoa(rand.Intn(n))
			if err := conn.Send("PUBLISH", topic, "derp"); err != nil {
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

func subscribe(conn redis.Conn, n int) {
	pubsub := &redis.PubSubConn{conn}
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
		}
		if total == 0 {
			startTime = time.Now()
		}
		total++
	}
}
