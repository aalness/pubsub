package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/golang/time/rate"
	"gopkg.in/redis.v3"
)

var R = flag.Int("r", 0, "rate to produce messages (per second)")
var N = flag.Int("n", 0, "number of topics")

var RECEIVE_TIMEOUT_SECONDS = 2

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
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	publisher := false
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		<-c
		stop = true
		if !publisher {
			os.Exit(1)
		}
	}()

	switch flag.Arg(0) {
	case "publish":
		if *N == 0 || *R == 0 {
			usage()
			return
		}
		publisher = true
		publish(client, *R, *N)
	case "subscribe":
		if *N == 0 {
			usage()
			return
		}
		subscribe(client, *N)
	default:
		usage()
		return
	}

	offset := float64(RECEIVE_TIMEOUT_SECONDS)
	if publisher {
		offset = 0.0
	}
	rate := float64(total) / (time.Now().Sub(startTime).Seconds() - offset)
	fmt.Printf("total msgs processed: %d, achieved rate: %.2f msgs/sec\n", total, rate)
}

func publish(client *redis.Client, r, n int) {
	limiter := rate.NewLimiter(rate.Limit(float64(r)), 100)
	startTime = time.Now()
	for !stop {
		r := limiter.ReserveN(time.Now(), 1)
		if r.OK() {
			topic := strconv.Itoa(rand.Intn(n))
			if err := client.Publish(topic, "derp").Err(); err != nil {
				panic(err)
			}
			total++
		}
		time.Sleep(r.Delay())
	}
}

func subscribe(client *redis.Client, n int) {
	var pubsub *redis.PubSub
	for i := 0; i < n; i++ {
		topic := strconv.Itoa(i)
		var err error
		if pubsub == nil {
			pubsub, err = client.Subscribe(topic)
		} else {
			err = pubsub.Subscribe(topic)
		}
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("subscribed to %d topics\n", n)
	for {
		if total == 0 {
			if _, err := pubsub.ReceiveMessage(); err != nil {
				panic(err)
			}
			startTime = time.Now()
		} else {
			if _, err := pubsub.ReceiveTimeout(time.Duration(RECEIVE_TIMEOUT_SECONDS) * time.Second); err != nil {
				if strings.Contains(err.Error(), "i/o timeout") {
					return
				} else {
					panic(err)
				}
			}
		}
		total++
	}
}
