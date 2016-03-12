package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/time/rate"
)

var R = flag.Int("r", 0, "rate to produce messages (per second)")
var N = flag.Int("n", 0, "number of topics")
var P = flag.Int("p", 0, "number of publishers")

var total int
var startTime time.Time

var NUM_SUBSCRIBER_THREADS = 16

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

	switch flag.Arg(0) {
	case "publish":
		if *N == 0 || *R == 0 {
			usage()
			return
		}
		c := make(chan os.Signal, 1)
		done := make(chan struct{}, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			<-c
			done <- struct{}{}
		}()
		publish(address, *R, *N, done)
	case "subscribe":
		if *N == 0 {
			usage()
			return
		}
		subscribe(address, *N, *P)
	default:
		usage()
		return
	}
	rate := float64(total) / time.Now().Sub(startTime).Seconds()
	fmt.Printf("total msgs processed: %d, achieved rate: %.2f msgs/sec\n", total, rate)
}

// publish at a fixed rate with a single thread
func publish(address string, r, n int, done chan struct{}) {
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	limiter := rate.NewLimiter(rate.Limit(float64(r)), 100)
	startTime = time.Now()
	ok := true
	for ok {
		r := limiter.ReserveN(time.Now(), 1)
		if r.OK() {
			topic := strconv.Itoa(rand.Intn(n))
			if _, err := conn.Do("PUBLISH", topic, "update"); err != nil {
				panic(err)
			}
			total++
		}
		time.Sleep(r.Delay())
		select {
		case <-done:
			ok = false
		default:
		}
	}
	// send exit
	if _, err := conn.Do("PUBLISH", "0", "exit"); err != nil {
		panic(err)
	}
}

// subscribe NUM_SUBSCRIBER_THREADS threads
func subscribe(address string, n, p int) {
	// create a connection per thread
	pubsubs := make([]*redis.PubSubConn, NUM_SUBSCRIBER_THREADS)
	for i := 0; i < len(pubsubs); i++ {
		conn, err := redis.Dial("tcp", address)
		if err != nil {
			panic(err)
		}
		pubsubs[i] = &redis.PubSubConn{conn}
	}
	// evenly split topics across connections
	for i := 0; i < n; i++ {
		topic := strconv.Itoa(i)
		pubsub := pubsubs[i%len(pubsubs)]
		if err := pubsub.Subscribe(topic); err != nil {
			panic(err)
		}
		switch msg := pubsub.Receive().(type) {
		case error:
			panic(msg)
		}
	}
	fmt.Printf("subscribed to %d topics\n", n)

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(pubsubs))

	// poll the connections
	for _, pubsub := range pubsubs {
		go func(pubsub *redis.PubSubConn) {
			exits := 0
			for {
				switch msg := pubsub.Receive().(type) {
				case error:
					fmt.Println(msg)
					wg.Done()
					return
				case redis.Message:
					if string(msg.Data) == "exit" {
						// only first thread gets exits
						if exits++; exits == p {
							// close all pubsubs
							for _, ps := range pubsubs {
								ps.Close()
							}
							wg.Done()
							return
						}
					}
				}
				func() {
					mu.Lock()
					defer mu.Unlock()
					if total == 0 {
						startTime = time.Now()
					}
					total++
				}()
			}
		}(pubsub)
	}
	wg.Wait()
}
