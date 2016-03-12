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
var B = flag.Int("b", 0, "number of publishes to include in a batch")

var total int
var startTime time.Time

var NUM_PUBLISHER_THREADS = 4
var NUM_SUBSCRIBER_THREADS = 16

func usage() {
	fmt.Printf("Usage:\n\n")
	fmt.Printf("  publish messages at rate R randomly across N different topics:\n")
	fmt.Printf("    -r <R> -n <N> -b <batch size if any> publish\n\n")
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
			close(done)
		}()
		publish(address, *R, *N, *B, done)
	case "subscribe":
		if *N == 0 || *P == 0 {
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
func publish(address string, r, n, b int, done chan struct{}) {
	// create a connection per thread
	conns := make([]redis.Conn, NUM_PUBLISHER_THREADS)
	for i := 0; i < len(conns); i++ {
		conn, err := redis.Dial("tcp", address)
		if err != nil {
			panic(err)
		}
		conns[i] = conn
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(conns))

	startTime = time.Now()
	for _, c := range conns {
		go func(conn redis.Conn) {
			count := 0
			limiter := rate.NewLimiter(rate.Limit(float64(r/len(conns))), 100)
			for {
				r := limiter.ReserveN(time.Now(), 1)
				if r.OK() {
					topic := strconv.Itoa(rand.Intn(n))
					if err := conn.Send("PUBLISH", topic, "update"); err != nil {
						panic(err)
					}
					count++
					// support batching
					if b == 0 || count%b == 0 {
						if err := conn.Flush(); err != nil {
							panic(err)
						}
						times := b
						if times == 0 {
							times = 1
						}
						for i := 0; i < times; i++ {
							if _, err := conn.Receive(); err != nil {
								panic(err)
							}
						}
					}
					func() {
						mu.Lock()
						defer mu.Unlock()
						total++
					}()
				}
				time.Sleep(r.Delay())
				select {
				case _, ok := <-done:
					if !ok {
						wg.Done()
					}
					return
				default:
				}
			}
		}(c)
	}
	wg.Wait()

	// send exit
	if _, err := conns[0].Do("PUBLISH", "0", "exit"); err != nil {
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
