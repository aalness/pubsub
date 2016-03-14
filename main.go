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
var B = flag.Int("b", 1, "number of messages to include in a batch")
var S = flag.Int("s", 0, "number of start messages to wait before publishing")
var C = flag.Bool("c", false, "reset counter")

var NUM_PUBLISHER_THREADS = 4
var NUM_SUBSCRIBER_THREADS = 16

func usage() {
	fmt.Printf("Usage:\n\n")
	fmt.Printf("  after S start messages publish messages at rate R randomly across N topics for which it also subscribes:\n")
	fmt.Printf("    -r <R> -n <N> -s <S> -b <batch size if any> publish\n\n")
}

const letterBytes = "abcdef0123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randHexString(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

// measure latencies of at least our own messages
type latencyMeter struct {
	sent map[string]time.Time
	received map[string]time.Time
	mu sync.Mutex
}

func (lm *latencyMeter) markSent(msg string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.sent[msg] = time.Now()
}

func (lm *latencyMeter) markReceived(msg string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if _, ok := lm.sent[msg]; ok {
		lm.received[msg] = time.Now()
	}
}

func (lm *latencyMeter) stats() (min, mean, max int64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for msg, received := range lm.received {
		diff := received.Sub(lm.sent[msg])
		if min == 0 || diff.Nanoseconds() < min {
			min = diff.Nanoseconds()
		}
		if max == 0 || diff.Nanoseconds() > max {
			max = diff.Nanoseconds()
		}
		mean += diff.Nanoseconds() / int64(time.Millisecond)
	}
	min /= int64(time.Millisecond)
	max /= int64(time.Millisecond)
	mean /= int64(len(lm.received))
	return
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

	rand.Seed(time.Now().UnixNano())
	
	switch flag.Arg(0) {
	case "publish":
		if *N == 0 || *R == 0 || *S == 0 {
			usage()
			return
		}
		c := make(chan os.Signal, 1)
		ready := make(chan struct{}, 1)
		done := make(chan struct{}, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			<-c
			close(done)
		}()
		latencyMeter := &latencyMeter{
			sent: make(map[string]time.Time),
			received: make(map[string]time.Time),
		}
		var totalReceived, totalSent int
		var startSubscribe, startPublish time.Time
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			totalSent, startPublish = publish(address, *S, *R, *N, *B, latencyMeter, ready, done)
			wg.Done()
		}()
		go func() {
			totalReceived, startSubscribe = subscribe(address, *N, *S, *C, latencyMeter, ready)
			wg.Done()
		}()
		wg.Wait()
		receiveRate := float64(totalReceived) / time.Now().Sub(startSubscribe).Seconds()
		sendRate := float64(totalSent) / time.Now().Sub(startPublish).Seconds()
		min, mean, max := latencyMeter.stats()
		fmt.Printf("total msgs sent: %d, achieved rate: %.2f msgs/sec\n", totalSent, sendRate)
		fmt.Printf("total msgs received: %d, achieved rate: %.2f msgs/sec\n", totalReceived, receiveRate)
		fmt.Printf("latency: min %d ms, mean %d ms, max %d ms\n", min, mean, max)
	default:
		usage()
		return
	}
}

// publish at a fixed rate with a single thread
func publish(address string, s, r, n, b int, lm *latencyMeter, ready, done chan struct{}) (total int, start time.Time) {
	// create a connection per thread
	conns := make([]redis.Conn, NUM_PUBLISHER_THREADS)
	for i := 0; i < len(conns); i++ {
		conn, err := redis.Dial("tcp", address)
		if err != nil {
			panic(err)
		}
		conns[i] = conn
	}
	
	// wait for all of the expected subscribers to subscribe
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	conn2, err := redis.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	pubsub := &redis.PubSubConn{conn}
	if err := pubsub.Subscribe("ready"); err != nil {
		panic(err)
	}
	switch msg := pubsub.Receive().(type) {
	case error:
		panic(msg)
	}
	close(ready)
	ready2 := false
	for !ready2 {
		switch msg := pubsub.Receive().(type) {
		case error:
			panic(msg)
		case redis.Message:
			num, err := conn2.Do("GET", "nsubscribers")
			if err != nil {
				panic(err)
			}
			numStr, _ := redis.String(num, err)
			n, _ := strconv.Atoi(numStr)
			if n == s {
				pubsub.Close()
				conn2.Close()
				ready2 = true
			}
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(conns))

	start = time.Now()
	for _, c := range conns {
		go func(conn redis.Conn) {
			count := 0
			limiter := rate.NewLimiter(rate.Limit(float64(r/len(conns))), 100)
			for {
				r := limiter.ReserveN(time.Now(), 1)
				if r.OK() {
					topic := strconv.Itoa(rand.Intn(n))
					msg := randHexString(64)
					lm.markSent(msg)
					if err := conn.Send("PUBLISH", topic, msg); err != nil {
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

	return
}

// subscribe NUM_SUBSCRIBER_THREADS threads
func subscribe(address string, n, p int, reset bool, lm *latencyMeter, ready chan struct{}) (total int, start time.Time) {
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

	<-ready
	
	// tell the publishers we're ready
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	if reset {
		if _, err := conn.Do("SET", "nsubscribers", 0); err != nil {
			panic(err)
		}
	}
	if _, err := conn.Do("INCR", "nsubscribers"); err != nil {
		panic(err)
	}
	if _, err := conn.Do("PUBLISH", "ready", "ready"); err != nil {
		panic(err)
	}
	conn.Close()
	
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
					m := string(msg.Data)
					if m == "exit" {
						// only first thread gets exits
						if exits++; exits == p {
							// close all pubsubs
							for _, ps := range pubsubs {
								ps.Close()
							}
							wg.Done()
							return
						}
					} else {
						lm.markReceived(m)
					}
				}
				func() {
					mu.Lock()
					defer mu.Unlock()
					if total == 0 {
						start = time.Now()
					}
					total++
				}()
			}
		}(pubsub)
	}
	wg.Wait()
	return
}
