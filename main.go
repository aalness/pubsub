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
var P = flag.Int("p", 1, "number of publishers")
var S = flag.Int("s", 1, "number of subscribers")
var F = flag.Int("f", 50, "flush interval msecs")

func usage() {
	fmt.Printf("Usage:\n\n")
	fmt.Printf("  publish at rate R randomly across N topics with P publishers and F flush interval:\n")
	fmt.Printf("    -r <R> -n <N> [-p <P> -f <F>] publish\n\n")
	fmt.Printf("  subscribe to N different topics across S subscribers:\n")
	fmt.Printf("   -n <N> [-s <S>] subscribe\n\n")
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

	var total int
	var startTime time.Time

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
		fmt.Printf("publish: %d goroutines, %d topics, %d msgs/sec, %d ms flush interval\n",
			*P, *N, *R, *F)
		total, startTime = publish(address, *P, *N, *R, *F, done)
	case "subscribe":
		if *N == 0 {
			usage()
			return
		}
		fmt.Printf("subscribe: %d goroutines, %d topics\n", *S, *N)
		total, startTime = subscribe(address, *S, *N)
	default:
		usage()
		return
	}
	rate := float64(total) / time.Now().Sub(startTime).Seconds()
	fmt.Printf("total msgs processed: %d, achieved rate: %.2f msgs/sec\n", total, rate)
}

type publisher struct {
	total int
}

// start a single publisher
func (p *publisher) start(address string, n, r, f int, done chan struct{}, wg *sync.WaitGroup) (err error) {
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		return err
	}

	var mu sync.Mutex
	doneWrite := make(chan struct{}, 1)

	// start a flusher
	go func() {
		for {
			time.Sleep(time.Duration(f) * time.Millisecond)
			mu.Lock()
			if err := conn.Flush(); err != nil {
				mu.Unlock()
				return
			}
			mu.Unlock()
		}
	}()

	// start a drainer
	go func() {
		count := 0
		for {
			if _, err := conn.Receive(); err != nil {
				panic(err)
				return
			}
			count++
			select {
			case <-doneWrite:
				mu.Lock()
				if count == p.total {
					conn.Close()
					mu.Unlock()
					wg.Done()
					return
				}
				mu.Unlock()
			default:
			}
		}
	}()

	// start publishing
	go func() {
		limiter := rate.NewLimiter(rate.Limit(float64(r)), 100)
		for {
			r := limiter.ReserveN(time.Now(), 1)
			if r.OK() {
				topic := strconv.Itoa(rand.Intn(n))
				mu.Lock()
				if err := conn.Send("PUBLISH", topic, "update"); err != nil {
					panic(err)
				}
				p.total++
				mu.Unlock()
				select {
				case <-done:
					close(doneWrite)
					return
				default:
				}
			}
			time.Sleep(r.Delay())
		}
	}()
	return nil
}

// start publishing
func publish(address string, p, n, r, f int, done chan struct{}) (int, time.Time) {
	var wg sync.WaitGroup
	wg.Add(p)

	startTime := time.Now()
	publishers := make([]*publisher, p)
	for i := 0; i < p; i++ {
		publisher := &publisher{}
		if err := publisher.start(address, n, r, f, done, &wg); err != nil {
			panic(err)
		}
		publishers[i] = publisher
	}
	wg.Wait()

	total := 0
	for _, publisher := range publishers {
		total += publisher.total
	}
	return total, startTime
}

type subscriber struct {
	total     int
	startTime time.Time
}

// start polling with a single subscriber
func (s *subscriber) start(pubsub *redis.PubSubConn, done, read chan struct{}, wg *sync.WaitGroup) {
	switch msg := pubsub.Receive().(type) {
	case error:
		panic(msg)
		wg.Done()
		return
	}
	s.total++
	s.startTime = time.Now()
	go func() {
		<-done
		pubsub.Close()
	}()
	for {
		switch msg := pubsub.Receive().(type) {
		case error:
			fmt.Println(msg)
			wg.Done()
			return
		}
		s.total++
		read <- struct{}{}
	}
}

// start subscribing
func subscribe(address string, s, n int) (int, time.Time) {
	// create a connection per goroutine
	pubsubs := make([]*redis.PubSubConn, s)
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

	read := make(chan struct{}, 1000000)
	done := make(chan struct{}, 1)

	// after the first read giveup after 2s of inactivity
	go func() {
		<-read
		for {
			select {
			case <-read:
			case <-time.After(time.Second * 2):
				close(done)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(len(pubsubs))

	// start polling
	subscribers := make([]*subscriber, len(pubsubs))
	for i, pubsub := range pubsubs {
		subscriber := &subscriber{}
		subscribers[i] = subscriber
		go subscriber.start(pubsub, done, read, &wg)
	}
	wg.Wait()

	var startTime time.Time
	total := 0
	for i, subscriber := range subscribers {
		if i == 0 {
			startTime = subscriber.startTime
		}
		if subscriber.startTime.Before(startTime) {
			startTime = subscriber.startTime
		}
		total += subscriber.total
	}
	return total, startTime
}
