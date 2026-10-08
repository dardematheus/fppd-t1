package main

import (
	"fmt"
	"flag"
	"os"
	"sync"
	"strconv"
	"time"
)

var consumers[] int
var producers[] int

type sharedBuffer struct {
	sharedBuffer[] string
	out, in, size int
}

var buf sharedBuffer
var notEmpty chan struct{}
var notFull chan struct{}
var mutex chan struct{}

type benchmark struct {
	option string
	bufsize int
	countPerConsumer int
	totalThroughput int
	avgBuffOccupancy int
	totalConsumed int
	totalProduced int
}

func (bench *benchmark) printResults() {
	fmt.Printf("--- Benchmark completed ---\nBuffer size: [%d]\nTotal throughput: [%d]\nItems consumed per consumer: [%d]\nAvg Buffer occupancy: [%d]",
							bench.bufsize, bench.totalThroughput, bench.countPerConsumer,  bench.avgBuffOccupancy)
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: prodcon [benchmark] [K] [P] [C]\n")
	fmt.Fprintf(os.Stderr, "Benchmarks: [channel, semaphore]\n")
	fmt.Fprintf(os.Stderr, "K: Buffer size | P: Producers | C: Consumers\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()

	if len(args) != 4 {
		usage()
	}

	option := args[0]
	bufsize, _ := strconv.Atoi(args[1])
	p, _ := strconv.Atoi(args[2]) // prod 
	c, _ := strconv.Atoi(args[3]) // cons

	consumers = make([]int, c)
	producers = make([]int, p)
	buf = sharedBuffer { make([]string, bufsize), 0, 0, bufsize}
	notEmpty = make(chan struct{})
	notFull = make(chan struct{}, bufsize)
	mutex = make(chan struct{})

	var prodWg sync.WaitGroup
	var consWg sync.WaitGroup

	if option == "channel" {
		ch := make(chan string, bufsize)

		for i := range p {
			prodWg.Add(1)
			go chanProducer(i, ch, &prodWg)
		}

		for i := range consumers {
			consWg.Add(1)
			consumers[i] = 0
			go chanConsumer(i, ch, &consWg)
		}

	}
	if option == "semaphore" {

		for i := range p {
			prodWg.Add(1)
			go semProducer(i, &prodWg)
		}

		for i := range c - 1 {
			consWg.Add(1)
			consumers[i] = 0
			go semConsumer(i, &consWg)
		}
		consWg.Add(1)
		consumers[c] = 0
		go semConsumerSelect(c, &consWg)
	}

	consWg.Wait()
	prodWg.Wait()

	var totalConsumed int
	var totalProduced int
	for i := range p {
		totalProduced += producers[i]
	}
	for i:= range c {
		totalConsumed += consumers[i]
	}
}

func chanProducer(id int, ch chan<- string, wg *sync.WaitGroup) {
  defer wg.Done()
	for i := range consumers {
      item := fmt.Sprintf("P%d-item%d", id, i)
      fmt.Printf("[P%d] Produced: %s\n", id, item)
      ch <- item
			producers[id]++
  }
}

func chanConsumer(id int, ch <-chan string, wg *sync.WaitGroup) {
  defer wg.Done()
	for item := range ch {
      fmt.Printf("[C%d] Consumed: %s\n", id, item)
			consumers[id]++
  }
}

func semProducer(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := range 3 {
		<-notFull
		mutex <- struct{}{}

		item := fmt.Sprintf("P%d-item%d", id, i)
		fmt.Printf("[P%d Produced: %s\n]", id, item)
		buf.sharedBuffer[buf.in] = item
		producers[id]++
		buf.in = (buf.in + 1) & buf.size

		<-mutex 
		notEmpty <- struct{}{}
	}
}

func semConsumer(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		<-notEmpty
		mutex <- struct{}{}

		item := buf.sharedBuffer[buf.out]
		fmt.Printf("[C%d] Consumed: %s\n", id, item)
		consumers[id]++
		buf.out = (buf.out + 1) % buf.size

		<-mutex
		notFull <- struct{}{}
	}
}

// TEMPO ALEATORIO JANTAR FILOSOFOS E CONSUMER PRODUCER
func semConsumerSelect(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for range 5 {
		select {
		case <-notEmpty:
			mutex <- struct{}{}

			item := buf.sharedBuffer[buf.out]
			fmt.Printf("[C%d] Consumed: %s\n", id, item)
			consumers[id]++
			buf.out = (buf.out + 1) % buf.size

			<-mutex
			notFull <- struct{}{}
		case <-time.After(time.Second):
			fmt.Printf("[C%d] Entered Timeout: Buffer empty for more than one second", id)
		}
	}
}
