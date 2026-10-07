package main

import (
	"fmt"
	"flag"
	"os"
	"sync"
	"strconv"
)

var consumers[] int

type benchmark struct {
	option string
	bufsize int
	countPerConsumer int
	totalThroughput int
	avgBufOccupancy int
}

func (bench *benchmark) printResults() {
	fmt.Printf("--- Benchmark completed ---\nBuffer size: [%d]\nTotal throughput: [%d]\nItems consumed per consumer: [%d]\nAvg Buffer occupancy: [%d]",
							bench.bufsize, bench.totalThroughput, bench.countPerConsumer,  bench.avgBufOccupancy)
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
	p, _ := strconv.Atoi(args[2])
	c, _ := strconv.Atoi(args[3])

	if option == "channel" {
		ch := make(chan string, bufsize)
		consumers = make([]int, c)
		var prodWg sync.WaitGroup
		var consWg sync.WaitGroup

		for i := range p {
			prodWg.Add(1)
			go chanProducer(i, ch, &prodWg)
		}

		for i := range consumers {
			consWg.Add(1)
			consumers[i] = 0
			go chanConsumer(i, ch , &consWg)
		}

	}
	if option == "semaphore" {

	}
}

func chanProducer(id int, ch chan<- string, wg *sync.WaitGroup) {
    for i := 0; i < 3; i++ {
        item := fmt.Sprintf("P%d-item%d", id, i)
        fmt.Printf("[P%d] Produced: %s\n", id, item)
        ch <- item
    }
    wg.Done()
}

func chanConsumer(id int, ch <-chan string, wg *sync.WaitGroup) {
    for item := range ch {
        fmt.Printf("[C%d] Consumed: %s\n", id, item)
				consumers[id]++
    }
    wg.Done()
}
