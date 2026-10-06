package main

import (
	"fmt"
	"time"
	"flag"
	"os"
	"strconv"
	"sync"
)

type State int
var forks[]chan bool
var N int

const (
	THINKING State = iota
	EATING
)

type philosopher struct {
	id int
	count int
	State State
}

type TimeTracker struct {
	mu sync.Mutex
	avgTime time.Duration
}

type benchmark struct {
	option string
	N int
	R int
	tracker time.Duration
	totalCount []int
}

func (p *philosopher) think() {
	fmt.Printf("Philosopher [%d] is THINKING\n", p.id)
	time.Sleep(time.Second * 1)
}

func (p *philosopher) eat() {
	fmt.Printf("Philosopher [%d] is EATING\n", p.id)
	p.count++;
	time.Sleep(time.Second * 1)
}

func (p *philosopher) grabForkDeadlock(fork int) {
	<-forks[fork]
	time.Sleep(time.Second * 2)
	<-forks[(fork + 1) % N]
}

func (p *philosopher) releaseFork(fork int) {
	forks[fork] <- true
	forks[(fork + 1) % N] <- true
}

func (tracker *TimeTracker) inc(t time.Duration) {
	tracker.mu.Lock()
	tracker.avgTime += t 
	tracker.mu.Unlock()
}

func (bench *benchmark) printResults() {
	fmt.Printf("--- Benchmark complete ---\n[Option]: \t%s\n[Philosophers]: \t%d\n[Iterations]: \t%d\n[Average Wait Time]: \t%.6f\n--------------------------\n", bench.option, bench.N, bench.R, bench.tracker)
	
	for i := 0; i < bench.N; i++ {
		fmt.Printf("[Philosopher] - %d | [Meals] - %d", i, bench.totalCount[i])
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: filosofos [benchmark] [N] [R]\n")
	fmt.Fprintf(os.Stderr, "benchmark: deadlock, distinct1, distinct2\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	wg := new(sync.WaitGroup)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 4 {
		usage()
	}
	
	N, _ := strconv.Atoi(args[2])
	r, _ := strconv.Atoi(args[3])

	forks := make([]chan bool, N)
	var philosophers = make([]philosopher, N)
	for i := range forks {
		wg.Add(1)
		forks[i] = make(chan bool)
		philosophers[i] = philosopher { i, 0, THINKING }
	}

	tracker := TimeTracker {
		sync.Mutex{}, time.Duration(time.Second * 0),
	}

	switch option := args[1]; option {
	case "deadlock":
		for i := range forks {
			go fork(i)
			go deadlock(wg, &tracker, &philosophers[i], r, )
		}
	case "distinct1":
		for i := range forks {
			go fork(i)
			go distinct1(wg, &philosophers[i], r)
		}
	case "distinct2":
		for i := range forks {
			go fork(i)
			go distinct2(wg, &philosophers[i], r)
		}
	default:
		usage()
	}

	wg.Wait()

	bench := benchmark {
		args[1], N, r, tracker.avgTime / time.Duration(N), make([]int, N),
	}
	for i := range philosophers {
		bench.totalCount[philosophers[i].id] = philosophers[i].count
	}
	bench.printResults()
}

func fork(i int) {
	for {
		forks[i] <- true // offer fork
		<- forks[i] 	   // give it back
	}
}

func deadlock(wg *sync.WaitGroup, tracker *TimeTracker, p *philosopher, iter int) {
	defer wg.Done()
	var avgPWaitTime time.Duration
	for i := 0; i < iter; i++ {
		p.think()

		start := time.Now()
		p.grabForkDeadlock(p.id)
		avgPWaitTime = time.Since(start)

		p.eat()
		p.releaseFork(p.id)
	}

	tracker.inc(avgPWaitTime / time.Duration(iter))
}

func distinct1(wg *sync.WaitGroup, p *philosopher, iter int) {
	defer wg.Done()
	for i := 0; i < iter; i++ {
		p.think()
		p.eat()
	}
}

func distinct2(wg *sync.WaitGroup, p *philosopher, iter int) {
	defer wg.Done()
	for i := 0; i < iter; i++ {
		p.think()
		p.eat()
	}
}
