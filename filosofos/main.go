package main

import (
	"fmt"
	"time"
	"flag"
	"os"
	"strconv"
)

type State int

var forks[]chan bool

const (
	THINKING State = iota
	EATING
)

type philosopher struct {
	id int
	count int
	State State
}

func (p philosopher) think() {

	//Critical section
	fmt.Printf("Philosopher [%d] is THINKING\n", p.id)
	//Critical section

	time.Sleep(time.Second * 1)
}

func (p philosopher) eat() {
	
	//Critical Section
	fmt.Printf("Philosopher [%d] is EATING\n", p.id)
	p.count++;
	//Critical Section

	time.Sleep(time.Second * 1)
}

func (p philosopher) grabFork(fork int) {

}

func (p philosopher) releaseFork(fork int) {

}

type benchmark struct {
	option string
	N int
	R int
	avgTime float64
	totalActions []philosopher
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: filosofos [benchmark] [N] [R]\n")
	fmt.Fprintf(os.Stderr, "benchmark: deadlock, distinct1, distinct2\n")
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
	
	n, _ := strconv.Atoi(args[2])
	r, _ := strconv.Atoi(args[3])

	forks := make([]chan bool, n)
	var philosophers = make([]philosopher, n)
	for i := range forks {
		forks[i] = make(chan bool)
		philosophers[i] = philosopher { i, 0, THINKING }
	}

	switch option := args[1]; option {
	case "deadlock":
		for i := range forks {
			go fork(i)
			go deadlock(philosophers[i], r)
		}
	case "distinct1":
		for i := range forks {
			go fork(i)
			go distinct1(philosophers[i], r)
		}
	case "distinct2":
		for i := range forks {
			go fork(i)
			go distinct2(philosophers[i], r)
		}
	default:
		usage()
	}
}

func fork(i int) {
	for {
		forks[i] <- true //offer fork
		<- forks[i] 	   // give it back
	}
}

func deadlock(p philosopher, iter int) {
	for i := 0; i < iter; i++ {

	}
}

func distinct1(p philosopher, iter int) {
	for i := 0; i < iter; i++ {

	}
}

func distinct2(p philosopher, iter int) {
	for i:= 0; i < iter; i++ {

	}
}
