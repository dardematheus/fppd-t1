package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
	"unicode/utf8"
)

const (
	minIntervalMs = 67
	maxIntervalMs = 267

	deadlockNap    = 2 * time.Second
	deadlockDetect = 5 * time.Second
)

var (
	N     int
	forks []chan bool
)

const (
	cReset = "\033[0m"
	cBold  = "\033[1m"
	cDim   = "\033[2m"
	cRed   = "\033[31m"
	cCyan  = "\033[36m"
)

var useColor = isTTY(os.Stdout)

func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func paint(color, s string) string {
	if !useColor {
		return s
	}
	return color + s + cReset
}


func banner(color string, lines ...string) {
	const pad = 2
	w := 0
	for _, l := range lines {
		if n := utf8.RuneCountInString(l); n > w {
			w = n
		}
	}
	inner := w + pad*2
	var b strings.Builder
	b.WriteString(paint(color, "╭"+strings.Repeat("─", inner)+"╮") + "\n")
	for _, l := range lines {
		n := utf8.RuneCountInString(l)
		left := (inner - n) / 2
		right := inner - n - left
		b.WriteString(paint(color, "│") + strings.Repeat(" ", left) + l + strings.Repeat(" ", right) + paint(color, "│") + "\n")
	}
	b.WriteString(paint(color, "╰"+strings.Repeat("─", inner)+"╯"))
	fmt.Println(b.String())
}

func kv(label, value string) {
	const width = 34
	dots := strings.Repeat(".", width-utf8.RuneCountInString(label))
	fmt.Printf("  %s %s %s\n", label, paint(cDim, dots), paint(cBold, value))
}

func fmtDur(d time.Duration) string {
	return fmt.Sprintf("%.3f ms", float64(d.Nanoseconds())/1e6)
}

func modeTitle(mode string) string {
	switch mode {
	case "deadlock":
		return "DEADLOCK"
	case "hierarchy":
		return "HIERARCHY"
	case "tanenbaum":
		return "TANENBAUM"
	}
	return mode
}

type State int

const (
	THINKING State = iota
	HUNGRY
	EATING
)

type philosopher struct {
	id    int
	meals int
	wait  time.Duration
}

func randInterval() time.Duration {
	return time.Duration(rand.Intn(maxIntervalMs-minIntervalMs+1)+minIntervalMs) * time.Millisecond
}

func (p *philosopher) think() {
	fmt.Printf("Filósofo %d está pensando\n", p.id)
	time.Sleep(randInterval())
}

func (p *philosopher) eat() {
	fmt.Printf("Filósofo %d está comendo (refeição %d)\n", p.id, p.meals+1)
	p.meals++
	time.Sleep(randInterval())
}

func fork(i int) {
	for {
		forks[i] <- true
		<-forks[i]
	}
}

func (p *philosopher) grabForksDeadlock() {
	<-forks[p.id]
	time.Sleep(deadlockNap)
	<-forks[(p.id+1)%N]
}

func (p *philosopher) grabForksHierarchy() {
	first, second := p.id, (p.id+1)%N
	if second < first {
		first, second = second, first
	}
	<-forks[first]
	<-forks[second]
}

func (p *philosopher) releaseForks() {
	forks[p.id] <- true
	forks[(p.id+1)%N] <- true
}

func dineForks(mode string, wg *sync.WaitGroup, p *philosopher, iter int) {
	defer wg.Done()
	for range iter {
		p.think()

		start := time.Now()
		if mode == "deadlock" {
			p.grabForksDeadlock()
		} else {
			p.grabForksHierarchy()
		}
		p.wait += time.Since(start)

		p.eat()
		p.releaseForks()
	}
}

type table struct {
	mu    sync.Mutex
	state []State
	sem   []chan struct{}
}

func newTable(n int) *table {
	t := &table{state: make([]State, n), sem: make([]chan struct{}, n)}
	for i := range t.sem {
		t.sem[i] = make(chan struct{}, 1)
	}
	return t
}

func (t *table) takeForks(i int) {
	t.mu.Lock()
	t.state[i] = HUNGRY
	t.test(i)
	t.mu.Unlock()
	<-t.sem[i]
}

func (t *table) putForks(i int) {
	t.mu.Lock()
	t.state[i] = THINKING
	t.test((i + N - 1) % N)
	t.test((i + 1) % N)
	t.mu.Unlock()
}

func (t *table) test(i int) {
	left, right := (i+N-1)%N, (i+1)%N
	if t.state[i] == HUNGRY && t.state[left] != EATING && t.state[right] != EATING {
		t.state[i] = EATING
		t.sem[i] <- struct{}{}
	}
}

func dineTanenbaum(wg *sync.WaitGroup, t *table, p *philosopher, iter int) {
	defer wg.Done()
	for range iter {
		p.think()

		start := time.Now()
		t.takeForks(p.id)
		p.wait += time.Since(start)

		p.eat()
		t.putForks(p.id)
	}
}

func waitOrDeadlock(wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(deadlockDetect):
		fmt.Println()
		banner(cRed, "DEADLOCK DETECTADO",
			"cada filósofo segura o garfo esquerdo",
			"e espera o direito do vizinho — o jantar congela")
		os.Exit(1)
	}
}

func printReport(mode string, phis []philosopher) {
	fmt.Println()
	banner(cCyan, "RELATÓRIO — "+modeTitle(mode))
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 3, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Filósofo\tRefeições\tEspera média pelos garfos")
	var totalMeals int
	var totalWait time.Duration
	for _, p := range phis {
		fmt.Fprintf(w, "%d\t%d\t%s\n", p.id, p.meals, fmtDur(p.wait/time.Duration(max(p.meals, 1))))
		totalMeals += p.meals
		totalWait += p.wait
	}
	w.Flush()

	fmt.Println("  " + strings.Repeat("─", 46))
	kv("Refeições totais", strconv.Itoa(totalMeals))
	kv("Espera média global pelos garfos", fmtDur(totalWait/time.Duration(max(totalMeals, 1))))
	fmt.Println()
}

func usage() {
	fmt.Fprintln(os.Stderr, "uso: filosofos <modo> <N> <R>")
	fmt.Fprintln(os.Stderr, "  modo   deadlock | hierarchy | tanenbaum")
	fmt.Fprintln(os.Stderr, "  N      número de filósofos")
	fmt.Fprintln(os.Stderr, "  R      refeições por filósofo")
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 3 {
		usage()
	}
	mode := args[0]
	switch mode {
	case "deadlock", "hierarchy", "tanenbaum":
	default:
		usage()
	}

	N, _ = strconv.Atoi(args[1])
	r, _ := strconv.Atoi(args[2])

	banner(cCyan, "JANTAR DOS FILÓSOFOS — "+modeTitle(mode),
		fmt.Sprintf("%d filósofos · %d refeições cada", N, r))
	fmt.Println()

	philosophers := make([]philosopher, N)
	for i := range philosophers {
		philosophers[i].id = i
	}

	var wg sync.WaitGroup
	if mode == "tanenbaum" {
		t := newTable(N)
		for i := range philosophers {
			wg.Add(1)
			go dineTanenbaum(&wg, t, &philosophers[i], r)
		}
		wg.Wait()
	} else {
		forks = make([]chan bool, N)
		for i := range forks {
			forks[i] = make(chan bool)
			go fork(i)
		}
		for i := range philosophers {
			wg.Add(1)
			go dineForks(mode, &wg, &philosophers[i], r)
		}
		if mode == "deadlock" {
			waitOrDeadlock(&wg)
		} else {
			wg.Wait()
		}
	}

	printReport(mode, philosophers)
}
