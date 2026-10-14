package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	itemsPerProducer = 8 
	minWorkMs        = 10
	maxWorkMs        = 50
	sampleEvery      = 2 * time.Millisecond
)

var (
	K, P, C int

	buf      circularBuffer
	notFull  chan struct{}
	notEmpty chan struct{}

	produced []int
	consumed []int

	occupancy atomic.Int64
)

const (
	cReset = "\033[0m"
	cBold  = "\033[1m"
	cDim   = "\033[2m"
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
	if d >= time.Second {
		return fmt.Sprintf("%.2f s", d.Seconds())
	}
	return fmt.Sprintf("%.1f ms", float64(d.Nanoseconds())/1e6)
}

func modeTitle(mode string) string {
	switch mode {
	case "channel":
		return "CHANNEL"
	case "semaphore":
		return "SEMAPHORE"
	}
	return mode
}

func workTime() time.Duration {
	return time.Duration(rand.Intn(maxWorkMs-minWorkMs+1)+minWorkMs) * time.Millisecond
}

type circularBuffer struct {
	mu    sync.Mutex
	items []string
	in    int
	out   int
}

func (b *circularBuffer) put(item string) {
	b.mu.Lock()
	b.items[b.in] = item
	b.in = (b.in + 1) % len(b.items)
	b.mu.Unlock()
}

func (b *circularBuffer) get() string {
	b.mu.Lock()
	item := b.items[b.out]
	b.out = (b.out + 1) % len(b.items)
	b.mu.Unlock()
	return item
}

func chanProducer(id int, ch chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := range itemsPerProducer {
		time.Sleep(workTime())
		item := fmt.Sprintf("P%d-item%d", id, i)
		ch <- item
		occupancy.Add(1)
		produced[id]++
		fmt.Printf("[P%d] produziu %s (buffer %d/%d)\n", id, item, occupancy.Load(), K)
	}
}

func chanConsumer(id int, ch <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for item := range ch {
		occupancy.Add(-1)
		consumed[id]++
		fmt.Printf("[C%d] consumiu %s (buffer %d/%d)\n", id, item, occupancy.Load(), K)
		time.Sleep(workTime())
	}
}

func semProducer(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := range itemsPerProducer {
		time.Sleep(workTime())
		item := fmt.Sprintf("P%d-item%d", id, i)

		<-notFull
		buf.put(item)
		occupancy.Add(1)
		produced[id]++
		notEmpty <- struct{}{}

		fmt.Printf("[P%d] produziu %s (buffer %d/%d)\n", id, item, occupancy.Load(), K)
	}
}

func semConsumeOne(id int) {
	item := buf.get()
	occupancy.Add(-1)
	notFull <- struct{}{}
	consumed[id]++
	fmt.Printf("[C%d] consumiu %s (buffer %d/%d)\n", id, item, occupancy.Load(), K)
	time.Sleep(workTime())
}

func semConsumer(id int, wg *sync.WaitGroup, done <-chan struct{}) {
	defer wg.Done()
	for {
		select {
		case <-notEmpty:
			semConsumeOne(id)
		case <-done:
			for {
				select {
				case <-notEmpty:
					semConsumeOne(id)
				default:
					return
				}
			}
		}
	}
}

type occupancyStats struct {
	sum     int64
	samples int64
}

func monitorOccupancy(st *occupancyStats, stop <-chan struct{}) {
	t := time.NewTicker(sampleEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			st.sum += occupancy.Load()
			st.samples++
		}
	}
}

func printReport(mode string, elapsed time.Duration, st occupancyStats) {
	totalProduced, totalConsumed := 0, 0
	for _, v := range produced {
		totalProduced += v
	}
	for _, v := range consumed {
		totalConsumed += v
	}

	avgOcc := 0.0
	if st.samples > 0 {
		avgOcc = float64(st.sum) / float64(st.samples)
	}

	fmt.Println()
	banner(cCyan, "RESULTADOS — "+modeTitle(mode),
		fmt.Sprintf("execução em %s", fmtDur(elapsed)))
	fmt.Println()

	kv("Throughput total", fmt.Sprintf("%d itens em %s (%.1f itens/s)",
		totalConsumed, fmtDur(elapsed), float64(totalConsumed)/elapsed.Seconds()))
	fmt.Println()
	fmt.Println("  Itens consumidos por consumidor:")
	for i, v := range consumed {
		kv(fmt.Sprintf("     Consumidor %d", i), fmt.Sprintf("%d itens", v))
	}
	kv("     Média por consumidor", fmt.Sprintf("%.2f itens", float64(totalConsumed)/float64(C)))
	fmt.Println()
	kv("Ocupação média do buffer", fmt.Sprintf("%.2f de %d vagas (%.1f%%)",
		avgOcc, K, avgOcc/float64(K)*100))
	kv("Total produzido", fmt.Sprintf("%d itens", totalProduced))
	fmt.Println()
}

func usage() {
	fmt.Fprintln(os.Stderr, "uso: prodcon <modo> <K> <P> <C>")
	fmt.Fprintln(os.Stderr, "  modo   channel | semaphore")
	fmt.Fprintln(os.Stderr, "  K      tamanho do buffer")
	fmt.Fprintln(os.Stderr, "  P      produtores")
	fmt.Fprintln(os.Stderr, "  C      consumidores")
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 4 {
		usage()
	}
	mode := args[0]
	switch mode {
	case "channel", "semaphore":
	default:
		usage()
	}

	K, _ = strconv.Atoi(args[1])
	P, _ = strconv.Atoi(args[2])
	C, _ = strconv.Atoi(args[3])

	banner(cCyan, "PRODUTOR-CONSUMIDOR — "+modeTitle(mode),
		fmt.Sprintf("buffer K=%d · %d produtores · %d consumidores · %d itens por produtor",
			K, P, C, itemsPerProducer))
	fmt.Println()

	produced = make([]int, P)
	consumed = make([]int, C)

	var prodWg, consWg sync.WaitGroup
	var st occupancyStats
	stopMonitor := make(chan struct{})
	monitorDone := make(chan struct{})
	done := make(chan struct{})

	start := time.Now()
	go func() {
		defer close(monitorDone)
		monitorOccupancy(&st, stopMonitor)
	}()

	switch mode {
	case "channel":
		ch := make(chan string, K)
		for i := range P {
			prodWg.Add(1)
			go chanProducer(i, ch, &prodWg)
		}
		for i := range C {
			consWg.Add(1)
			go chanConsumer(i, ch, &consWg)
		}
		go func() {
			prodWg.Wait()
			close(ch)
		}()
	case "semaphore":
		buf = circularBuffer{items: make([]string, K)}
		notFull = make(chan struct{}, K)
		for range K {
			notFull <- struct{}{}
		}
		notEmpty = make(chan struct{}, K)
		for i := range P {
			prodWg.Add(1)
			go semProducer(i, &prodWg)
		}
		for i := range C {
			consWg.Add(1)
			go semConsumer(i, &consWg, done)
		}
		go func() {
			prodWg.Wait()
			close(done)
		}()
	}

	consWg.Wait()
	elapsed := time.Since(start)

	close(stopMonitor)
	<-monitorDone

	printReport(mode, elapsed, st)
}
