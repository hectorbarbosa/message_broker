package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// waiter represents a waiting GET request
type waiter struct {
	ch     chan string
	served bool // true if a message has been sent to this waiter
}

// Queue represents a single message queue with FIFO ordering
type Queue struct {
	messages []string
	waiters  []*waiter // channels for waiting GET requests (FIFO order)
	mu       sync.Mutex
}

// MessageBroker manages multiple named queues
type MessageBroker struct {
	queues map[string]*Queue
	mu     sync.RWMutex
}

// NewMessageBroker creates a new message broker instance
func NewMessageBroker() *MessageBroker {
	return &MessageBroker{
		queues: make(map[string]*Queue),
	}
}

// getOrCreateQueue returns existing queue or creates new one
func (mb *MessageBroker) getOrCreateQueue(name string) *Queue {
	mb.mu.RLock()
	if q, ok := mb.queues[name]; ok {
		mb.mu.RUnlock()
		return q
	}
	mb.mu.RUnlock()

	mb.mu.Lock()
	defer mb.mu.Unlock()
	if q, ok := mb.queues[name]; ok {
		return q
	}
	q := &Queue{
		messages: make([]string, 0),
		waiters:  make([]*waiter, 0),
	}
	mb.queues[name] = q
	return q
}

// enqueue adds a message to the queue, delivering to waiting receiver if available
func (q *Queue) DeliverOrEnqueue(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// if there are waiting receivers, deliver to the first one (FIFO order)
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w.served = true
		w.ch <- msg // send while holding lock, prevents race with timeout
		return
	}

	// no waiters, add to queue
	q.messages = append(q.messages, msg)
}

// dequeue removes and returns oldest message, blocking until available,
// timeout elapses, or ctx is cancelled (e.g. client disconnect).
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (string, bool) {
	q.mu.Lock()

	// if message available, return immediately
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return msg, true
	}

	// create waiter for this request (FIFO ordering)
	w := &waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()

	// nil channel stays inert in select, so "no timeout" is handled cleanly
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	select {
	case msg := <-w.ch:
		return msg, true
	case <-timer:
		return q.dropWaiter(w)
	case <-ctx.Done():
		return q.dropWaiter(w)
	}
}

// dropWaiter removes w from the waiters list unless it was already served
// concurrently by DeliverOrEnqueue. If served, the queued message is returned.
func (q *Queue) dropWaiter(w *waiter) (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if w.served {
		msg := <-w.ch
		return msg, true
	}
	for i, ww := range q.waiters {
		if ww == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
	return "", false
}

// global broker instance
var broker = NewMessageBroker()

// handlePut handles PUT /{queue}?v=message
func handlePut(w http.ResponseWriter, r *http.Request) {
	u, err := url.ParseRequestURI(r.URL.String())
	if err != nil {
		http.Error(w, "Invalid URI string", http.StatusBadRequest)
		return
	}

	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if len(u.Path) < 2 {
		http.Error(w, "Queue name required", http.StatusBadRequest)
		return
	}

	queueName := u.Path[1:] // strip leading /
	if queueName == "" {
		http.Error(w, "Queue name required", http.StatusBadRequest)
		return
	}

	msg := r.URL.Query().Get("v")
	if msg == "" {
		http.Error(w, "Message value 'v' required", http.StatusBadRequest)
		return
	}

	q := broker.getOrCreateQueue(queueName)
	q.DeliverOrEnqueue(msg)
	w.WriteHeader(http.StatusOK)
}

// handleGet handles GET /{queue}[?timeout=N]
func handleGet(w http.ResponseWriter, r *http.Request) {
	u, err := url.ParseRequestURI(r.URL.String())
	if err != nil {
		http.Error(w, "Invalid URI string", http.StatusBadRequest)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if len(u.Path) < 2 {
		http.Error(w, "Queue name required", http.StatusBadRequest)
		return
	}

	queueName := u.Path[1:] // strip leading /
	if queueName == "" {
		http.Error(w, "Queue name required", http.StatusBadRequest)
		return
	}

	// Parse timeout parameter
	var timeout time.Duration
	if t := u.Query().Get("timeout"); t != "" {
		var n int
		fmt.Sscanf(t, "%d", &n)
		timeout = time.Duration(n) * time.Second
	}

	q := broker.getOrCreateQueue(queueName)
	msg, ok := q.Dequeue(r.Context(), timeout)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(msg))
}

func newMessageServer(addr string) *http.Server {
	return &http.Server{
		Addr: addr,
	}
}

func main() {
	port := flag.Int("port", 8080, "Port to run the server on")
	flag.Parse()

	addr := ":" + strconv.Itoa(*port)
	s := newMessageServer(addr)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			handlePut(w, r)
		case http.MethodGet:
			handleGet(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	go func() {
		log.Printf("Listening and serving on %s\n", addr)
		s.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	<-sigCh
	log.Println("Received terminating signal, shutting down ...")
	s.Shutdown(ctx)
	cancel()
}
