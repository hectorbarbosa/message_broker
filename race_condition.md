# Race Condition in Message Broker

## The Issue

The original implementation had a race condition between `Enqueue` and `Dequeue` when a waiter timed out.

### Original Code Flow

**Enqueue (lines 66-76):**
```go
if len(q.waiters) > 0 {
    waiter := q.waiters[0]
    q.waiters = q.waiters[1:]
    select {
    case waiter <- msg:
    default:
        // Receiver timed out, message goes to queue
        q.messages = append(q.messages, msg)
    }
    close(waiter)
    return
}
```

**Dequeue timeout (lines 103-118):**
```go
case <-time.After(timeout):
    q.mu.Lock()
    for i, w := range q.waiters {
        if w == waiter {
            q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
            close(waiter)
            break
        }
    }
    q.mu.Unlock()
    <-waiter
    return "", false
```

### Race Scenario

1. `Enqueue` locks, takes waiter from slice, removes it, unlocks
2. Before `Enqueue` sends, `Dequeue` times out, locks, doesn't find waiter (already removed), unlocks, closes channel
3. `Enqueue` tries `waiter <- msg` on closed channel → **panic: send on closed channel**

Or the reverse:
1. `Dequeue` times out, closes channel
2. `Enqueue` tries to send on closed channel → **panic**

Or a subtle data loss:
1. `Enqueue` sends to buffered channel, closes it
2. `Dequeue` timeout fires, closes channel (already closed - double close panic)
3. Even if no panic, the message might be lost if the receiver already returned

## The Solution

Track whether a waiter has been served using a flag, and hold the lock during the send operation.

### Fixed Code

**Added waiter struct:**
```go
type waiter struct {
    ch     chan string
    served bool // true if a message has been sent to this waiter
}
```

**Enqueue holds lock during send:**
```go
func (q *Queue) Enqueue(msg string) {
    q.mu.Lock()
    defer q.mu.Unlock()

    if len(q.waiters) > 0 {
        w := q.waiters[0]
        q.waiters = q.waiters[1:]
        w.served = true
        w.ch <- msg // send while holding lock
        return
    }

    q.messages = append(q.messages, msg)
}
```

**Dequeue checks served flag on timeout:**
```go
case <-time.After(timeout):
    q.mu.Lock()
    defer q.mu.Unlock()
    
    // check if we were served while timing out
    if w.served {
        msg := <-w.ch
        return msg, true
    }
    
    // remove from waiters if still there
    for i, waiter := range q.waiters {
        if waiter == w {
            q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
            break
        }
    }
    return "", false
```

### Why This Works

1. **No closed channel sends**: We never close the channel, eliminating the panic
2. **Lock during send**: `Enqueue` holds the lock while sending, so timeout can't interfere
3. **Served flag**: If timeout fires while message is being sent, the lock ensures atomicity - either the message is fully sent (served=true) or not. If served, the waiter reads the message instead of returning 404
4. **No race windows**: The lock serializes all operations on the waiter state

## Key Takeaway

When coordinating between concurrent operations with timeouts, use explicit state tracking (flags) and hold locks during critical operations to prevent race conditions.
