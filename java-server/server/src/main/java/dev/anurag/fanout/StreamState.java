package dev.anurag.fanout;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One stream: its replay buffer, its subscribers and the newest seq seen.
 *
 * <p>{@code publish} (Kafka thread) and {@code attach} (connection threads) share one lock.
 * That is what makes joining race-free: a new subscriber's replay snapshot and its registration
 * for live events happen atomically, so every seq lands in exactly one of the two.
 *
 * <p>A {@link ReentrantLock} rather than {@code synchronized}: on Java 21 a virtual thread that
 * blocks inside a {@code synchronized} block pins its carrier thread (fixed in JDK 24, JEP 491).
 * Nothing here blocks on I/O, but the explicit lock keeps it safe by construction.
 */
final class StreamState {
    final String id;
    private final RingBuffer ring;
    private final Set<ClientConnection> subscribers = new LinkedHashSet<>();
    private final ReentrantLock lock = new ReentrantLock();
    private long head = 0;

    StreamState(String id, int ringCapacity) {
        this.id = id;
        this.ring = new RingBuffer(ringCapacity);
    }

    record Attach(long head, boolean reset, List<byte[]> replay) {}

    /**
     * Records the event and offers it to every subscriber without blocking. Returns the
     * subscribers whose queues overflowed; the caller disconnects them outside the lock.
     */
    List<ClientConnection> publish(Event event) {
        List<ClientConnection> slow = null;
        lock.lock();
        try {
            if (event.seq() <= head) {
                return List.of(); // Kafka redelivery: already published
            }
            head = event.seq();
            ring.add(event.seq(), event.line());
            for (ClientConnection c : subscribers) {
                if (!c.offer(event.line())) {
                    if (slow == null) {
                        slow = new ArrayList<>();
                    }
                    slow.add(c);
                }
            }
        } finally {
            lock.unlock();
        }
        return slow == null ? List.of() : slow;
    }

    /**
     * Registers a subscriber and returns what it must be sent first.
     * cursor &lt; 0: start live from now. Otherwise replay everything after cursor if the buffer
     * still holds it, else reset (the client restarts from the current head).
     */
    Attach attach(ClientConnection client, long cursor) {
        lock.lock();
        try {
            subscribers.add(client);
            if (cursor < 0 || cursor == head) {
                return new Attach(head, false, List.of());
            }
            if (cursor < head && ring.covers(cursor)) {
                return new Attach(head, false, ring.since(cursor));
            }
            return new Attach(head, true, List.of()); // too old, or ahead of the stream
        } finally {
            lock.unlock();
        }
    }

    void detach(ClientConnection client) {
        lock.lock();
        try {
            subscribers.remove(client);
        } finally {
            lock.unlock();
        }
    }

    int subscriberCount() {
        lock.lock();
        try {
            return subscribers.size();
        } finally {
            lock.unlock();
        }
    }

    long head() {
        lock.lock();
        try {
            return head;
        } finally {
            lock.unlock();
        }
    }
}
