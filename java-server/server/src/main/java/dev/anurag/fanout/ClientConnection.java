package dev.anurag.fanout;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * One subscriber, running on its own virtual thread with ordinary blocking code:
 * read the {@code SUB} line, send the replay, then loop "take from queue, write, flush".
 *
 * <p>Backpressure is the blocking {@code write}: when the client stops reading, the kernel buffer
 * fills, {@code write} blocks, and only this virtual thread parks. Its carrier thread goes off to
 * run other connections. The Kafka thread never waits for a client: {@link #offer} is non-blocking,
 * and a client whose queue overflows is disconnected (it resumes from its cursor).
 */
final class ClientConnection implements Runnable {
    private static final byte[] ERROR = "{\"type\":\"error\",\"reason\":\"expected: SUB <stream> <cursor>\"}\n"
            .getBytes(StandardCharsets.US_ASCII);

    private final Socket socket;
    private final StreamRegistry registry;
    private final Stats stats;
    private final ArrayBlockingQueue<byte[]> queue;
    private final int maxBatchBytes;
    private volatile boolean slow = false;

    ClientConnection(Socket socket, StreamRegistry registry, Stats stats, Config config) {
        this.socket = socket;
        this.registry = registry;
        this.stats = stats;
        this.queue = new ArrayBlockingQueue<>(config.queueCapacity());
        this.maxBatchBytes = config.maxBatchBytes();
    }

    /** Called by the Kafka thread while holding the stream lock: must never block. */
    boolean offer(byte[] line) {
        if (slow) {
            return true; // already being disconnected
        }
        if (!queue.offer(line)) {
            slow = true;
            stats.slowDisconnects.increment();
            return false;
        }
        return true;
    }

    /** Empties the queue (used by the JMH fan-out benchmark between invocations). */
    int drainQueue() {
        int n = queue.size();
        queue.clear();
        return n;
    }

    /** Disconnect from another thread; unblocks a write stuck on a full socket buffer. */
    void kill() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // closing anyway
        }
    }

    @Override
    public void run() {
        stats.connections.incrementAndGet();
        stats.accepted.increment();
        StreamState stream = null;
        try (socket) {
            socket.setTcpNoDelay(true);
            var in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII), 256);
            var out = new BufferedOutputStream(socket.getOutputStream(), maxBatchBytes);
            String command = in.readLine();
            if (command == null) {
                return;
            }
            if (command.equals("STATS")) {
                out.write(stats.toJson(registry).getBytes(StandardCharsets.US_ASCII));
                out.flush();
                return;
            }
            String[] parts = command.trim().split(" ");
            if (parts.length != 3 || !parts[0].equals("SUB")) {
                out.write(ERROR);
                out.flush();
                return;
            }
            long cursor;
            try {
                cursor = Long.parseLong(parts[2]);
            } catch (NumberFormatException e) {
                out.write(ERROR);
                out.flush();
                return;
            }
            stream = registry.get(parts[1]);
            StreamState.Attach attach = stream.attach(this, cursor);
            out.write(("{\"type\":\"ok\",\"head\":" + attach.head() + ",\"reset\":" + attach.reset() + "}\n")
                    .getBytes(StandardCharsets.US_ASCII));
            for (byte[] line : attach.replay()) {
                out.write(line);
            }
            out.flush();
            stats.delivered.add(attach.replay().size());
            watchForDisconnect(in, Thread.currentThread());
            writeLoop(out);
        } catch (IOException | InterruptedException e) {
            // client went away, or we killed it as a slow consumer
        } finally {
            if (stream != null) {
                stream.detach(this);
            }
            stats.connections.decrementAndGet();
        }
    }

    /**
     * The writer thread spends its life parked in {@code queue.take()}, so it would only notice a
     * departed client on its next write. A second virtual thread blocks on {@code read()} and
     * interrupts the writer on EOF. Two threads per connection cost almost nothing when they are
     * virtual: this is the blocking-style equivalent of an event loop's "connection lost".
     */
    private static void watchForDisconnect(BufferedReader in, Thread writer) {
        Thread.ofVirtual().start(() -> {
            try {
                while (in.read() != -1) {
                    // clients send nothing after SUB; ignore anything that arrives
                }
            } catch (IOException ignored) {
                // socket closed
            }
            writer.interrupt();
        });
    }

    private void writeLoop(OutputStream out) throws IOException, InterruptedException {
        while (!slow) {
            byte[] line = queue.take(); // parks this virtual thread until there is work
            int bytes = 0;
            int count = 0;
            do {
                out.write(line); // blocks (parks) when the client is slow: that is backpressure
                bytes += line.length;
                count++;
            } while (bytes < maxBatchBytes && (line = queue.poll()) != null);
            out.flush();
            stats.delivered.add(count);
            stats.flushes.increment();
        }
    }
}
