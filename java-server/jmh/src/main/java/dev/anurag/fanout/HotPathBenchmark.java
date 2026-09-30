package dev.anurag.fanout;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Micro-benchmarks of the server's per-event hot path, isolated from the network and Kafka.
 *
 * <p>Warm-up is the point of using JMH: 5 warm-up iterations let the JIT compile and optimise
 * the hot methods before anything is measured, and 2 forks (fresh JVMs) guard against one
 * lucky or unlucky compilation. `python -m bench.microbench` measures the same operations in
 * the Python server.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Thread)
public class HotPathBenchmark {
    static byte[] event(long seq) {
        return ("{\"stream\":\"s42\",\"seq\":" + seq + ",\"sent_at\":1790000000123.456,\"payload\":\""
                + "x".repeat(200) + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    private byte[] raw;

    @Param({"10", "100", "1000"})
    public int subscribers;

    private StreamState stream;
    private List<ClientConnection> connections;
    private Event event;
    private long seq;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        raw = event(1);
        Config config = Config.fromMap(Map.of("FANOUT_QUEUE", "100000"));
        StreamRegistry registry = new StreamRegistry(10_000);
        stream = registry.get("s42");
        connections = new ArrayList<>();
        for (int i = 0; i < subscribers; i++) {
            ClientConnection c = new ClientConnection(new Socket(), registry, new Stats(), config);
            connections.add(c);
            stream.attach(c, -1);
        }
        event = EventParser.parse(raw);
    }

    @Setup(Level.Iteration)
    public void drain() {
        connections.forEach(ClientConnection::drainQueue);
    }

    @TearDown(Level.Iteration)
    public void drainAfter() {
        connections.forEach(ClientConnection::drainQueue);
    }

    /** Parse one Kafka record value (Jackson streaming parser). */
    @Benchmark
    public Event parse() throws IOException {
        return EventParser.parse(raw);
    }

    /** Publish one event: ring buffer append + a non-blocking offer to every subscriber's queue. */
    @Benchmark
    public void publish(Blackhole bh) {
        seq++;
        bh.consume(stream.publish(new Event(event.stream(), seq, event.line())));
        if (seq % 50_000 == 0) {
            connections.forEach(ClientConnection::drainQueue);
        }
    }
}
