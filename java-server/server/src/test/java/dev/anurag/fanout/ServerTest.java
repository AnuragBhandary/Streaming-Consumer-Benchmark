package dev.anurag.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The real accept loop and connection threads, with events published directly (no Kafka). */
class ServerTest {
    private ServerSocket server;
    private ExecutorService connections;
    private StreamRegistry registry;
    private Stats stats;

    @BeforeEach
    void start() throws IOException {
        Config config = Config.fromMap(Map.of("FANOUT_QUEUE", "300", "FANOUT_RING", "100", "FANOUT_MAX_BATCH_BYTES", "4096"));
        registry = new StreamRegistry(config.ringCapacity());
        stats = new Stats();
        server = new ServerSocket();
        server.bind(new InetSocketAddress("127.0.0.1", 0));
        connections = Executors.newVirtualThreadPerTaskExecutor();
        Thread.ofVirtual().start(() -> Main.serve(server, connections, registry, stats, config));
    }

    @AfterEach
    void stop() throws IOException {
        server.close();
        connections.shutdownNow();
    }

    private record Client(Socket socket, BufferedReader in) {
        String line() throws IOException {
            return in.readLine();
        }
    }

    private Client connect(String command) throws IOException {
        Socket socket = new Socket("127.0.0.1", server.getLocalPort());
        socket.setSoTimeout(5000);
        OutputStream out = socket.getOutputStream();
        out.write((command + "\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
        return new Client(socket, new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)));
    }

    private void publish(String stream, long from, long to) throws IOException {
        for (long seq = from; seq <= to; seq++) {
            for (ClientConnection slow : registry.get(stream).publish(CoreTest.parsed(stream, seq))) {
                slow.kill();
            }
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met");
            }
            Thread.sleep(10);
        }
    }

    @Test
    void liveSubscriberReceivesEventsInOrder() throws Exception {
        publish("m", 1, 3);
        Client c = connect("SUB m -1");
        assertEquals("{\"type\":\"ok\",\"head\":3,\"reset\":false}", c.line());
        awaitTrue(() -> registry.get("m").subscriberCount() == 1);
        publish("m", 4, 6);
        for (long seq = 4; seq <= 6; seq++) {
            assertTrue(c.line().contains("\"seq\":" + seq));
        }
        c.socket().close();
        awaitTrue(() -> registry.get("m").subscriberCount() == 0 && stats.connections.get() == 0);
    }

    @Test
    void resumeReplaysFromCursorThenGoesLive() throws Exception {
        publish("m", 1, 10);
        Client c = connect("SUB m 7");
        assertEquals("{\"type\":\"ok\",\"head\":10,\"reset\":false}", c.line());
        for (long seq = 8; seq <= 10; seq++) {
            assertTrue(c.line().contains("\"seq\":" + seq));
        }
        publish("m", 11, 11);
        assertTrue(c.line().contains("\"seq\":11"));
    }

    @Test
    void cursorOlderThanBufferResets() throws Exception {
        publish("m", 1, 150); // ring holds the last 100
        Client c = connect("SUB m 3");
        assertEquals("{\"type\":\"ok\",\"head\":150,\"reset\":true}", c.line());
    }

    @Test
    void slowConsumerIsDisconnectedWithoutAffectingOthers() throws Exception {
        Client stuck = connect("SUB m -1"); // never reads after the ok line
        Client fast = connect("SUB m -1");
        assertTrue(fast.line().startsWith("{\"type\":\"ok\""));
        awaitTrue(() -> registry.get("m").subscriberCount() == 2);
        // Big events so the kernel buffers fill and the stuck client's queue overflows.
        String padding = "y".repeat(20_000);
        Thread reader = Thread.ofVirtual().start(() -> {
            try {
                while (fast.line() != null) {
                    // drain
                }
            } catch (IOException ignored) {
                // socket closed at test end
            }
        });
        for (long seq = 1; seq <= 10_000 && stats.slowDisconnects.sum() == 0; seq++) {
            byte[] raw = ("{\"stream\":\"m\",\"seq\":" + seq + ",\"payload\":\"" + padding + "\"}").getBytes(StandardCharsets.UTF_8);
            for (ClientConnection slow : registry.get("m").publish(EventParser.parse(raw))) {
                slow.kill();
            }
        }
        assertEquals(1, stats.slowDisconnects.sum());
        awaitTrue(() -> registry.get("m").subscriberCount() == 1); // only the fast one remains
        fast.socket().close();
        reader.join(5000);
        stuck.socket().close();
    }

    @Test
    void statsAndBadCommands() throws Exception {
        Client s = connect("STATS");
        String json = s.line();
        assertTrue(json.contains("\"impl\":\"java-virtual-threads\""), json);
        assertNull(s.line());
        for (String bad : new String[] {"HELLO", "SUB m", "SUB m notanumber"}) {
            Client c = connect(bad);
            assertTrue(c.line().contains("\"type\":\"error\""), bad);
        }
        Socket silent = new Socket("127.0.0.1", server.getLocalPort());
        silent.close(); // connects and leaves without a command
    }
}
