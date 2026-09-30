package dev.anurag.fanout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka to TCP fan-out server: one platform thread polls Kafka, one virtual thread per client
 * connection. A thread per connection with plain blocking I/O; no selector, no callbacks.
 */
public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {}

    public static void main(String[] args) throws IOException {
        Config config = Config.fromEnv();
        Stats stats = new Stats();
        StreamRegistry registry = new StreamRegistry(config.ringCapacity());

        stats.shards = config.consumers();
        java.util.List<KafkaSource> sources = new java.util.ArrayList<>();
        for (int shard = 0; shard < config.consumers(); shard++) {
            KafkaSource source = new KafkaSource(config, registry, stats, shard, config.consumers());
            sources.add(source);
            Thread.ofPlatform().name("kafka-poll-" + shard).start(source);
        }

        try (ServerSocket server = new ServerSocket();
                ExecutorService connections = Executors.newVirtualThreadPerTaskExecutor()) {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(config.port()), 8192);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                sources.forEach(KafkaSource::stop);
                try {
                    server.close();
                } catch (IOException ignored) {
                    // exiting
                }
            }));
            log.info("listening on {} (virtual thread per connection)", config.port());
            serve(server, connections, registry, stats, config);
        }
    }

    static void serve(
            ServerSocket server,
            ExecutorService connections,
            StreamRegistry registry,
            Stats stats,
            Config config) {
        while (!server.isClosed()) {
            try {
                Socket socket = server.accept();
                connections.submit(new ClientConnection(socket, registry, stats, config));
            } catch (IOException e) {
                if (!server.isClosed()) {
                    log.warn("accept failed: {}", e.getMessage());
                }
            }
        }
    }
}
