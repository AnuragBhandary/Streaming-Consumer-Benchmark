package dev.anurag.fanout;

import java.util.Map;

/** Runtime settings, read from {@code FANOUT_*} environment variables (same names as the Python server). */
public record Config(
        String kafkaBootstrap,
        String topic,
        int port,
        int queueCapacity,
        int ringCapacity,
        int maxBatchBytes,
        int writeBufferBytes,
        int socketSendBuffer,
        int consumers) {

    public static Config fromEnv() {
        return fromMap(System.getenv());
    }

    static Config fromMap(Map<String, String> env) {
        return new Config(
                env.getOrDefault("FANOUT_KAFKA", "localhost:9094"),
                env.getOrDefault("FANOUT_TOPIC", "events"),
                Integer.parseInt(env.getOrDefault("FANOUT_PORT", "7000")),
                Integer.parseInt(env.getOrDefault("FANOUT_QUEUE", "1000")),
                Integer.parseInt(env.getOrDefault("FANOUT_RING", "10000")),
                Integer.parseInt(env.getOrDefault("FANOUT_MAX_BATCH_BYTES", "65536")),
                Integer.parseInt(env.getOrDefault("FANOUT_WRITE_BUFFER_BYTES", "8192")),
                Integer.parseInt(env.getOrDefault("FANOUT_SNDBUF", "0")), // 0 = OS default
                Integer.parseInt(env.getOrDefault("FANOUT_CONSUMERS", "1")));
    }
}
