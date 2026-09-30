package dev.anurag.fanout;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single Kafka poll loop (KafkaConsumer is not thread-safe), on a platform thread.
 * Assigns every partition and starts at the end: this server fans out live traffic; history
 * for reconnecting clients comes from the in-memory ring buffers.
 */
final class KafkaSource implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

    private final Config config;
    private final StreamRegistry registry;
    private final Stats stats;
    private final int shard;
    private final int shards;
    private volatile boolean running = true;

    /**
     * {@code shard} of {@code shards}: this consumer owns partitions p where p % shards == shard.
     * A stream lives on one partition (Kafka key = stream), so sharding by partition keeps every
     * stream's order while spreading the dispatch work over several threads.
     */
    KafkaSource(Config config, StreamRegistry registry, Stats stats, int shard, int shards) {
        this.config = config;
        this.registry = registry;
        this.stats = stats;
        this.shard = shard;
        this.shards = shards;
    }

    static Properties consumerProperties(String bootstrap) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "5"); // same as the Python server
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
        return props;
    }

    void stop() {
        running = false;
    }

    @Override
    public void run() {
        try (var consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties(config.kafkaBootstrap()))) {
            List<TopicPartition> partitions = waitForPartitions(consumer).stream()
                    .filter(tp -> tp.partition() % shards == shard)
                    .toList();
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            partitions.forEach(consumer::position); // resolve the end offsets now, not lazily
            stats.readyShards.incrementAndGet();
            log.info("consuming {} partitions of {}", partitions.size(), config.topic());
            while (running) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(100))) {
                    stats.consumed.increment();
                    Event event;
                    try {
                        event = EventParser.parse(record.value());
                    } catch (IOException e) {
                        log.warn("skipping malformed event at {}-{}", record.partition(), record.offset());
                        continue;
                    }
                    for (ClientConnection slow : registry.get(event.stream()).publish(event)) {
                        slow.kill();
                    }
                }
            }
        }
    }

    private List<TopicPartition> waitForPartitions(KafkaConsumer<byte[], byte[]> consumer) {
        while (true) {
            var infos = consumer.partitionsFor(config.topic(), Duration.ofSeconds(5));
            if (infos != null && !infos.isEmpty()) {
                return infos.stream().map(i -> new TopicPartition(i.topic(), i.partition())).toList();
            }
            log.info("waiting for topic {}", config.topic());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
