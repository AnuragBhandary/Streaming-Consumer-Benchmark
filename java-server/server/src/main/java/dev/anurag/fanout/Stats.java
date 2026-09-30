package dev.anurag.fanout;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/** Counters shared by every thread. LongAdder avoids contention on hot increments. */
final class Stats {
    final LongAdder consumed = new LongAdder();
    final LongAdder delivered = new LongAdder();
    final LongAdder flushes = new LongAdder();
    final LongAdder slowDisconnects = new LongAdder();
    final LongAdder accepted = new LongAdder();
    final AtomicInteger connections = new AtomicInteger();
    final AtomicInteger readyShards = new AtomicInteger();
    volatile int shards = 1;

    String toJson(StreamRegistry registry) {
        Runtime rt = Runtime.getRuntime();
        return "{\"impl\":\"java-virtual-threads\""
                + ",\"ready\":" + (readyShards.get() >= shards)
                + ",\"connections\":" + connections.get()
                + ",\"accepted\":" + accepted.sum()
                + ",\"streams\":" + registry.size()
                + ",\"consumed\":" + consumed.sum()
                + ",\"delivered\":" + delivered.sum()
                + ",\"flushes\":" + flushes.sum()
                + ",\"slow_disconnects\":" + slowDisconnects.sum()
                + ",\"heap_used_mb\":" + (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024)
                + "}\n";
    }
}
