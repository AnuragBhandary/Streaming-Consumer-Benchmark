package dev.anurag.fanout;

import java.util.concurrent.ConcurrentHashMap;

final class StreamRegistry {
    private final ConcurrentHashMap<String, StreamState> streams = new ConcurrentHashMap<>();
    private final int ringCapacity;

    StreamRegistry(int ringCapacity) {
        this.ringCapacity = ringCapacity;
    }

    StreamState get(String id) {
        return streams.computeIfAbsent(id, key -> new StreamState(key, ringCapacity));
    }

    int size() {
        return streams.size();
    }
}
