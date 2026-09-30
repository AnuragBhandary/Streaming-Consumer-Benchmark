package dev.anurag.fanout;

import java.util.ArrayList;
import java.util.List;

/**
 * The last {@code capacity} events of one stream, for cursor resume (bounded replay).
 * Seqs are contiguous within the buffer, so lookup is index arithmetic, not a search.
 * Not thread-safe: {@link StreamState} guards it.
 */
final class RingBuffer {
    private final byte[][] lines;
    private long oldestSeq = 0; // seq stored at `start`; 0 while empty
    private int start = 0;
    private int size = 0;

    RingBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        lines = new byte[capacity][];
    }

    /** Appends the next seq. A jump (the server joined mid-stream) restarts the buffer. */
    void add(long seq, byte[] line) {
        if (size > 0 && seq != newestSeq() + 1) {
            size = 0;
        }
        if (size == 0) {
            start = 0;
            oldestSeq = seq;
        }
        if (size < lines.length) {
            lines[(start + size) % lines.length] = line;
            size++;
        } else {
            lines[start] = line;
            start = (start + 1) % lines.length;
            oldestSeq++;
        }
    }

    boolean isEmpty() {
        return size == 0;
    }

    long oldestSeq() {
        return oldestSeq;
    }

    long newestSeq() {
        return size == 0 ? 0 : oldestSeq + size - 1;
    }

    /** Can we replay everything after {@code cursor} from memory? */
    boolean covers(long cursor) {
        return size > 0 && cursor >= oldestSeq - 1 && cursor <= newestSeq();
    }

    /** Events with seq greater than {@code cursor}, oldest first. Requires {@link #covers}. */
    List<byte[]> since(long cursor) {
        int skip = (int) (cursor - oldestSeq + 1);
        List<byte[]> out = new ArrayList<>(size - skip);
        for (int i = skip; i < size; i++) {
            out.add(lines[(start + i) % lines.length]);
        }
        return out;
    }
}
