package dev.anurag.fanout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CoreTest {
    static byte[] event(String stream, long seq) {
        return ("{\"stream\":\"" + stream + "\",\"seq\":" + seq + ",\"sent_at\":1.5,\"payload\":\"x\"}")
                .getBytes(StandardCharsets.UTF_8);
    }

    static Event parsed(String stream, long seq) throws IOException {
        return EventParser.parse(event(stream, seq));
    }

    static String text(byte[] line) {
        return new String(line, StandardCharsets.UTF_8);
    }

    static final Config CONFIG = Config.fromMap(Map.of("FANOUT_QUEUE", "3"));

    @Nested
    class Parser {
        @Test
        void extractsStreamAndSeqAndAppendsNewline() throws IOException {
            Event e = parsed("s1", 42);
            assertEquals("s1", e.stream());
            assertEquals(42, e.seq());
            assertEquals('\n', e.line()[e.line().length - 1]);
        }

        @Test
        void fieldOrderAndNestedValuesDoNotMatter() throws IOException {
            byte[] raw = "{\"payload\":{\"a\":[1,2,{\"seq\":9}]},\"seq\":7,\"stream\":\"x\"}"
                    .getBytes(StandardCharsets.UTF_8);
            Event e = EventParser.parse(raw);
            assertEquals("x", e.stream());
            assertEquals(7, e.seq());
        }

        @Test
        void rejectsMalformedEvents() {
            for (String bad : List.of("[1]", "{\"stream\":\"s\"}", "{\"seq\":1}", "{\"stream\":\"s\",\"seq\":0}", "nope")) {
                assertThrows(IOException.class, () -> EventParser.parse(bad.getBytes(StandardCharsets.UTF_8)), bad);
            }
        }
    }

    @Nested
    class Ring {
        @Test
        void keepsTheLastCapacityEvents() {
            RingBuffer ring = new RingBuffer(3);
            assertTrue(ring.isEmpty());
            for (long seq = 1; seq <= 5; seq++) {
                ring.add(seq, new byte[] {(byte) seq});
            }
            assertEquals(3, ring.oldestSeq());
            assertEquals(5, ring.newestSeq());
            assertTrue(ring.covers(2));
            assertFalse(ring.covers(1));
            assertFalse(ring.covers(6));
            List<byte[]> since = ring.since(3);
            assertEquals(2, since.size());
            assertArrayEquals(new byte[] {4}, since.get(0));
            assertArrayEquals(new byte[] {5}, since.get(1));
            assertEquals(0, ring.since(5).size());
        }

        @Test
        void restartsOnAJump() {
            RingBuffer ring = new RingBuffer(5);
            ring.add(1, new byte[0]);
            ring.add(2, new byte[0]);
            ring.add(10, new byte[0]);
            assertEquals(10, ring.oldestSeq());
            assertEquals(10, ring.newestSeq());
        }

        @Test
        void rejectsZeroCapacity() {
            assertThrows(IllegalArgumentException.class, () -> new RingBuffer(0));
        }
    }

    @Nested
    class Stream {
        final StreamState state = new StreamState("s", 5);
        final ClientConnection client = new ClientConnection(new Socket(), new StreamRegistry(5), new Stats(), CONFIG);

        @Test
        void attachDecidesReplayOrReset() throws IOException {
            for (long seq = 1; seq <= 8; seq++) {
                state.publish(parsed("s", seq));
            }
            assertEquals(8, state.head());
            var live = state.attach(client, -1);
            assertEquals(8, live.head());
            assertTrue(live.replay().isEmpty());
            assertFalse(live.reset());

            var replay = state.attach(client, 5);
            assertEquals(3, replay.replay().size());
            assertTrue(text(replay.replay().get(0)).contains("\"seq\":6"));

            assertTrue(state.attach(client, 1).reset()); // older than the buffer (holds 4..8)
            assertTrue(state.attach(client, 99).reset()); // ahead of the stream
            assertTrue(state.attach(client, 8).replay().isEmpty());
            assertEquals(1, state.subscriberCount());
            state.detach(client);
            assertEquals(0, state.subscriberCount());
        }

        @Test
        void duplicatesAreIgnoredAndSlowSubscribersReported() throws IOException {
            state.attach(client, -1);
            assertTrue(state.publish(parsed("s", 1)).isEmpty());
            assertTrue(state.publish(parsed("s", 1)).isEmpty()); // redelivery: not offered again
            state.publish(parsed("s", 2));
            state.publish(parsed("s", 3)); // queue capacity 3 is now full
            assertEquals(List.of(client), state.publish(parsed("s", 4)));
            assertTrue(state.publish(parsed("s", 5)).isEmpty()); // reported once
        }
    }

    @Test
    void configDefaultsAndOverrides() {
        Config defaults = Config.fromMap(Map.of());
        assertEquals(7000, defaults.port());
        assertEquals("events", defaults.topic());
        assertEquals(3, CONFIG.queueCapacity());
        assertEquals(1, new StreamRegistry(3).get("a") == new StreamRegistry(3).get("a") ? 0 : 1);
        StreamRegistry registry = new StreamRegistry(3);
        assertTrue(registry.get("a") == registry.get("a"));
        assertEquals(1, registry.size());
    }
}
