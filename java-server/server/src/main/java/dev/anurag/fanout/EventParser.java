package dev.anurag.fanout;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.Arrays;

/**
 * Pulls {@code stream} and {@code seq} out of an event with Jackson's streaming parser: no object
 * tree, nothing allocated for the fields we skip. The same job as {@code orjson.loads} in Python.
 */
public final class EventParser {
    private static final JsonFactory FACTORY = new JsonFactory();

    private EventParser() {}

    public static Event parse(byte[] value) throws IOException {
        String stream = null;
        long seq = -1;
        try (JsonParser p = FACTORY.createParser(value)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("event is not a JSON object");
            }
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String field = p.currentName();
                JsonToken token = p.nextToken();
                if ("stream".equals(field) && token == JsonToken.VALUE_STRING) {
                    stream = p.getText();
                } else if ("seq".equals(field) && token == JsonToken.VALUE_NUMBER_INT) {
                    seq = p.getLongValue();
                } else {
                    p.skipChildren();
                }
                if (stream != null && seq >= 0) {
                    break;
                }
            }
        }
        if (stream == null || seq < 1) {
            throw new IOException("event needs a string 'stream' and a positive 'seq'");
        }
        byte[] line = Arrays.copyOf(value, value.length + 1);
        line[value.length] = '\n';
        return new Event(stream, seq, line);
    }
}
