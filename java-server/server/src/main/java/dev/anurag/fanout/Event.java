package dev.anurag.fanout;

/**
 * One event from Kafka. {@code line} is the record value plus a trailing newline: the exact bytes
 * every subscriber receives. It is built once and shared by all connections (never copied per client).
 */
public record Event(String stream, long seq, byte[] line) {}
