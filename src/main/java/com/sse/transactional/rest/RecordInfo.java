package com.sse.transactional.rest;

public record RecordInfo(String topic, int partition, long offset) {
}
