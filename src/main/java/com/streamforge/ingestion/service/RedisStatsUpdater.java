package com.streamforge.ingestion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisStatsUpdater {

    private static final Logger log = LoggerFactory.getLogger(RedisStatsUpdater.class);
    private final ReactiveStringRedisTemplate redisTemplate;

    public RedisStatsUpdater(ReactiveStringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void incrementEventCount(java.util.UUID projectId, String eventType) {
        String projectPrefix = "stats:project:" + projectId + ":";
        String globalPrefix = "stats:global:";

        // Project-scoped
        redisTemplate.opsForValue().increment(projectPrefix + "events:total")
                .doOnError(e -> log.error("Failed to increment project total events", e))
                .subscribe();
        redisTemplate.opsForValue().increment(projectPrefix + "events:type:" + eventType)
                .doOnError(e -> log.error("Failed to increment project type events", e))
                .subscribe();

        // Global aggregate
        redisTemplate.opsForValue().increment(globalPrefix + "events:total")
                .doOnError(e -> log.error("Failed to increment global total events", e))
                .subscribe();
        redisTemplate.opsForValue().increment(globalPrefix + "events:type:" + eventType)
                .doOnError(e -> log.error("Failed to increment global type events", e))
                .subscribe();
    }

    public void incrementErrorCount(java.util.UUID projectId) {
        String projectPrefix = "stats:project:" + projectId + ":";
        redisTemplate.opsForValue().increment(projectPrefix + "events:errors")
                .doOnError(e -> log.error("Failed to increment project error events in Redis", e))
                .subscribe();
    }
}
