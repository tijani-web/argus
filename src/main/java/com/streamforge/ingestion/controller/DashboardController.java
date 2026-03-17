package com.streamforge.ingestion.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Dashboard query API.
 * Supports both global and project-scoped analytics for the ARGUS platform.
 */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final JdbcTemplate jdbcTemplate;

    public DashboardController(ReactiveStringRedisTemplate redisTemplate,
                               JdbcTemplate jdbcTemplate) {
        this.redisTemplate = redisTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/counters")
    public Mono<DashboardStats> getCounters(@RequestParam(required = false) UUID projectId) {
        String prefix = (projectId != null) ? "stats:project:" + projectId + ":" : "stats:global:";

        return Mono.zip(
                redisTemplate.opsForValue().get(prefix + "events:total")
                        .map(Long::parseLong).defaultIfEmpty(0L),
                redisTemplate.opsForValue().get(prefix + "events:errors")
                        .map(Long::parseLong).defaultIfEmpty(0L)
        ).flatMap(tuple -> {
            long total = tuple.getT1();
            long errors = tuple.getT2();

            return redisTemplate.keys(prefix + "events:type:*")
                    .flatMap(key -> redisTemplate.opsForValue().get(key)
                            .map(count -> Map.entry(key.replace(prefix + "events:type:", ""), Long.parseLong(count))))
                    .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                    .map(typeCounts -> new DashboardStats(total, errors, typeCounts));
        });
    }

    @GetMapping("/series")
    public Mono<List<Map<String, Object>>> getSeries(@RequestParam(required = false) UUID projectId) {
        return Mono.fromCallable(() -> {
            if (projectId != null) {
                String sql = "SELECT time, SUM(event_count) AS \"eventCount\" FROM events_aggregation WHERE project_id = ? GROUP BY time ORDER BY time ASC";
                return jdbcTemplate.queryForList(sql, projectId);
            } else {
                // Aggregate across all projects for global view
                String sql = "SELECT time, SUM(event_count) as \"eventCount\" FROM events_aggregation GROUP BY time ORDER BY time ASC";
                return jdbcTemplate.queryForList(sql);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/events")
    public Mono<List<Map<String, Object>>> getRawEvents(@RequestParam(required = false) UUID projectId, @RequestParam(defaultValue = "50") int limit) {
        return Mono.fromCallable(() -> {
            if (projectId != null) {
                String sql = "SELECT time, event_type, payload FROM raw_events WHERE project_id = ? ORDER BY time DESC LIMIT ?";
                return jdbcTemplate.queryForList(sql, projectId, limit);
            } else {
                String sql = "SELECT time, event_type, payload FROM raw_events ORDER BY time DESC LIMIT ?";
                return jdbcTemplate.queryForList(sql, limit);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public record DashboardStats(
            long totalEvents,
            long errorEvents,
            Map<String, Long> eventsByType
    ) {}
}
