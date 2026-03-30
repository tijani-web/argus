package com.streamforge.ingestion.controller;

import com.streamforge.ingestion.model.control.Project;
import com.streamforge.ingestion.repository.ProjectRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Dashboard query API.
 * Supports both global (user-scoped) and project-scoped analytics for the ARGUS platform.
 */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ProjectRepository projectRepository;

    public DashboardController(ReactiveStringRedisTemplate redisTemplate,
                               JdbcTemplate jdbcTemplate,
                               ProjectRepository projectRepository) {
        this.redisTemplate = redisTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.projectRepository = projectRepository;
    }

    @GetMapping("/counters")
    public Mono<DashboardStats> getCounters(@RequestParam UUID userId, @RequestParam(required = false) UUID projectId) {
        // 1. Determine which projects to aggregate
        List<UUID> projectIds = getAuthorizedProjectIds(userId, projectId);
        if (projectIds.isEmpty()) {
            return Mono.just(new DashboardStats(0, 0, Map.of()));
        }

        // 2. Sum up totals across all authorized projects from Redis
        return Flux.fromIterable(projectIds)
                .flatMap(pid -> {
                    String prefix = "stats:project:" + pid + ":";
                    return Mono.zip(
                            redisTemplate.opsForValue().get(prefix + "events:total").map(Long::parseLong).defaultIfEmpty(0L),
                            redisTemplate.opsForValue().get(prefix + "events:errors").map(Long::parseLong).defaultIfEmpty(0L),
                            redisTemplate.keys(prefix + "events:type:*")
                                    .flatMap(key -> redisTemplate.opsForValue().get(key)
                                            .map(count -> Map.entry(key.replace(prefix + "events:type:", ""), Long.parseLong(count))))
                                    .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                    );
                })
                .collectList()
                .map(results -> {
                    long total = 0;
                    long errors = 0;
                    java.util.HashMap<String, Long> types = new java.util.HashMap<>();

                    for (var res : results) {
                        total += res.getT1();
                        errors += res.getT2();
                        res.getT3().forEach((k, v) -> types.merge(k, v, Long::sum));
                    }
                    return new DashboardStats(total, errors, types);
                });
    }

    @GetMapping("/series")
    public Mono<List<Map<String, Object>>> getSeries(
            @RequestParam UUID userId,
            @RequestParam(required = false) UUID projectId,
            @RequestParam(required = false) String eventType) {
        List<UUID> projectIds = getAuthorizedProjectIds(userId, projectId);
        if (projectIds.isEmpty()) return Mono.just(List.of());

        return Mono.fromCallable(() -> {
            String inSql = projectIds.stream().map(id -> "?").collect(Collectors.joining(","));
            String sql = "SELECT time, SUM(event_count) AS \"eventCount\" FROM events_aggregation WHERE project_id IN (" + inSql + ")";
            
            Object[] params;
            if (eventType != null && !eventType.isBlank()) {
                sql += " AND event_type = ?";
                sql += " GROUP BY time ORDER BY time ASC";
                params = new Object[projectIds.size() + 1];
                for (int i = 0; i < projectIds.size(); i++) params[i] = projectIds.get(i);
                params[projectIds.size()] = eventType;
            } else {
                sql += " GROUP BY time ORDER BY time ASC";
                params = projectIds.toArray();
            }
            
            return jdbcTemplate.queryForList(sql, params);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/events")
    public Mono<List<Map<String, Object>>> getRawEvents(
            @RequestParam UUID userId,
            @RequestParam(required = false) UUID projectId,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) String country,
            @RequestParam(defaultValue = "50") int limit) {
        
        List<UUID> projectIds = getAuthorizedProjectIds(userId, projectId);
        if (projectIds.isEmpty()) return Mono.just(List.of());

        return Mono.fromCallable(() -> {
            StringBuilder sql = new StringBuilder("SELECT time, event_type, payload FROM raw_events WHERE project_id IN (");
            sql.append(projectIds.stream().map(id -> "?").collect(Collectors.joining(",")));
            sql.append(")");

            java.util.List<Object> params = new java.util.ArrayList<>(projectIds);

            if (eventType != null && !eventType.isBlank()) {
                sql.append(" AND event_type = ?");
                params.add(eventType);
            }
            if (country != null && !country.isBlank()) {
                sql.append(" AND payload->>'country' = ?");
                params.add(country);
            }

            sql.append(" ORDER BY time DESC LIMIT ?");
            params.add(limit);
            
            return jdbcTemplate.queryForList(sql.toString(), params.toArray());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/countries")
    public Mono<List<Map<String, Object>>> getCountries(@RequestParam UUID userId, @RequestParam(required = false) UUID projectId) {
        List<UUID> projectIds = getAuthorizedProjectIds(userId, projectId);
        if (projectIds.isEmpty()) return Mono.just(List.of());

        return Mono.fromCallable(() -> {
            String inSql = projectIds.stream().map(id -> "?").collect(Collectors.joining(","));
            String sql = "SELECT payload->>'country' AS country, COUNT(*) AS count FROM raw_events " +
                         "WHERE project_id IN (" + inSql + ") " +
                         "AND payload->>'country' IS NOT NULL " +
                         "GROUP BY payload->>'country' ORDER BY count DESC LIMIT 10";
            return jdbcTemplate.queryForList(sql, projectIds.toArray());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Helper to get a list of authorized project IDs.
     * If projectId is provided, verifies ownership.
     * If projectId is null, returns all projects owned by the user.
     */
    private List<UUID> getAuthorizedProjectIds(UUID userId, UUID projectId) {
        List<Project> userProjects = projectRepository.findByUserId(userId);
        if (projectId != null) {
            // Verify ownership
            if (userProjects.stream().noneMatch(p -> p.getId().equals(projectId))) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Access denied to project");
            }
            return List.of(projectId);
        }
        return userProjects.stream().map(Project::getId).toList();
    }

    public record DashboardStats(
            long totalEvents,
            long errorEvents,
            Map<String, Long> eventsByType
    ) {}
}
