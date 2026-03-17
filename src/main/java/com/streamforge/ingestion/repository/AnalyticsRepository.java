package com.streamforge.ingestion.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

@Repository
public class AnalyticsRepository {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public AnalyticsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void upsertEventTypeCount(java.util.UUID projectId, String eventType, long count, Instant windowStart) {
        log.info("DB_WRITE: Executing upsert for {} on project {} at {}", eventType, projectId, windowStart);

        String sql = """
                    INSERT INTO events_aggregation (time, project_id, event_type, event_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (time, project_id, event_type)
                    DO UPDATE SET event_count = events_aggregation.event_count + EXCLUDED.event_count;
                """;

        jdbcTemplate.update(
            con -> {
                var ps = con.prepareStatement(sql);
                ps.setTimestamp(1, Timestamp.from(windowStart));
                ps.setObject(2, projectId, Types.OTHER);
                ps.setObject(3, eventType, Types.VARCHAR);
                ps.setObject(4, count, Types.BIGINT);
                return ps;
            }
        );
    }

    @Transactional
    public void upsertRawEvent(java.util.UUID projectId, String eventType, String payloadJson, Instant timestamp) {
        String sql = """
                INSERT INTO raw_events (time, project_id, event_type, payload)
                VALUES (?, ?, ?, ?::jsonb)
                """;
        
        jdbcTemplate.update(sql, 
            Timestamp.from(timestamp), 
            projectId, 
            eventType, 
            payloadJson
        );
    }
}
