package com.streamforge.ingestion.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.ingestion.model.StreamEvent;
import com.streamforge.ingestion.model.control.Project;
import com.streamforge.ingestion.repository.AnalyticsRepository;
import com.streamforge.ingestion.repository.ProjectRepository;
import com.streamforge.ingestion.service.RedisStatsUpdater;
import com.streamforge.ingestion.service.SlackAlertService;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Configuration
public class StreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(StreamProcessor.class);
    private final ObjectMapper objectMapper;
    private final JsonSerde<StreamEvent> streamEventSerde;
    private final Map<UUID, Project> projectCache = new ConcurrentHashMap<>();

    public StreamProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.streamEventSerde = new JsonSerde<>(StreamEvent.class, objectMapper);
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> processEvents(
            AnalyticsRepository analyticsRepository,
            RedisStatsUpdater redisStatsUpdater,
            ProjectRepository projectRepository,
            SlackAlertService slackAlertService) {
        
        return input -> {
            // 1. Safety Branching: Separate valid JSON from Poison Pills
            @SuppressWarnings("unchecked")
            KStream<String, String>[] branches = input.branch(
                (key, value) -> {
                    try {
                        objectMapper.readValue(value, StreamEvent.class);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                },
                (key, value) -> true
            );
            
            KStream<String, String> validJsonStream = branches[0];
            
            // Route Poison Pills to the DLQ topic for inspection and replay
            branches[1]
                .peek((key, value) -> log.error("POISON_PILL: routing to DLQ -> {}", value))
                .to("raw-events-dlq");

            // 2. Transformation
            KStream<String, StreamEvent> parsedStream = validJsonStream
                .mapValues(value -> {
                    try { return objectMapper.readValue(value, StreamEvent.class); }
                    catch (Exception e) { return null; }
                })
                .filter((key, value) -> value != null);

            // 3. Alerts Branch (5xx Errors)
            KStream<String, StreamEvent> alertsStream = parsedStream
                .filter((key, event) -> event.statusCode() != null && event.statusCode() >= 500)
                .peek((key, event) -> {
                    log.warn("ALERT: 5xx on project {} service {}", event.projectId(), event.service());
                    redisStatsUpdater.incrementErrorCount(event.projectId());
                });
 
            // 4. Real-time Redis updates
            parsedStream.peek((key, event) -> {
                redisStatsUpdater.incrementEventCount(event.projectId(), event.eventType() != null ? event.eventType() : "unknown");
            });
 
            // 5. Analytics Sink (TimescaleDB)
            parsedStream
                .foreach((key, event) -> {
                    String type = event.eventType() != null ? event.eventType() : "unknown";
                    log.info("STREAM_FLOW: Scoping event to project {} -> type: {}", event.projectId(), type);
                    
                    // Historical aggregation (Bucket to nearest minute)
                    Instant bucket = event.timestamp().truncatedTo(ChronoUnit.MINUTES);
                    analyticsRepository.upsertEventTypeCount(event.projectId(), type, 1L, bucket);
                    
                    // Raw Event Storage (for Inspector)
                    try {
                        String jsonPayload = objectMapper.writeValueAsString(event);
                        analyticsRepository.upsertRawEvent(event.projectId(), type, jsonPayload, Instant.now());
                    } catch (Exception e) {
                        log.error("Failed to serialize event for raw storage", e);
                    }

                    // 6. Slack Alerts Trigger
                    try {
                        Project project = projectCache.computeIfAbsent(event.projectId(), 
                            id -> projectRepository.findById(id).orElse(null));
                        
                        if (project != null && project.getSlackWebhookUrl() != null) {
                            Map<String, Object> config = objectMapper.readValue(project.getAlertConfig(), Map.class);
                            boolean alertOnErrors = (boolean) config.getOrDefault("alertOnErrors", true);
                            List<String> alertOnTypes = (List<String>) config.getOrDefault("alertOnTypes", List.of());

                            boolean shouldAlert = false;
                            String reason = "";

                            if (alertOnErrors && event.statusCode() != null && event.statusCode() >= 500) {
                                shouldAlert = true;
                                reason = "Critical Error (Status: " + event.statusCode() + ")";
                            } else if (alertOnTypes.contains(type)) {
                                shouldAlert = true;
                                reason = "Tracked Event: " + type;
                            }

                            if (shouldAlert) {
                                String msg = String.format("🔔 *ARGUS Alert* for *%s*\n*Reason:* %s\n*Service:* %s\n*User:* %s", 
                                    project.getName(), reason, event.service(), event.userId());
                                slackAlertService.sendFormattedAlert(project.getSlackWebhookUrl(), msg).subscribe();
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error evaluating alert rules for project {}", event.projectId(), e);
                    }
                });

            // Route alerts to the output topic
            return alertsStream.mapValues(event -> {
                try { return objectMapper.writeValueAsString(event); }
                catch (Exception e) { return "{}"; }
            });
        };
    }
}