package com.streamforge.ingestion.service;

import com.streamforge.ingestion.model.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);
    private static final String TOPIC_RAW_EVENTS = "raw-events";
    private static final String TOPIC_DLQ = "raw-events-dlq";

    private final ReactiveKafkaProducerTemplate<String, StreamEvent> kafkaTemplate;
    private final GeoIpService geoIpService;

    public KafkaProducerService(ReactiveKafkaProducerTemplate<String, StreamEvent> kafkaTemplate,
            GeoIpService geoIpService) {
        this.kafkaTemplate = kafkaTemplate;
        this.geoIpService = geoIpService;
    }

    public void publishEvent(StreamEvent event, String clientIp) {
        geoIpService.getCountryFromIp(clientIp)
                .map(country -> new StreamEvent(
                        event.eventId(),
                        event.projectId(),
                        event.eventType(),
                        event.timestamp(),
                        event.userId(),
                        event.sessionId(),
                        event.url(),
                        event.service(),
                        event.data(),
                        event.browser(),
                        event.device(),
                        event.os(),
                        country,
                        event.latency(),
                        event.statusCode(),
                        event.error()))
                .flatMap(enrichedEvent -> {
                    String key = (enrichedEvent.userId() != null && !enrichedEvent.userId().isBlank())
                            ? enrichedEvent.userId()
                            : enrichedEvent.sessionId();
                    return kafkaTemplate.send(TOPIC_RAW_EVENTS, key, enrichedEvent)
                            .doOnSuccess(result -> log.debug("Sent event {} to {}", enrichedEvent.eventId(),
                                    TOPIC_RAW_EVENTS))
                            .thenReturn(enrichedEvent);
                })
                .onErrorResume(e -> {
                    log.error("Failed to send event to raw-events, routing to DLQ. EventId: {}, Error: {}",
                            event.eventId(), e.getMessage());
                    String key = (event.userId() != null && !event.userId().isBlank()) ? event.userId()
                            : event.sessionId();
                    return kafkaTemplate.send(TOPIC_DLQ, key, event)
                            .doOnSuccess(result -> log.debug("Sent event {} to DLQ", event.eventId()))
                            .doOnError(dlqError -> log.error("Failed to send event to DLQ. EventId: {}, Error: {}",
                                    event.eventId(), dlqError.getMessage()))
                            .then(Mono.<StreamEvent>empty())
                            .onErrorResume(dlqError -> Mono.empty());
                })
                .subscribe(); // Fire and forget for async processing
    }
}
