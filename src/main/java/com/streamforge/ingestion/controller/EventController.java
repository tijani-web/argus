package com.streamforge.ingestion.controller;

import com.streamforge.ingestion.model.StreamEvent;
import com.streamforge.ingestion.service.KafkaProducerService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final KafkaProducerService kafkaProducerService;

    public EventController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping
    public Mono<ResponseEntity<Void>> ingestEvent(@Valid @RequestBody StreamEvent event, ServerWebExchange exchange) {
        String clientIp = getClientIp(exchange);
        java.util.UUID projectId = exchange.getAttribute("projectId");

        StreamEvent scopedEvent = new StreamEvent(
                event.eventId(),
                projectId,
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
                event.country(),
                event.latency(),
                event.statusCode(),
                event.error()
        );

        kafkaProducerService.publishEvent(scopedEvent, clientIp);

        return Mono.just(ResponseEntity.accepted().build());
    }

    private String getClientIp(ServerWebExchange exchange) {
        String forwardedFor = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank()) {
            return forwardedFor.split(",")[0].trim();
        }
        InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
        return remoteAddress != null && remoteAddress.getAddress() != null
                ? remoteAddress.getAddress().getHostAddress()
                : "unknown";
    }
}
