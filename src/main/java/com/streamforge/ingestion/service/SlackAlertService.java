package com.streamforge.ingestion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SlackAlertService {
    private static final Logger log = LoggerFactory.getLogger(SlackAlertService.class);
    private final WebClient webClient;

    public SlackAlertService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    public Mono<Void> sendTestAlert(String webhookUrl) {
        return sendFormattedAlert(webhookUrl, "🚀 *ARGUS Connectivity Test*\nYour Slack integration is now active and ready to guard your projects.");
    }

    public Mono<Void> sendFormattedAlert(String webhookUrl, String markdownMessage) {
        if (webhookUrl == null || webhookUrl.isBlank()) {
            return Mono.empty();
        }

        Map<String, Object> payload = new HashMap<>();
        payload.put("text", markdownMessage);
        
        // Using Slack Block Kit for a more professional look
        Map<String, Object> block = new HashMap<>();
        block.put("type", "section");
        Map<String, String> textMap = new HashMap<>();
        textMap.put("type", "mrkdwn");
        textMap.put("text", markdownMessage);
        block.put("text", textMap);
        
        payload.put("blocks", List.of(block));

        return webClient.post()
                .uri(webhookUrl)
                .bodyValue(payload)
                .retrieve()
                .bodyToMono(String.class)
                .doOnSuccess(s -> log.info("Slack alert sent successfully"))
                .doOnError(e -> log.error("Failed to send Slack alert: {}", e.getMessage()))
                .then();
    }
}
