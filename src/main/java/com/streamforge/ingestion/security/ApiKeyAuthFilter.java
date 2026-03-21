package com.streamforge.ingestion.security;

import com.streamforge.ingestion.repository.ApiKeyRepository;
import com.streamforge.ingestion.service.RateLimitService;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Reactive WebFilter that enforces dynamic API key authentication and rate limiting
 * on all /api/** endpoints (ingestion + dashboard).
 */
@Component
@Order(-1)
public class ApiKeyAuthFilter implements WebFilter {

    private static final List<String> EXCLUDED_PREFIXES = List.of(
        "/actuator",
        "/api/v1/projects",
        "/api/v1/users",
        "/api/v1/dashboard"
    );

    private final ApiKeyRepository apiKeyRepository;
    private final RateLimitService rateLimitService;

    public ApiKeyAuthFilter(ApiKeyRepository apiKeyRepository, RateLimitService rateLimitService) {
        this.apiKeyRepository = apiKeyRepository;
        this.rateLimitService = rateLimitService;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // Allow CORS preflight requests through without authentication
        if (exchange.getRequest().getMethod() == HttpMethod.OPTIONS) {
            return chain.filter(exchange);
        }

        String path = exchange.getRequest().getPath().value();

        // Allow actuator endpoints through without an API key
        if (EXCLUDED_PREFIXES.stream().anyMatch(path::startsWith)) {
            return chain.filter(exchange);
        }

        String apiKey = exchange.getRequest().getHeaders().getFirst("X-API-Key");

        if (apiKey == null || apiKey.isBlank()) {
            return reject(exchange, HttpStatus.UNAUTHORIZED);
        }

        // Validate Key via Database asynchronously (JPA is blocking)
        return Mono.fromCallable(() -> apiKeyRepository.findByKeyValueAndIsActiveTrue(apiKey))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(apiKeyOpt -> {
                    if (apiKeyOpt.isEmpty()) {
                        return reject(exchange, HttpStatus.UNAUTHORIZED);
                    }
                    
                    com.streamforge.ingestion.model.control.ApiKey keyObj = apiKeyOpt.get();
                    java.util.UUID projectId = keyObj.getProject().getId();
                    exchange.getAttributes().put("projectId", projectId);

                    String ip = getClientIp(exchange);

                    return rateLimitService.isAllowedByIp(ip)
                            .flatMap(ipAllowed -> {
                                if (!ipAllowed) {
                                    return reject(exchange, HttpStatus.TOO_MANY_REQUESTS);
                                }
                                return rateLimitService.isAllowedByApiKey(apiKey)
                                        .flatMap(keyAllowed -> {
                                            if (!keyAllowed) {
                                                return reject(exchange, HttpStatus.TOO_MANY_REQUESTS);
                                            }
                                            return chain.filter(exchange);
                                        });
                            });
                });
    }

    private Mono<Void> reject(ServerWebExchange exchange, HttpStatus status) {
        exchange.getResponse().setStatusCode(status);
        return exchange.getResponse().setComplete();
    }

    private String getClientIp(ServerWebExchange exchange) {
        String forwardedFor = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isEmpty()) {
            return forwardedFor.split(",")[0].trim();
        }
        InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
        return remoteAddress != null && remoteAddress.getAddress() != null
                ? remoteAddress.getAddress().getHostAddress()
                : "unknown";
    }
}
