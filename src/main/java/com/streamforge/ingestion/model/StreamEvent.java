package com.streamforge.ingestion.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record StreamEvent(
        @NotNull(message = "eventId is required")
        @Pattern(regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", message = "eventId must be a valid UUID")
        String eventId,

        java.util.UUID projectId,

        @NotBlank(message = "eventType is required")
        @Pattern(regexp = "^(page_view|click|api_call|error|signup|purchase|service_health)$", message = "Invalid eventType")
        String eventType,

        @NotNull(message = "timestamp is required")
        Instant timestamp,

        String userId,

        @NotBlank(message = "sessionId is required")
        String sessionId,

        String url,

        String service,

        Map<String, Object> data,

        String browser,

        String device,

        String os,

        String country,

        @PositiveOrZero(message = "latency must be zero or positive")
        Integer latency,

        @Min(value = 100, message = "statusCode must be >= 100")
        @Max(value = 599, message = "statusCode must be <= 599")
        Integer statusCode,

        Map<String, Object> error
) {}
