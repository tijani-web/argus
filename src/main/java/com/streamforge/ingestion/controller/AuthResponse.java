package com.streamforge.ingestion.controller;

import java.time.Instant;
import java.util.UUID;

/**
 * Response body for POST /api/v1/users/signup and POST /api/v1/users/login.
 * Returns only the fields the frontend needs — never the password hash.
 */
public record AuthResponse(
        UUID id,
        String email,
        Instant createdAt
) {}
