package com.streamforge.ingestion.controller;

import com.streamforge.ingestion.model.control.User;
import com.streamforge.ingestion.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * User authentication endpoints.
 *
 * POST /api/v1/users/signup  — creates a new account (fails if email already exists)
 * POST /api/v1/users/login   — validates credentials, returns user record
 *
 * Both return AuthResponse which contains the user's UUID needed for project lookups.
 * The password_hash is annotated @JsonIgnore on the User entity and is never returned.
 *
 * Both endpoints are excluded from ApiKeyAuthFilter so they are publicly accessible
 * (a user needs to be created before they can own a project and get an API key).
 */
@RestController
@RequestMapping("/api/v1/users")
public class UserController {

    private static final Logger log = LoggerFactory.getLogger(UserController.class);

    private final UserRepository userRepository;
    private final BCryptPasswordEncoder passwordEncoder;

    public UserController(UserRepository userRepository) {
        this.userRepository = userRepository;
        this.passwordEncoder = new BCryptPasswordEncoder();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Request bodies
    // ─────────────────────────────────────────────────────────────────────────

    public record SignupRequest(String email, String password) {}
    public record LoginRequest(String email, String password) {}

    // ─────────────────────────────────────────────────────────────────────────
    // POST /api/v1/users/signup
    // Creates a new user. Returns 409 Conflict if the email is already registered.
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/signup")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<AuthResponse> signup(@RequestBody SignupRequest request) {
        if (request.email() == null || request.email().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Email is required");
        }
        if (request.password() == null || request.password().length() < 8) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Password must be at least 8 characters");
        }

        return Mono.fromCallable(() -> {
            // Conflict if email already exists
            if (userRepository.findByEmail(request.email()).isPresent()) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "An account with this email already exists");
            }

            String hash = passwordEncoder.encode(request.password());
            User saved = userRepository.save(new User(request.email(), hash));
            log.info("AUTH: New user created — {}", saved.getEmail());

            return new AuthResponse(saved.getId(), saved.getEmail(), saved.getCreatedAt());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // POST /api/v1/users/login
    // Validates email + password. Returns 401 for any credential mismatch.
    // ─────────────────────────────────────────────────────────────────────────

    @PostMapping("/login")
    @ResponseStatus(HttpStatus.OK)
    public Mono<AuthResponse> login(@RequestBody LoginRequest request) {
        if (request.email() == null || request.email().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Email is required");
        }
        if (request.password() == null || request.password().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Password is required");
        }

        return Mono.fromCallable(() -> {
            User user = userRepository.findByEmail(request.email())
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid email or password"));

            // BCrypt constant-time compare — prevents timing attacks
            if (!passwordEncoder.matches(request.password(), user.getPasswordHash())) {
                log.warn("AUTH: Failed login attempt for email {}", request.email());
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid email or password");
            }

            log.info("AUTH: Successful login — {}", user.getEmail());
            return new AuthResponse(user.getId(), user.getEmail(), user.getCreatedAt());
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
