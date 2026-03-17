package com.streamforge.ingestion;

import com.streamforge.ingestion.model.control.Project;
import com.streamforge.ingestion.model.control.User;
import com.streamforge.ingestion.repository.UserRepository;
import com.streamforge.ingestion.service.ProjectManagementService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for the ARGUS Ingestion API.
 *
 * Spins up real Kafka, PostgreSQL (TimescaleDB-compatible), and Redis
 * containers via Testcontainers so the full Spring context loads correctly.
 *
 * Tests:
 *  1. Context loads successfully with all infrastructure available.
 *  2. Unauthenticated POST /api/events → 401 Unauthorized.
 *  3. Authenticated POST /api/events with valid payload → 202 Accepted.
 *  4. Dashboard counters endpoint requires auth → 401 Unauthorized.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class IngestionApiApplicationTests {

    // ── Infrastructure Containers ──────────────────────────────────────────

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
                    .withDatabaseName("postgres")
                    .withUsername("postgres")
                    .withPassword("holdontohope");

    @SuppressWarnings("resource")
    @Container
    static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    // ── Wire container addresses into Spring properties ────────────────────

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.cloud.stream.kafka.streams.binder.brokers", kafka::getBootstrapServers);

        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        // Use a non-existent mmdb path — GeoIpService gracefully falls back to "Unknown"
        registry.add("geoip.database.path", () -> "/tmp/nonexistent.mmdb");
    }

    // ── Test Client ────────────────────────────────────────────────────────

    @Autowired
    WebTestClient webTestClient;

    @Autowired
    ProjectManagementService projectManagementService;

    @Autowired
    UserRepository userRepository;

    private String validApiKey;

    @BeforeEach
    void setup() {
        if (validApiKey == null) {
            User user = userRepository.save(new User("test-" + java.util.UUID.randomUUID().toString() + "@example.com", "$2a$10$test.hash.for.integration.tests.only.placeholder"));
            Project project = projectManagementService.createProject(user.getId(), "Test Project");
            validApiKey = project.getApiKeys().get(0).getKeyValue();
        }
    }

    // ── Payload ────────────────────────────────────────────────────────────

    private static final String VALID_PAYLOAD = """
            {
              "eventId":   "550e8400-e29b-41d4-a716-446655440000",
              "eventType": "page_view",
              "timestamp": "2026-03-14T05:00:00Z",
              "sessionId": "sess-test-001",
              "userId":    "user-99",
              "url":       "/home",
              "service":   "frontend",
              "statusCode": 200,
              "latency":    42
            }
            """;

    // ── Tests ──────────────────────────────────────────────────────────────

    /**
     * Verifies that the Spring application context loads without errors
     * when all infrastructure (Kafka, Postgres, Redis) is available.
     * This is the most fundamental smoke test.
     */
    @Test
    void contextLoads() {
        // If Spring context fails to load, this test never even reaches this line.
    }

    /**
     * A request with no X-API-Key header must be rejected with 401.
     */
    @Test
    void postEvent_withoutApiKey_returns401() {
        webTestClient.post()
                .uri("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(VALID_PAYLOAD)
                .exchange()
                .expectStatus().isUnauthorized();
    }

    /**
     * A request with an invalid API key must also be rejected with 401.
     */
    @Test
    void postEvent_withInvalidApiKey_returns401() {
        webTestClient.post()
                .uri("/api/events")
                .header("X-API-Key", "totally-wrong-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(VALID_PAYLOAD)
                .exchange()
                .expectStatus().isUnauthorized();
    }

    /**
     * A well-formed request with a valid API key must be accepted with 202.
     * The event is published to Kafka asynchronously — the API returns
     * immediately without waiting for stream processing.
     */
    @Test
    void postEvent_withValidKeyAndPayload_returns202() {
        webTestClient.post()
                .uri("/api/events")
                .header("X-API-Key", validApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(VALID_PAYLOAD)
                .exchange()
                .expectStatus().isAccepted();
    }

    /**
     * The dashboard counters endpoint is also protected — no public access.
     */
    @Test
    void getDashboardCounters_withoutApiKey_returns401() {
        webTestClient.get()
                .uri("/api/v1/dashboard/counters")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    /**
     * With a valid key, the dashboard counters endpoint responds successfully.
     */
    @Test
    void getDashboardCounters_withValidKey_returns200() {
        webTestClient.get()
                .uri("/api/v1/dashboard/counters")
                .header("X-API-Key", validApiKey)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.totalEvents").exists()
                .jsonPath("$.errorEvents").exists()
                .jsonPath("$.eventsByType").exists();
    }
}
