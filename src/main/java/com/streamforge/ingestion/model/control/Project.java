package com.streamforge.ingestion.model.control;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "projects")
public class Project {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Column(name = "created_at", insertable = false, updatable = false)
    private Instant createdAt;

    @JsonIgnore
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @OneToMany(mappedBy = "project", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<ApiKey> apiKeys = new ArrayList<>();

    @Column(name = "slack_webhook_url")
    private String slackWebhookUrl;

    @Column(name = "alert_config", columnDefinition = "jsonb")
    private String alertConfig = "{\"alertOnErrors\": true, \"alertOnTypes\": []}";

    // default constructor for JPA
    public Project() {}

    public Project(String name, User user) {
        this.name = name;
        this.user = user;
    }

    public UUID getId() { return id; }
    public String getName() { return name; }
    public Instant getCreatedAt() { return createdAt; }
    public User getUser() { return user; }
    public List<ApiKey> getApiKeys() { return apiKeys; }

    public String getSlackWebhookUrl() { return slackWebhookUrl; }
    public void setSlackWebhookUrl(String slackWebhookUrl) { this.slackWebhookUrl = slackWebhookUrl; }

    public String getAlertConfig() { return alertConfig; }
    public void setAlertConfig(String alertConfig) { this.alertConfig = alertConfig; }
}
