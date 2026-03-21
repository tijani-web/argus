package com.streamforge.ingestion.controller;

import com.streamforge.ingestion.model.control.Project;
import com.streamforge.ingestion.repository.ProjectRepository;
import com.streamforge.ingestion.service.ProjectManagementService;
import com.streamforge.ingestion.service.SlackAlertService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/projects")
public class ProjectController {

    private final ProjectManagementService projectManagementService;
    private final ProjectRepository projectRepository;
    private final SlackAlertService slackAlertService;

    public ProjectController(ProjectManagementService projectManagementService,
                             ProjectRepository projectRepository,
                             SlackAlertService slackAlertService) {
        this.projectManagementService = projectManagementService;
        this.projectRepository = projectRepository;
        this.slackAlertService = slackAlertService;
    }

    public record CreateProjectRequest(UUID userId, String name) {}

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Project> createProject(@RequestBody CreateProjectRequest request) {
        // JPA is blocking, so we wrap it in a Mono and subscribe on boundedElastic
        return Mono.fromCallable(() -> 
                projectManagementService.createProject(request.userId(), request.name())
        ).subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/user/{userId}")
    public Flux<Project> getUserProjects(@PathVariable UUID userId) {
        // Wrap blocking JPA calls
        return Mono.fromCallable(() -> projectRepository.findByUserId(userId))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(Flux::fromIterable);
    }

    public record UpdateSlackConfigRequest(String slackWebhookUrl, String alertConfig) {}

    @PatchMapping("/{projectId}/slack")
    public Mono<Project> updateSlackConfig(@PathVariable UUID projectId, @RequestBody UpdateSlackConfigRequest request) {
        return Mono.fromCallable(() -> {
            Project project = projectRepository.findById(projectId)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
            
            if (request.slackWebhookUrl() != null) {
                project.setSlackWebhookUrl(request.slackWebhookUrl());
            }
            if (request.alertConfig() != null) {
                project.setAlertConfig(request.alertConfig());
            }
            
            return projectRepository.save(project);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/{projectId}/slack/test")
    public Mono<Void> testSlackAlert(@PathVariable UUID projectId) {
        return Mono.fromCallable(() -> projectRepository.findById(projectId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND)))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(project -> slackAlertService.sendTestAlert(project.getSlackWebhookUrl()));
    }
}
