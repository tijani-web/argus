package com.streamforge.ingestion.service;

import com.streamforge.ingestion.model.control.ApiKey;
import com.streamforge.ingestion.model.control.Project;
import com.streamforge.ingestion.model.control.User;
import com.streamforge.ingestion.repository.ApiKeyRepository;
import com.streamforge.ingestion.repository.ProjectRepository;
import com.streamforge.ingestion.repository.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;

@Service
public class ProjectManagementService {

    private final UserRepository userRepository;
    private final ProjectRepository projectRepository;
    private final ApiKeyRepository apiKeyRepository;
    private final SecureRandom secureRandom = new SecureRandom();

    public ProjectManagementService(UserRepository userRepository,
                                    ProjectRepository projectRepository,
                                    ApiKeyRepository apiKeyRepository) {
        this.userRepository = userRepository;
        this.projectRepository = projectRepository;
        this.apiKeyRepository = apiKeyRepository;
    }

    @Transactional
    public Project createProject(UUID userId, String projectName) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.NOT_FOUND, "User not found: " + userId));

        Project project = new Project(projectName, user);
        project = projectRepository.save(project);

        String rawKey = generateSecureApiKey();
        ApiKey apiKey = new ApiKey(rawKey, project);
        apiKeyRepository.save(apiKey);

        // Add the key to the project's collection so the returned object is fully populated
        project.getApiKeys().add(apiKey);

        return project;
    }

    private String generateSecureApiKey() {
        byte[] randomBytes = new byte[32];
        secureRandom.nextBytes(randomBytes);
        String base64Encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
        // Prefix with argus_live_ for easy identification (like Stripe does)
        return "argus_live_" + base64Encoded;
    }
}
