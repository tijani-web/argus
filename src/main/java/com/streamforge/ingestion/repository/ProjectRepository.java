package com.streamforge.ingestion.repository;

import com.streamforge.ingestion.model.control.Project;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.EntityGraph;

@Repository
public interface ProjectRepository extends JpaRepository<Project, UUID> {
    @EntityGraph(attributePaths = {"apiKeys"})
    List<Project> findByUserId(UUID userId);
}
