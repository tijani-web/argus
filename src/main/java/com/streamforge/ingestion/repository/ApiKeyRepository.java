package com.streamforge.ingestion.repository;

import com.streamforge.ingestion.model.control.ApiKey;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ApiKeyRepository extends JpaRepository<ApiKey, UUID> {
    Optional<ApiKey> findByKeyValueAndIsActiveTrue(String keyValue);
}
