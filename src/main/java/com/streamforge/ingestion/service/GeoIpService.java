package com.streamforge.ingestion.service;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Resolves a client IP address to a country name using a local MaxMind-compatible
 * mmdb file (DB-IP City Lite, bundled in the Docker image at build time).
 *
 * Falls back to "Unknown" if the database is unavailable or the IP is unresolvable
 * (e.g. localhost / private ranges).
 */
@Service
public class GeoIpService {

    private static final Logger log = LoggerFactory.getLogger(GeoIpService.class);

    @Value("${geoip.database.path:/app/geoip/GeoLite2-City.mmdb}")
    private String dbPath;

    private DatabaseReader reader;

    @PostConstruct
    public void init() {
        File dbFile = new File(dbPath);
        if (!dbFile.exists()) {
            log.warn("GeoIP database not found at '{}'. Country enrichment will return 'Unknown'.", dbPath);
            return;
        }
        try {
            reader = new DatabaseReader.Builder(dbFile).build();
            log.info("GeoIP database loaded successfully from '{}'", dbPath);
        } catch (IOException e) {
            log.error("Failed to load GeoIP database from '{}': {}", dbPath, e.getMessage());
        }
    }

    /**
     * Returns a Mono that resolves to the country name for the given IP address.
     * Never throws — emits "Unknown" on any lookup failure.
     */
    public Mono<String> getCountryFromIp(String ip) {
        if (reader == null || ip == null || ip.isBlank()) {
            return Mono.just("Unknown");
        }
        return Mono.fromCallable(() -> {
            try {
                InetAddress address = InetAddress.getByName(ip);
                return reader.city(address).getCountry().getName();
            } catch (GeoIp2Exception | IOException e) {
                // Private/loopback IPs and addresses not in the DB return "Unknown"
                return "Unknown";
            }
        }).onErrorReturn("Unknown");
    }
}
