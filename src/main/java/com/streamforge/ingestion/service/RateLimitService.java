package com.streamforge.ingestion.service;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

@Service
public class RateLimitService {

    private final ReactiveStringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> script;

    public RateLimitService(ReactiveStringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;

        // Token Bucket Algorithm Lua Script
        String lua = """
                    local rate = tonumber(ARGV[1])
                    local capacity = tonumber(ARGV[2])
                    local now = tonumber(ARGV[3])

                    local tokens_key = KEYS[1]
                    local timestamp_key = KEYS[2]

                    local last_tokens = tonumber(redis.call('get', tokens_key) or capacity)
                    local last_refreshed = tonumber(redis.call('get', timestamp_key) or now)

                    local delta_ms = math.max(0, now - last_refreshed)
                    local tokens_to_add = (delta_ms / 1000.0) * rate

                    local filled_tokens = math.min(capacity, last_tokens + tokens_to_add)

                    if filled_tokens >= 1 then
                        filled_tokens = filled_tokens - 1
                        local ttl = math.floor((capacity / rate) + 2)
                        redis.call('setex', tokens_key, ttl, tostring(filled_tokens))
                        redis.call('setex', timestamp_key, ttl, tostring(now))
                        return 1
                    else
                        return 0
                    end
                """;
        this.script = new DefaultRedisScript<>(lua, Long.class);
    }

    public Mono<Boolean> isAllowedByApiKey(String apiKey) {
        long now = Instant.now().toEpochMilli();
        return redisTemplate.execute(script,
                List.of("rl:apikey:" + apiKey + ":tokens", "rl:apikey:" + apiKey + ":ts"),
                List.of("5000", "5000", String.valueOf(now))).next().defaultIfEmpty(0L).map(res -> res == 1L);
    }

    public Mono<Boolean> isAllowedByIp(String ip) {
        long now = Instant.now().toEpochMilli();
        return redisTemplate.execute(script,
                List.of("rl:ip:" + ip + ":tokens", "rl:ip:" + ip + ":ts"),
                List.of("50", "50", String.valueOf(now))).next().defaultIfEmpty(0L).map(res -> res == 1L);
    }
}
