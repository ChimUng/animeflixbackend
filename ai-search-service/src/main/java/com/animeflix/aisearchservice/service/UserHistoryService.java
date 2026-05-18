package com.animeflix.aisearchservice.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class UserHistoryService {

    private final WebClient userServiceWebClient;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ObjectMapper objectMapper;

    public UserHistoryService(
            @Qualifier("userServiceWebClient") WebClient userServiceWebClient,
            ReactiveRedisTemplate<String, String> reactiveRedisTemplate,
            ObjectMapper objectMapper) {
        this.userServiceWebClient = userServiceWebClient;
        this.reactiveRedisTemplate = reactiveRedisTemplate;
        this.objectMapper = objectMapper;
    }

    private static final String CACHE_PREFIX = "ai:user:genre:";
    private static final Duration CACHE_TTL = Duration.ofHours(1);

    public Mono<Map<String, Integer>> getGenrePreference(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(new HashMap<>());
        }

        String cacheKey = CACHE_PREFIX + userId;

        return reactiveRedisTemplate.opsForValue().get(cacheKey)
                .map(this::parseGenreMap)
                .switchIfEmpty(fetchFromUserService(userId, cacheKey))
                .onErrorReturn(new HashMap<>());
    }

    private Mono<Map<String, Integer>> fetchFromUserService(String userId, String cacheKey) {
        log.debug("Fetching genre preference from user-service for user: {}", userId);

        return userServiceWebClient.get()
                .uri("/api/user/history")
                .header("X-User-Id", userId)
                .retrieve()                          // Giữ nguyên retrieve()
                .bodyToMono(JsonNode.class)
                .flatMap(response -> {
                    Map<String, Integer> genreCount = extractGenreCount(response);
                    return serializeAndCache(cacheKey, genreCount)
                            .thenReturn(genreCount);
                })
                .onErrorResume(e -> {
                    log.error("Failed to fetch history from user-service for user {}: {}", userId, e.getMessage(), e);
                    return Mono.just(new HashMap<>());
                });
    }

    // Tách riêng method này để rõ ràng
    private Map<String, Integer> extractGenreCount(JsonNode response) {
        Map<String, Integer> genreCount = new HashMap<>();
        JsonNode historyList = response.path("data");   // JsonNode.path() là hợp lệ

        if (historyList != null && historyList.isArray()) {
            historyList.elements().forEachRemaining(item -> {
                JsonNode genres = item.path("genres");
                if (genres != null && genres.isArray()) {
                    genres.elements().forEachRemaining(g ->
                            genreCount.merge(g.asText().trim(), 1, Integer::sum));
                }
            });
        }
        return genreCount;
    }

    private Mono<Boolean> serializeAndCache(String key, Map<String, Integer> genreCount) {
        try {
            String json = objectMapper.writeValueAsString(genreCount);
            return reactiveRedisTemplate.opsForValue()
                    .set(key, json, CACHE_TTL)
                    .doOnError(e -> log.warn("⚠️ Cannot cache genre preference for key {}: {}", key, e.getMessage()))
                    .onErrorReturn(false);
        } catch (Exception e) {
            log.warn("⚠️ Cannot serialize genre preference: {}", e.getMessage());
            return Mono.just(false);
        }
    }

    private Map<String, Integer> parseGenreMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, Integer>>() {});
        } catch (Exception e) {
            log.warn("⚠️ Failed to parse cached genre map: {}", e.getMessage());
            return new HashMap<>();
        }
    }
}