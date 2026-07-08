package com.animeflix.userservice.service;

import com.animeflix.userservice.exception.ExternalServiceException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExternalAnimeService {

    @Qualifier("animeCatalogWebClient")
    private final WebClient animeCatalogClient;

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String CACHE_PREFIX = "anime:";
    private static final Duration CACHE_TTL = Duration.ofHours(1);

    // Lấy thông tin cơ bản anime (cho history, continue-watching) — cache-aside qua Redis
    public Mono<AnimeBasicInfo> getAnimeBasicInfo(String animeId) {
        String cacheKey = CACHE_PREFIX + animeId + ":basic";

        return redisTemplate.opsForValue().get(cacheKey)
                .flatMap(this::parseBasicInfo)
                .switchIfEmpty(fetchAndCacheAnimeBasicInfo(animeId, cacheKey))
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(e -> {
                    log.warn("Failed to fetch anime info for {}: {}", animeId, e.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<AnimeBasicInfo> fetchAndCacheAnimeBasicInfo(String animeId, String cacheKey) {
        return fetchAnimeBasicInfo(animeId)
                .doOnNext(info -> cacheAnimeInfo(cacheKey, info)
                        .subscribe(v -> {}, err -> log.warn("Failed to cache anime info {}: {}", animeId, err.getMessage())));
    }

    private Mono<AnimeBasicInfo> fetchAnimeBasicInfo(String animeId) {
        return animeCatalogClient.get()
                .uri("/{id}", animeId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(this::toBasicInfo);
    }

    private AnimeBasicInfo toBasicInfo(JsonNode response) {
        JsonNode data = response.path("data");
        return AnimeBasicInfo.builder()
                .id(data.path("id").asText())
                .title(data.path("title").path("userPreferred").asText())
                .coverImage(data.path("coverImage").path("large").asText())
                .bannerImage(data.path("bannerImage").asText())
                .totalEpisodes(data.path("episodes").asInt())
                .status(data.path("status").asText())
                .format(data.path("format").asText())
                .build();
    }

    // Lấy chi tiết đầy đủ anime (cho favorites) — KHÔNG cache vì dữ liệu ít khi lặp lại request liên tục
    public Mono<AnimeDetails> getAnimeDetails(String animeId) {
        return animeCatalogClient.get()
                .uri("/{id}", animeId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(this::toDetails)
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(e -> {
                    log.error("Failed to fetch anime details: {}", e.getMessage());
                    return Mono.error(new ExternalServiceException("Failed to fetch anime details", e));
                });
    }

    private AnimeDetails toDetails(JsonNode response) {
        JsonNode data = response.path("data");
        return AnimeDetails.builder()
                .title(data.path("title").path("userPreferred").asText())
                .coverImage(data.path("coverImage").path("large").asText())
                .bannerImage(data.path("bannerImage").asText())
                .status(data.path("status").asText())
                .totalEpisodes(data.path("episodes").asInt())
                .build();
    }

    // Parse JSON từ cache về object — nếu lỗi format thì coi như cache miss
    private Mono<AnimeBasicInfo> parseBasicInfo(String json) {
        try {
            return Mono.just(objectMapper.readValue(json, AnimeBasicInfo.class));
        } catch (Exception e) {
            log.warn("Failed to parse cached anime info, treating as cache miss: {}", e.getMessage());
            return Mono.empty();
        }
    }

    // Serialize object rồi ghi vào Redis với TTL 1 giờ
    private Mono<Boolean> cacheAnimeInfo(String key, AnimeBasicInfo info) {
        try {
            String json = objectMapper.writeValueAsString(info);
            return redisTemplate.opsForValue().set(key, json, CACHE_TTL);
        } catch (Exception e) {
            log.warn("Failed to serialize anime info for caching: {}", e.getMessage());
            return Mono.just(false);
        }
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class AnimeBasicInfo {
        private String id;
        private String title;
        private String coverImage;
        private String bannerImage;
        private Integer totalEpisodes;
        private String status;
        private String format;
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class AnimeDetails {
        private String title;
        private String coverImage;
        private String bannerImage;
        private String status;
        private Integer totalEpisodes;
    }
}