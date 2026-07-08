package com.animeflix.userservice.service;

import com.animeflix.userservice.dto.response.RecommendationResponse;
import com.animeflix.userservice.entity.WatchHistory;
import com.animeflix.userservice.repository.WatchHistoryRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class RecommendationService {

    private final WatchHistoryRepository historyRepo;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    @Qualifier("animeCatalogWebClient")
    private final WebClient animeCatalogClient;

    private static final String CACHE_KEY_PREFIX = "recommendations:";
    private static final Duration CACHE_TTL = Duration.ofHours(6);

    // Lấy gợi ý anime cho user, ưu tiên đọc cache trước
    public Mono<RecommendationResponse> getRecommendations(String userId) {
        String cacheKey = CACHE_KEY_PREFIX + userId;

        return redisTemplate.opsForValue().get(cacheKey)
                .flatMap(this::parseFromCache)
                .switchIfEmpty(Mono.defer(() -> generateAndCacheRecommendations(userId, cacheKey)));
    }

    private Mono<RecommendationResponse> generateAndCacheRecommendations(String userId, String cacheKey) {
        return generateRecommendations(userId)
                .doOnNext(response -> cacheRecommendations(cacheKey, response)
                        .subscribe(v -> {}, err -> log.warn("Failed to cache recommendations for {}: {}", userId, err.getMessage())));
    }

    // Nếu chưa có history -> trending; nếu có -> phân tích genre rồi rank lại trending theo độ khớp
    private Mono<RecommendationResponse> generateRecommendations(String userId) {
        return historyRepo.findTop20ByUserIdOrderByCreatedAtDesc(userId)
                .collectList()
                .flatMap(history -> history.isEmpty() ? getTrendingAnime() : buildFromHistory(history));
    }

    private Mono<RecommendationResponse> buildFromHistory(List<WatchHistory> history) {
        List<String> recentAnimeIds = history.stream()
                .limit(5)
                .map(WatchHistory::getAniId)
                .distinct()
                .collect(Collectors.toList());

        return analyzeWatchHistory(recentAnimeIds).flatMap(this::findSimilarAnime);
    }

    // Gọi catalog-service để lấy genres của các anime đã xem, đếm tần suất từng genre
    private Mono<Map<String, Integer>> analyzeWatchHistory(List<String> animeIds) {
        return Flux.fromIterable(animeIds)
                .flatMap(this::fetchGenresSafely, 3)
                .flatMap(Flux::fromIterable)
                .collectMultimap(genre -> genre, genre -> 1)
                .map(this::toGenreScores)
                .defaultIfEmpty(Collections.emptyMap());
    }

    private Mono<List<String>> fetchGenresSafely(String animeId) {
        return animeCatalogClient.get()
                .uri("/{id}", animeId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(this::extractGenres)
                .onErrorResume(e -> {
                    log.warn("Failed to fetch genres for anime {}: {}", animeId, e.getMessage());
                    return Mono.just(Collections.emptyList());
                });
    }

    private List<String> extractGenres(JsonNode response) {
        JsonNode genresNode = response.path("data").path("Media").path("genres");
        List<String> genres = new ArrayList<>();
        if (genresNode.isArray()) genresNode.forEach(g -> genres.add(g.asText()));
        return genres;
    }

    private Map<String, Integer> toGenreScores(Map<String, Collection<Integer>> multimap) {
        return multimap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
    }

    // Rank lại danh sách trending dựa trên độ khớp genre với lịch sử xem
    private Mono<RecommendationResponse> findSimilarAnime(Map<String, Integer> genreScores) {
        if (genreScores.isEmpty()) return getTrendingAnime();

        List<String> topGenres = genreScores.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(3)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        return getTrendingAnime().map(response -> scoreAndFilter(response, genreScores, topGenres));
    }

    private RecommendationResponse scoreAndFilter(RecommendationResponse response, Map<String, Integer> genreScores, List<String> topGenres) {
        List<RecommendationResponse.AnimeRecommendation> scored = response.getRecommendations().stream()
                .peek(anime -> anime.setScore(calculateScore(anime.getGenres(), genreScores)))
                .peek(anime -> anime.setMatchReason(getMatchReason(topGenres)))
                .filter(anime -> anime.getScore() > 0)
                .sorted(Comparator.comparingInt(RecommendationResponse.AnimeRecommendation::getScore).reversed())
                .limit(10)
                .collect(Collectors.toList());

        return RecommendationResponse.builder()
                .recommendations(scored)
                .reason("Based on your watch history: " + String.join(", ", topGenres))
                .build();
    }

    // Lấy trending từ catalog-service, dùng làm nguồn ứng viên gợi ý
    private Mono<RecommendationResponse> getTrendingAnime() {
        return animeCatalogClient.get()
                .uri("/trending?page=1&perPage=20")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(this::toTrendingResponse)
                .onErrorResume(e -> {
                    log.error("Failed to fetch trending anime: {}", e.getMessage());
                    return Mono.just(RecommendationResponse.builder()
                            .recommendations(Collections.emptyList())
                            .reason("Unable to fetch recommendations")
                            .build());
                });
    }

    private RecommendationResponse toTrendingResponse(JsonNode response) {
        List<RecommendationResponse.AnimeRecommendation> recommendations = new ArrayList<>();
        JsonNode mediaList = response.path("data").path("Page").path("media");

        if (mediaList.isArray()) {
            mediaList.forEach(node -> recommendations.add(toRecommendation(node)));
        }

        return RecommendationResponse.builder()
                .recommendations(recommendations)
                .reason("Trending anime - Start watching to get personalized recommendations")
                .build();
    }

    private RecommendationResponse.AnimeRecommendation toRecommendation(JsonNode node) {
        List<String> genres = new ArrayList<>();
        JsonNode genresNode = node.path("genres");
        if (genresNode.isArray()) genresNode.forEach(g -> genres.add(g.asText()));

        return RecommendationResponse.AnimeRecommendation.builder()
                .id(node.path("id").asText())
                .title(node.path("title").path("userPreferred").asText())
                .coverImage(node.path("coverImage").path("large").asText())
                .bannerImage(node.path("bannerImage").asText(null))
                .genres(genres)
                .averageScore(node.path("averageScore").asInt(0))
                .popularity(node.path("popularity").asInt(0))
                .status(node.path("status").asText())
                .format(node.path("format").asText())
                .score(80)
                .matchReason("Trending now")
                .build();
    }

    private Integer calculateScore(List<String> animeGenres, Map<String, Integer> genreScores) {
        if (animeGenres == null || animeGenres.isEmpty()) return 0;
        return animeGenres.stream().mapToInt(genre -> genreScores.getOrDefault(genre, 0) * 10).sum();
    }

    private String getMatchReason(List<String> topGenres) {
        return topGenres.isEmpty() ? "Popular choice" : "Matches your favorite genres: " + String.join(", ", topGenres);
    }

    private Mono<RecommendationResponse> parseFromCache(String cachedJson) {
        try {
            return Mono.just(objectMapper.readValue(cachedJson, RecommendationResponse.class));
        } catch (Exception e) {
            log.warn("Failed to parse recommendations cache: {}", e.getMessage());
            return Mono.empty();
        }
    }

    private Mono<Boolean> cacheRecommendations(String key, RecommendationResponse response) {
        try {
            String json = objectMapper.writeValueAsString(response);
            return redisTemplate.opsForValue().set(key, json, CACHE_TTL);
        } catch (Exception e) {
            log.error("Failed to serialize recommendations for caching: {}", e.getMessage());
            return Mono.just(false);
        }
    }

    // Clear cache cho user — dùng khi user muốn refresh gợi ý ngay lập tức
    public Mono<Void> clearCache(String userId) {
        String cacheKey = CACHE_KEY_PREFIX + userId;
        return redisTemplate.delete(cacheKey)
                .doOnSuccess(deleted -> log.info(deleted > 0 ? "Cache cleared for user: " + userId : "No cache found for user: " + userId))
                .then();
    }

    // hàm cho userstat thống kê top3 genre của từng user
    public Mono<List<String>> getTop3Genres(String userId) {
        return historyRepo.findTop20ByUserIdOrderByCreatedAtDesc(userId)
                .collectList()
                .flatMap(history -> {
                    if (history.isEmpty()) return Mono.just(List.of());

                    List<String> recentAnimeIds = history.stream()
                            .limit(5).map(WatchHistory::getAniId).distinct().collect(Collectors.toList());

                    return analyzeWatchHistory(recentAnimeIds)
                            .map(genreScores -> genreScores.entrySet().stream()
                                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                                    .limit(3)
                                    .map(Map.Entry::getKey)
                                    .collect(Collectors.toList()));
                });
    }
}