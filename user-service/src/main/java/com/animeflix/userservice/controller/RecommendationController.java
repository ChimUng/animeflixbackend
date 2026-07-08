package com.animeflix.userservice.controller;

import com.animeflix.userservice.dto.response.RecommendationResponse;
import com.animeflix.userservice.exception.ApiResponse;
import com.animeflix.userservice.service.RecommendationService;
import com.animeflix.userservice.util.SecurityContextUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/user/recommendations")
@RequiredArgsConstructor
@Slf4j
public class RecommendationController {

    private final RecommendationService recommendationService;

    @GetMapping
    public Mono<ResponseEntity<ApiResponse<RecommendationResponse>>> getRecommendations(ServerWebExchange exchange) {
        log.debug("📥 Received recommendation request");
        return SecurityContextUtil.getCurrentUserId(exchange)
                .doOnNext(userId -> log.info("🔍 Getting recommendations for user: {}", userId))
                .flatMap(recommendationService::getRecommendations)
                .map(response -> {
                    log.info("✅ Returning {} recommendations",
                            response.getRecommendations().size());
                    return ResponseEntity.ok(ApiResponse.success(response));
                })
                .doOnError(error -> log.error("❌ Error getting recommendations: {}",
                        error.getMessage()));
    }

    @DeleteMapping("/cache")
    public Mono<ResponseEntity<ApiResponse<Void>>> clearCache(ServerWebExchange exchange) {
        log.debug("📥 Received clear cache request");
        return SecurityContextUtil.getCurrentUserId(exchange)
                .doOnNext(userId -> log.info("🗑️ Clearing recommendations cache for user: {}", userId))
                .flatMap(recommendationService::clearCache)
                .then(Mono.fromCallable(() -> {
                    ApiResponse<Void> response = ApiResponse.<Void>builder()
                            .success(true)
                            .message("Recommendations cache cleared")
                            .data(null)
                            .timestamp(java.time.LocalDateTime.now())
                            .build();
                    return ResponseEntity.ok(response);
                }))
                .doOnError(error -> log.error("❌ Error clearing cache: {}",
                        error.getMessage()));
    }
}