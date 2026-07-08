package com.animeflix.userservice.controller;

import com.animeflix.userservice.dto.response.UserStatsResponse;
import com.animeflix.userservice.exception.ApiResponse;
import com.animeflix.userservice.service.WatchHistoryService;
import com.animeflix.userservice.service.FavoriteService;
import com.animeflix.userservice.service.RecommendationService;
import com.animeflix.userservice.service.NotificationService;
import com.animeflix.userservice.util.SecurityContextUtil;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/user/stats")
@RequiredArgsConstructor
public class UserStatsController {

    private final WatchHistoryService historyService;
    private final FavoriteService favoriteService;
    private final NotificationService notificationService;
    private final RecommendationService recommendationService;

    @Qualifier("authServiceWebClient")
    private final WebClient authWebClient;

    @GetMapping
    public Mono<ResponseEntity<ApiResponse<UserStatsResponse>>> getUserStats(ServerWebExchange exchange) {
        return SecurityContextUtil.getCurrentUserId(exchange)
                .flatMap(userId -> this.buildStats(userId, exchange))
                .map(stats -> ResponseEntity.ok(ApiResponse.success(stats)));
    }

    private Mono<UserStatsResponse> buildStats(String userId, ServerWebExchange exchange) {
        Mono<Long> animeWatched = historyService.countAnimeWatched(userId);
        Mono<Long> totalSeconds = historyService.getTotalWatchedSeconds(userId);
        Mono<Long> favCount = favoriteService.countFavorites(userId);
        Mono<Long> unreadNoti = notificationService.countUnread(userId);
        Mono<List<String>> topGenresMono = recommendationService.getTop3Genres(userId).onErrorReturn(List.of());

        String accessToken = exchange.getRequest().getCookies().getFirst("access_token") != null
                ? exchange.getRequest().getCookies().getFirst("access_token").getValue()
                : "";

        // Gọi WebClient sang Auth Service kèm Cookie xác thực
        Mono<LocalDateTime> memberSinceMono = authWebClient.get()
                .uri("/api/auth/user/me")
                .cookie("access_token", accessToken)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(json -> LocalDateTime.parse(json.path("createdAt").asText()))
                .onErrorReturn(LocalDateTime.now());

        return Mono.zip(animeWatched, totalSeconds, favCount, unreadNoti, topGenresMono, memberSinceMono)
                .map(tuple -> UserStatsResponse.builder()
                        .totalAnimeWatched(tuple.getT1())
                        .totalWatchTimeSeconds(tuple.getT2())
                        .favoritesCount(tuple.getT3())
                        .unreadNotifications(tuple.getT4())
                        .topGenres(tuple.getT5())
                        .memberSince(tuple.getT6())
                        .build());
    }
}