package com.animeflix.userservice.service;

import com.animeflix.userservice.dto.request.AddFavoriteRequest;
import com.animeflix.userservice.dto.response.FavoriteResponse;
import com.animeflix.userservice.entity.Favorite;
import com.animeflix.userservice.exception.DuplicateResourceException;
import com.animeflix.userservice.exception.ResourceNotFoundException;
import com.animeflix.userservice.mapper.FavoriteMapper;
import com.animeflix.userservice.repository.FavoriteRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class FavoriteService {

    private final FavoriteRepository favoriteRepo;
    private final FavoriteMapper mapper;
    private final ExternalAnimeService animeService;

    // 1. Thêm vào yêu thích, chặn trùng nếu đã tồn tại
    public Mono<FavoriteResponse> addFavorite(String userId, AddFavoriteRequest request) {
        return favoriteRepo.existsByUserIdAndAnimeId(userId, request.getAnimeId())
                .flatMap(exists -> exists ? Mono.error(new DuplicateResourceException("Anime already in favorites")) : createFavorite(userId, request))
                .flatMap(favoriteRepo::save)
                .map(mapper::toResponse);
    }

    // 1a. Nếu request đã có sẵn info anime thì dùng luôn, không thì gọi catalog-service để lấy
    private Mono<Favorite> createFavorite(String userId, AddFavoriteRequest request) {
        if (request.getAnimeTitle() != null) {
            return Mono.just(buildFavorite(userId, request));
        }
        return animeService.getAnimeDetails(request.getAnimeId())
                .map(details -> enrichRequest(request, details))
                .map(enriched -> buildFavorite(userId, enriched))
                .onErrorResume(e -> fallbackWithoutDetails(userId, request, e));
    }

    // 1b. Gán thông tin denormalized (title, cover, banner...) từ catalog-service vào request
    private AddFavoriteRequest enrichRequest(AddFavoriteRequest request, ExternalAnimeService.AnimeDetails details) {
        request.setAnimeTitle(details.getTitle());
        request.setCoverImage(details.getCoverImage());
        request.setBannerImage(details.getBannerImage());
        request.setStatus(details.getStatus());
        request.setTotalEpisodes(details.getTotalEpisodes());
        return request;
    }

    // 1c. Nếu catalog-service lỗi/timeout, vẫn cho tạo favorite nhưng thiếu data denormalized
    private Mono<Favorite> fallbackWithoutDetails(String userId, AddFavoriteRequest request, Throwable e) {
        log.warn("Failed to fetch anime details, creating favorite without denormalized data: {}", e.getMessage());
        return Mono.just(buildFavorite(userId, request));
    }

    private Favorite buildFavorite(String userId, AddFavoriteRequest request) {
        return Favorite.builder()
                .userId(userId)
                .animeId(request.getAnimeId())
                .notifyNewEpisode(request.getNotifyNewEpisode())
                .animeTitle(request.getAnimeTitle())
                .coverImage(request.getCoverImage())
                .bannerImage(request.getBannerImage())
                .status(request.getStatus())
                .totalEpisodes(request.getTotalEpisodes())
                .addedAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    // 2. Lấy danh sách yêu thích (phân trang)
    public Flux<FavoriteResponse> getFavorites(String userId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        return favoriteRepo.findByUserIdOrderByAddedAtDesc(userId, pageable).map(mapper::toResponse);
    }

    // Lấy tất cả yêu thích, không phân trang
    public Flux<FavoriteResponse> getAllFavorites(String userId) {
        return favoriteRepo.findByUserIdOrderByAddedAtDesc(userId).map(mapper::toResponse);
    }

    // 3. Kiểm tra đã yêu thích chưa — dùng để hiện icon trái tim ở FE
    public Mono<Boolean> isFavorite(String userId, String animeId) {
        return favoriteRepo.existsByUserIdAndAnimeId(userId, animeId);
    }

    // 4. Xóa khỏi yêu thích
    public Mono<Void> removeFavorite(String userId, String animeId) {
        return favoriteRepo.existsByUserIdAndAnimeId(userId, animeId)
                .flatMap(exists -> exists ? favoriteRepo.deleteByUserIdAndAnimeId(userId, animeId) : Mono.error(new ResourceNotFoundException("Anime not found in favorites")));
    }

    // 5. Bật/tắt nhận thông báo tập mới cho 1 anime yêu thích
    public Mono<FavoriteResponse> toggleNotification(String userId, String animeId) {
        return favoriteRepo.findByUserIdAndAnimeId(userId, animeId)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Anime not found in favorites")))
                .flatMap(this::flipNotifyFlag)
                .map(mapper::toResponse);
    }

    private Mono<Favorite> flipNotifyFlag(Favorite favorite) {
        favorite.setNotifyNewEpisode(!favorite.getNotifyNewEpisode());
        favorite.setUpdatedAt(LocalDateTime.now());
        return favoriteRepo.save(favorite);
    }

    // Đếm số anime yêu thích ở UserStarController
    public Mono<Long> countFavorites(String userId) {
        return favoriteRepo.countByUserId(userId);
    }

    // Lấy user cần thông báo cho 1 anime cụ thể — dùng ở EpisodeEventConsumer khi có tập mới
    public Flux<Favorite> getFavoritesToNotify(String animeId) {
        return favoriteRepo.findByAnimeIdAndNotifyNewEpisodeTrue(animeId);
    }

    // Lấy favorites có bật thông báo của 1 user — dùng ở trang quản lý thông báo cá nhân
    public Flux<Favorite> getUserFavoritesWithNotification(String userId) {
        return favoriteRepo.findByUserIdAndNotifyNewEpisodeTrue(userId);
    }
}