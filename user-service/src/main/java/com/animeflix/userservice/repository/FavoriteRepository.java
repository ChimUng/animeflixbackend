package com.animeflix.userservice.repository;

import com.animeflix.userservice.entity.Favorite;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface FavoriteRepository extends ReactiveMongoRepository<Favorite, String> {

    // Lấy danh sách yêu thích (phân trang) — dùng ở GET /favorites
    Flux<Favorite> findByUserIdOrderByAddedAtDesc(String userId, Pageable pageable);

    // Lấy tất cả yêu thích không phân trang — dùng ở GET /favorites/all
    Flux<Favorite> findByUserIdOrderByAddedAtDesc(String userId);

    // Kiểm tra đã yêu thích chưa — dùng trước khi addFavorite/removeFavorite
    Mono<Boolean> existsByUserIdAndAnimeId(String userId, String animeId);

    // Tìm favorite cụ thể — dùng ở toggleNotification
    Mono<Favorite> findByUserIdAndAnimeId(String userId, String animeId);

    // Xóa khỏi yêu thích
    Mono<Void> deleteByUserIdAndAnimeId(String userId, String animeId);

    // Đếm số anime yêu thích — dùng ở profile stats
    Mono<Long> countByUserId(String userId);

    // Lấy user cần thông báo cho 1 anime cụ thể — dùng ở EpisodeEventConsumer, lọc ngay tại DB thay vì lấy hết rồi filter
    Flux<Favorite> findByAnimeIdAndNotifyNewEpisodeTrue(String animeId);

    // Lấy favorites có bật thông báo của 1 user — dùng ở trang "Quản lý thông báo" cá nhân
    Flux<Favorite> findByUserIdAndNotifyNewEpisodeTrue(String userId);
}