package com.animeflix.userservice.repository;

import com.animeflix.userservice.entity.Notification;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.Date;

public interface NotificationRepository extends ReactiveMongoRepository<Notification, String> {

    /**  (danh sách có phân trang, mới nhất trước) */
    Flux<Notification> findByUserIdOrderByCreatedAtDesc(String userId, Pageable pageable);

    /**  (chỉ lấy cái chưa đọc, mới nhất trước) */
    Flux<Notification> findByUserIdAndIsReadFalseOrderByCreatedAtDesc(String userId);

    /** (đếm số chưa đọc cho badge chuông) */
    Mono<Long> countByUserIdAndIsReadFalse(String userId);

    Flux<Notification> findByUserIdAndIsReadFalse(String userId);

    /** job dọn dẹp định kỳ, xóa notification quá hạn TTL 30 ngày */
    Mono<Void> deleteByExpiresAtBefore(Date date);

    /** (xóa toàn bộ thông báo của 1 user, VD khi user bấm "Clear all") */
    Mono<Void> deleteByUserId(String userId);

    /** NotificationService.createNotification() — chặn tạo trùng notification */
    Mono<Boolean> existsByUserIdAndAnimeIdAndEpisodeNumberAndCreatedAtAfter(String userId, String animeId, Integer episodeNumber, LocalDateTime createdAt);
}