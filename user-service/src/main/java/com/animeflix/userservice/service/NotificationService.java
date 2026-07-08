package com.animeflix.userservice.service;

import com.animeflix.userservice.dto.response.NotificationResponse;
import com.animeflix.userservice.entity.Notification;
import com.animeflix.userservice.exception.ResourceNotFoundException;
import com.animeflix.userservice.mapper.NotificationMapper;
import com.animeflix.userservice.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepo;
    private final NotificationMapper mapper;

    // Tạo notification mới, tự động chặn trùng lặp trong vòng 1 giờ
    public Mono<Notification> createNotification(String userId, Notification.NotificationType type, String title, String message, String animeId, Integer episodeNumber, String imageUrl) {
        LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);

        return notificationRepo.existsByUserIdAndAnimeIdAndEpisodeNumberAndCreatedAtAfter(userId, animeId, episodeNumber, oneHourAgo)
                .flatMap(exists -> exists ? skipDuplicate(userId, animeId, episodeNumber) : buildAndSave(userId, type, title, message, animeId, episodeNumber, imageUrl));
    }

    // Bỏ qua nếu đã có notification trùng trong 1 giờ gần đây
    private Mono<Notification> skipDuplicate(String userId, String animeId, Integer episodeNumber) {
        log.debug("Skipping duplicate notification for user: {}, anime: {}, ep: {}", userId, animeId, episodeNumber);
        return Mono.empty();
    }

    // Build entity và lưu vào MongoDB
    private Mono<Notification> buildAndSave(String userId, Notification.NotificationType type, String title, String message, String animeId, Integer episodeNumber, String imageUrl) {
        LocalDateTime now = LocalDateTime.now();
        Date expiresAt = Date.from(now.plusDays(30).atZone(ZoneId.systemDefault()).toInstant());

        Notification notification = Notification.builder()
                .userId(userId)
                .type(type)
                .title(title)
                .message(message)
                .imageUrl(imageUrl)
                .animeId(animeId)
                .episodeNumber(episodeNumber)
                .actionUrl(String.format("/anime/%s/episode/%d", animeId, episodeNumber))
                .isRead(false)
                .createdAt(now)
                .expiresAt(expiresAt)
                .build();

        return notificationRepo.save(notification);
    }

    // 1. Lấy danh sách thông báo (phân trang, mới nhất trước)
    public Flux<NotificationResponse> getNotifications(String userId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        return notificationRepo.findByUserIdOrderByCreatedAtDesc(userId, pageable).map(mapper::toResponse);
    }

    // 2. Lấy thông báo chưa đọc
    public Flux<NotificationResponse> getUnreadNotifications(String userId) {
        return notificationRepo.findByUserIdAndIsReadFalseOrderByCreatedAtDesc(userId).map(mapper::toResponse);
    }

    // 3. Đếm thông báo chưa đọc (cho badge chuông)
    public Mono<Long> countUnread(String userId) {
        return notificationRepo.countByUserIdAndIsReadFalse(userId);
    }

    // 4a. Đánh dấu 1 thông báo là đã đọc
    public Mono<NotificationResponse> markAsRead(String userId, String notificationId) {
        return notificationRepo.findById(notificationId)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Notification not found")))
                .flatMap(notification -> verifyOwnerAndMarkRead(notification, userId))
                .map(mapper::toResponse);
    }

    // 4b. Check quyền sở hữu trước khi update, tránh user A sửa notification của user B
    private Mono<Notification> verifyOwnerAndMarkRead(Notification notification, String userId) {
        if (!notification.getUserId().equals(userId)) {
            return Mono.error(new ResourceNotFoundException("Notification not found"));
        }
        notification.setIsRead(true);
        notification.setReadAt(LocalDateTime.now());
        return notificationRepo.save(notification);
    }

    // 5a. Đánh dấu tất cả thông báo của user là đã đọc
    public Mono<Long> markAllAsRead(String userId) {
        return notificationRepo.findByUserIdAndIsReadFalse(userId)
                .flatMap(this::markReadAndSave)
                .count();
    }

    // 5b. Gọi db cập nhập
    private Mono<Notification> markReadAndSave(Notification notification) {
        notification.setIsRead(true);
        notification.setReadAt(LocalDateTime.now());
        return notificationRepo.save(notification);
    }

    // 6a. Xóa 1 thông báo cụ thể (nút "x" trên từng item)
    public Mono<Void> deleteNotification(String userId, String notificationId) {
        return notificationRepo.findById(notificationId)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Notification not found")))
                .flatMap(notification -> verifyOwnerAndDelete(notification, userId));
    }

    // 6b. ....
    private Mono<Void> verifyOwnerAndDelete(Notification notification, String userId) {
        if (!notification.getUserId().equals(userId)) {
            return Mono.error(new ResourceNotFoundException("Notification not found"));
        }
        return notificationRepo.delete(notification);
    }

    // 7. Clear all — xóa toàn bộ thông báo của user (nút "Clear all")
    public Mono<Void> deleteAllNotifications(String userId) {
        return notificationRepo.deleteByUserId(userId);
    }

    // 8. Dọn dẹp thông báo hết hạn — gọi từ scheduler định kỳ
    public Mono<Void> cleanupExpiredNotifications() {
        Date now = new Date();
        return notificationRepo.deleteByExpiresAtBefore(now)
                .doOnSuccess(v -> log.info("Cleaned up expired notifications"));
    }
}