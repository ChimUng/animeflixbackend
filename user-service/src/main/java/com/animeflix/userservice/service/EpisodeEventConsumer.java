package com.animeflix.userservice.service;

import com.animeflix.userservice.dto.kafka.NewEpisodeEvent;
import com.animeflix.userservice.entity.Notification;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class EpisodeEventConsumer {

    private final FavoriteService favoriteService;
    private final NotificationService notificationService;
    private final UserPreferenceService preferenceService;
    private final WebSocketNotificationService webSocketService;

    /**
     * Kafka listener - nhận event tập mới từ catalog-service
     * Consumer group: user-service-notifications | Topic: anime.episode.new
     */
    @KafkaListener(
            topics = "${spring.kafka.topics.new-episode}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleNewEpisode(
            @Payload NewEpisodeEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("📨 Received event: anime={}, episode={}, partition={}, offset={}",
                event.getAnimeId(), event.getEpisodeNumber(), partition, offset);

        processEvent(event)
                .doOnSuccess(count -> log.info("✅ Processed: {} notifications created", count))
                .doOnError(error -> log.error("❌ Error processing event: anime={}, episode={}",
                        event.getAnimeId(), event.getEpisodeNumber(), error))
                // Luôn ack dù thành công hay lỗi -> tránh infinite retry loop
                // (nếu muốn retry thật sự, nên dùng Dead Letter Topic thay vì giữ message)
                .doFinally(signal -> acknowledgment.acknowledge())
                .subscribe();
    }

    /** Tìm user đang follow anime này, check preference, tạo notification, đẩy WebSocket */
    private Mono<Integer> processEvent(NewEpisodeEvent event) {
        return favoriteService.getFavoritesToNotify(event.getAnimeId())
                .flatMap(favorite -> notifyUserIfEnabled(event, favorite.getUserId()))
                .reduce(0, Integer::sum)
                .doOnNext(count -> log.info("👥 Notified {} users", count));
    }

    /** Chỉ tạo + gửi notification nếu user đã bật thông báo trong preference */
    private Mono<Integer> notifyUserIfEnabled(NewEpisodeEvent event, String userId) {
        return preferenceService.isNotificationEnabled(userId)
                .flatMap(enabled -> enabled
                        ? createAndPushNotification(event, userId)
                        : Mono.just(0));
    }

    /** Lưu notification vào MongoDB rồi đẩy real-time qua WebSocket nếu user online */
    private Mono<Integer> createAndPushNotification(NewEpisodeEvent event, String userId) {
        String title = "New Episode Available!";
        String message = String.format("%s - Episode %d is now available",
                event.getAnimeTitle(), event.getEpisodeNumber());

        return notificationService.createNotification(
                        userId,
                        Notification.NotificationType.NEW_EPISODE,
                        title,
                        message,
                        event.getAnimeId(),
                        event.getEpisodeNumber(),
                        event.getCoverImage())
                .doOnNext(notification -> webSocketService.sendToUser(userId, notification))
                .map(notification -> 1)
                .defaultIfEmpty(0);
    }
}