package com.animeflix.userservice.service;

import com.animeflix.userservice.dto.websocket.WebSocketNotificationMessage;
import com.animeflix.userservice.entity.Notification;
import com.animeflix.userservice.handler.NotificationWebSocketHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.ZoneId;

@Service
@RequiredArgsConstructor
@Slf4j
public class WebSocketNotificationService {

    private final NotificationWebSocketHandler webSocketHandler;

    // Gửi realtime tới 1 user, chỉ gửi nếu user đang mở WebSocket (online); trả Mono để caller tự compose/subscribe
    public Mono<Void> sendToUser(String userId, Notification notification) {
        return webSocketHandler.isUserOnline(userId).flatMap(online -> online ? pushMessage(userId, notification) : skipOffline(userId));
    }

    private Mono<Void> pushMessage(String userId, Notification notification) {
        webSocketHandler.sendToUser(userId, buildMessage(notification));
        return Mono.empty();
    }

    private Mono<Void> skipOffline(String userId) {
        log.debug("User {} offline, notification stored only", userId);
        return Mono.empty();
    }

    // Broadcast tới toàn bộ user đang online — hiện chưa có endpoint nào gọi, để dành cho tính năng gửi SYSTEM notification toàn hệ thống
    public void broadcast(WebSocketNotificationMessage message) {
        webSocketHandler.broadcast(message);
    }

    // Đếm số user đang online — hiện chưa có endpoint nào gọi, để dành cho admin dashboard
    public Mono<Long> getOnlineUsersCount() {
        return webSocketHandler.getOnlineUsersCount();
    }

    private WebSocketNotificationMessage buildMessage(Notification notification) {
        return WebSocketNotificationMessage.builder()
                .type(notification.getType().name())
                .notificationId(notification.getId())
                .title(notification.getTitle())
                .message(notification.getMessage())
                .imageUrl(notification.getImageUrl())
                .animeId(notification.getAnimeId())
                .episodeNumber(notification.getEpisodeNumber())
                .actionUrl(notification.getActionUrl())
                .timestamp(notification.getCreatedAt().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .priority("NORMAL")
                .build();
    }
}