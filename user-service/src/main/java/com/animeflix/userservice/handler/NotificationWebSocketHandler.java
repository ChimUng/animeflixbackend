package com.animeflix.userservice.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// Reactive WebSocket Handler — quản lý kết nối realtime cho notification
@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationWebSocketHandler implements WebSocketHandler {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    // Session vật lý đang mở, CHỈ tồn tại trong RAM của instance này — không share được giữa nhiều pod
    private final Map<String, WebSocketSession> activeSessions = new ConcurrentHashMap<>();

    private static final String ONLINE_USERS_KEY = "online:users";

    // Được Spring WebFlux tự động gọi mỗi khi có kết nối WebSocket mới tới /ws/notifications
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String userId = extractUserId(session);
        if (userId == null) {
            log.warn("❌ No userId found in WebSocket handshake");
            return session.close();
        }

        log.info("👤 User {} connected via WebSocket", userId);
        activeSessions.put(userId, session);

        return markUserOnline(userId)
                .then(session.receive().doOnNext(msg -> handleMessage(session, userId, msg)).then())
                .doFinally(signalType -> disconnectUser(userId));
    }

    private void disconnectUser(String userId) {
        log.info("👤 User {} disconnected", userId);
        activeSessions.remove(userId);
        markUserOffline(userId).subscribe();
    }

    // Lấy userId từ query param — ví dụ ws://host/ws/notifications?userId=123
    private String extractUserId(WebSocketSession session) {
        try {
            return UriComponentsBuilder.fromUri(session.getHandshakeInfo().getUri())
                    .build()
                    .getQueryParams()
                    .getFirst("userId");
        } catch (Exception e) {
            log.error("Failed to extract userId: {}", e.getMessage());
            return null;
        }
    }

    // Xử lý message client gửi lên — hiện chỉ hỗ trợ ping/pong giữ kết nối
    private void handleMessage(WebSocketSession session, String userId, WebSocketMessage message) {
        String payload = message.getPayloadAsText();
        log.debug("📨 Received message from {}: {}", userId, payload);
        if ("ping".equals(payload)) {
            sendToSession(session, "pong");
        }
    }

    // Gửi message tới 1 user cụ thể — CHỈ hoạt động nếu session vật lý nằm trên instance này
    public void sendToUser(String userId, Object message) {
        WebSocketSession session = activeSessions.get(userId);
        if (session == null || !session.isOpen()) {
            log.debug("⏭️ User {} offline, notification stored only", userId);
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(message);
            sendToSession(session, json);
            log.info("📲 Notification sent to user {} via WebSocket", userId);
        } catch (Exception e) {
            log.error("❌ Failed to serialize message for user {}: {}", userId, e.getMessage());
        }
    }

    // Broadcast tới tất cả session đang mở trên instance này
    public void broadcast(Object message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            activeSessions.values().stream().filter(WebSocketSession::isOpen).forEach(session -> sendToSession(session, json));
            log.info("📢 Notification broadcast to {} users", activeSessions.size());
        } catch (Exception e) {
            log.error("❌ Failed to serialize broadcast message: {}", e.getMessage());
        }
    }

    // Gửi raw text qua session, log lỗi thay vì để rơi vào dropped error
    private void sendToSession(WebSocketSession session, String message) {
        session.send(Mono.just(session.textMessage(message)))
                .subscribe(v -> {}, error -> log.error("❌ Failed to send WebSocket message: {}", error.getMessage()));
    }

    // Đánh dấu online trong Redis — trạng thái này share được giữa nhiều instance
    private Mono<Long> markUserOnline(String userId) {
        return redisTemplate.opsForSet().add(ONLINE_USERS_KEY, userId)
                .doOnSuccess(added -> log.info("✅ User {} marked as online", userId));
    }

    private Mono<Long> markUserOffline(String userId) {
        return redisTemplate.opsForSet().remove(ONLINE_USERS_KEY, userId)
                .doOnSuccess(removed -> log.info("✅ User {} marked as offline", userId));
    }

    // Đếm tổng user online toàn cụm — dùng cho admin dashboard/monitoring
    public Mono<Long> getOnlineUsersCount() {
        return redisTemplate.opsForSet().size(ONLINE_USERS_KEY);
    }

    // Check user có online không — đọc từ Redis (đúng toàn cụm), KHÔNG đảm bảo session vật lý nằm trên instance đang gọi hàm này
    public Mono<Boolean> isUserOnline(String userId) {
        return redisTemplate.opsForSet().isMember(ONLINE_USERS_KEY, userId);
    }
}