package com.animeflix.aisearchservice.service;

import com.animeflix.aisearchservice.dto.kafka.AnimeUpdatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmbeddingConsumerService {

    private final EmbeddingService embeddingService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${kafka.topics.anime-updated}",
            groupId = "ai-search-service",
            containerFactory = "animeUpdatedListenerFactory"
    )
    public void onAnimeUpdated(@Payload String message, Acknowledgment ack) {
        AnimeUpdatedEvent event;
        try {
            event = objectMapper.readValue(message, AnimeUpdatedEvent.class);
        } catch (Exception e) {
            log.error("❌ Deserialize failed, skipping poison pill: {}", e.getMessage());
            ack.acknowledge(); // poison pill → ack để skip
            return;
        }

        log.info("📨 Received anime.data.updated: animeId={}", event.getAnimeId());

        embeddingService.embedAndSave(event)
                .doOnSuccess(saved -> {
                    log.info("✅ Embedded: {}", event.getAnimeId());
                    ack.acknowledge();  // thành công → commit offset
                })
                .doOnError(e -> {
                    log.error("❌ Embed failed for {}: {}", event.getAnimeId(), e.getMessage());
                    // KHÔNG ack → Kafka sẽ retry message này sau
                    // Nhưng cần nack tường minh để không stall partition:
                    ack.nack(Duration.ofSeconds(5)); // retry sau 5 giây
                })
                .subscribe();
    }
}