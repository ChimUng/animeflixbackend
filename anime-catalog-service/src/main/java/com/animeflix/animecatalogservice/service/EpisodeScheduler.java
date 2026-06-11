package com.animeflix.animecatalogservice.service;

import com.animeflix.animecatalogservice.Entity.AnimeSchedule;
import com.animeflix.animecatalogservice.Repository.ScheduleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class EpisodeScheduler {

    private final ScheduleRepository scheduleRepository;
    private final EpisodeEventPublisher eventPublisher;
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    private static final String PUBLISHED_KEY_PREFIX = "published:";

    /**
     * Check for new episodes every 5 minutes
     * 1. Query schedules từ MongoDB
     * 2. Filter episodes chưa publish
     * 3. Publish Kafka events
     */
    @Scheduled(cron = "0 */15 * * * ?")
    public void checkNewEpisodes() {
        log.info("🔍 Checking for new episodes...");

        long now = Instant.now().getEpochSecond();
        long next3h = Instant.now().plus(Duration.ofHours(2)).getEpochSecond();

        // Query schedules trong 24 giờ tới
        Flux<AnimeSchedule> schedules = scheduleRepository.findByAiringAtBetweenOrderByAiringAtAsc(now - 3600, next3h);

        schedules
                .flatMap(this::processSchedule)
                .reduce(0, Integer::sum)
                .doOnError(error -> log.error("❌ Error checking episodes: {}", error.getMessage()))
                .subscribe();
    }

    /**
     * Process single schedule entry
     * - Check if already published (Redis cache)
     * - Publish Kafka event
     * - Mark as published
     */
    private reactor.core.publisher.Mono<Integer> processSchedule(AnimeSchedule schedule) {
        String cacheKey = PUBLISHED_KEY_PREFIX + schedule.getAnimeId() + ":" + schedule.getEpisode();

        // Check if already published
        return redisTemplate.hasKey(cacheKey)
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        log.debug("⏭️ Already published: anime={}, episode={}",
                                schedule.getAnimeId(), schedule.getEpisode());
                        return reactor.core.publisher.Mono.just(0);
                    }

                    //  Publish Kafka event
                    String animeTitle = extractTitle(schedule);
                    String coverImage = schedule.getCoverImage();

                    eventPublisher.publishNewEpisode(
                            schedule.getAnimeId(),
                            animeTitle,
                            schedule.getEpisode(),
                            schedule.getAiringAt(),
                            coverImage,
                            schedule.getBannerImage()
                    );

                    // Mark as published (TTL: 30 days)
                    return redisTemplate.opsForValue()
                            .set(cacheKey, "1", Duration.ofDays(30))
                            .thenReturn(1);
                });
    }

    /**
     *  Extract anime title safely
     */
    private String extractTitle(AnimeSchedule schedule) {
        if (schedule.getTitle() == null) {
            return "Unknown Anime";
        }

        String title = schedule.getTitle().getEnglish();
        if (title == null || title.isEmpty()) {
            title = schedule.getTitle().getRomaji();
        }
        if (title == null || title.isEmpty()) {
            title = schedule.getTitle().getUserPreferred();
        }

        return title != null ? title : "Unknown Anime";
    }
}