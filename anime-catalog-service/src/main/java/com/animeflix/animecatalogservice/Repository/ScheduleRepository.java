package com.animeflix.animecatalogservice.Repository;

import com.animeflix.animecatalogservice.Entity.AnimeSchedule;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ScheduleRepository extends ReactiveMongoRepository<AnimeSchedule, String> {

    Flux<AnimeSchedule> findByAiringAtBetweenOrderByAiringAtAsc(Long start, Long end);

    Flux<AnimeSchedule> findByDayOrderByAiringAtAsc(String day);

    // Kiểm tra tồn tại trả về Mono<Boolean>
    Mono<Boolean> existsByAnimeIdAndEpisode(String animeId, Integer episode);

    Mono<Void> deleteByAiringAtLessThan(Long timestamp);

    @Aggregation(pipeline = {
            "{ $match: { airingAt: { $gte: ?0, $lte: ?1 } } }",
            "{ $group: { _id: '$day', count: { $sum: 1 } } }",
            "{ $project: { _id: 0, day: '$_id', count: 1 } }"
    })
    Flux<DayCountProjection> countByDay(Long start, Long end);

    interface DayCountProjection {
        String getDay();
        Long getCount();
    }
}