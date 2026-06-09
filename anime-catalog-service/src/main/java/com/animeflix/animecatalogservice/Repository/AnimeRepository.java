package com.animeflix.animecatalogservice.Repository;

import com.animeflix.animecatalogservice.Entity.Anime;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.util.List;

@Repository
public interface AnimeRepository extends ReactiveMongoRepository<Anime, String> {

    // Tìm theo Season (cho api /season)
    Flux<Anime> findBySeasonAndSeasonYear(String season, Integer seasonYear, org.springframework.data.domain.Sort sort);

    // Tìm theo Format (cho api /popularmovie)
    Flux<Anime> findByFormat(String format, org.springframework.data.domain.Sort sort);

    // Tìm lịch chiếu (Những phim có nextAiringEpisode.airingAt > thời điểm hiện tại)
    @Query("{ 'nextAiringEpisode.airingAt': { $gt: ?0, $lt: ?1 } }")
    Flux<Anime> findAnimeSchedule(long startTime, long endTime, org.springframework.data.domain.Sort sort);

    // Search cơ bản (nếu cần search phức tạp hơn sẽ dùng MongoTemplate)
    Flux<Anime> findByTitleUserPreferredContainingIgnoreCase(String title, org.springframework.data.domain.Sort sort);}