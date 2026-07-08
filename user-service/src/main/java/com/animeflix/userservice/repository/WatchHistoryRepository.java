package com.animeflix.userservice.repository;

import com.animeflix.userservice.entity.WatchHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface WatchHistoryRepository extends ReactiveMongoRepository<WatchHistory, String> {

    // Lấy lịch sử xem (phân trang) — dùng ở getHistory
    Flux<WatchHistory> findByUserIdOrderByCreatedAtDesc(String userId, Pageable pageable);

    // Lấy lịch sử xem của 1 anime — dùng ở getHistoryByAnime
    Flux<WatchHistory> findByUserIdAndAniIdOrderByCreatedAtDesc(String userId, String aniId);

    // Tìm history của đúng 1 tập — dùng ở addOrUpdateHistory để check đã xem tập này chưa
    Mono<WatchHistory> findByUserIdAndAniIdAndEpId(String userId, String aniId, String epId);

    // Đếm số anime KHÁC NHAU đã xem — dùng ở countAnimeWatched (UserStats)
    @Aggregation(pipeline = {
            "{ $match: { userId: ?0 } }",
            "{ $group: { _id: '$aniId' } }",
            "{ $count: 'total' }"
    })
    Mono<Long> countDistinctAnimeByUserId(String userId);

    // Lấy 20 record gần nhất — dùng ở RecommendationService để phân tích genre
    Flux<WatchHistory> findTop20ByUserIdOrderByCreatedAtDesc(String userId);

    // Xóa lịch sử của 1 anime
    Mono<Void> deleteByUserIdAndAniId(String userId, String aniId);

    // Xóa toàn bộ lịch sử
    Mono<Void> deleteByUserId(String userId);

    // Tổng thời gian xem (giây) — dùng ở getTotalWatchedSeconds (UserStats)
    @Aggregation(pipeline = {
            "{ $match: { userId: ?0 } }",
            "{ $group: { _id: null, total: { $sum: '$timeWatched' } } }"
    })
    Mono<Long> getTotalWatchedSeconds(String userId);
}