package com.animeflix.userservice.repository;

import com.animeflix.userservice.entity.ContinueWatching;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ContinueWatchingRepository extends ReactiveMongoRepository<ContinueWatching, String> {

    // Lấy danh sách "Xem tiếp" (max maxItems) — dùng ở getContinueWatching
    Flux<ContinueWatching> findByUserIdOrderByLastWatchedAtDesc(String userId, Pageable pageable);

    // Tìm 1 anime cụ thể trong continue-watching — dùng ở updateFromHistory
    Mono<ContinueWatching> findByUserIdAndAniId(String userId, String aniId);

    // Xóa 1 anime khỏi continue-watching — dùng khi xem xong hoặc user tự xóa
    Mono<Void> deleteByUserIdAndAniId(String userId, String aniId);

    // Đếm số item hiện có — dùng ở cleanupOldEntries để check có vượt maxItems không
    Mono<Long> countByUserId(String userId);

    // Lấy danh sách cũ nhất trước — dùng ở cleanupOldEntries để xóa bớt khi vượt limit
    Flux<ContinueWatching> findByUserIdOrderByLastWatchedAtAsc(String userId);
}