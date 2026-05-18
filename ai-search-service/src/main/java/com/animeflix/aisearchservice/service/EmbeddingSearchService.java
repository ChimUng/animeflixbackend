package com.animeflix.aisearchservice.service;

import com.animeflix.aisearchservice.Entity.AnimeVector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmbeddingSearchService {

    private final MongoTemplate mongoTemplate;

    @Value("${search.embedding.vector-index-name}")
    private String vectorIndexName;

    @Value("${search.embedding.num-candidates}")
    private int numCandidates;

    @Value("${search.embedding.limit}")
    private int limit;

    /** Ngưỡng tuyệt đối tối thiểu — loại noise rõ ràng */
    @Value("${search.embedding.similarity-threshold:0.75}")
    private double similarityThreshold;

    /**
     * Gap tối thiểu giữa 2 score liền kề để coi là "điểm gãy".
     * Ví dụ gap=0.035: [0.871, 0.829, 0.789]
     *   gap(0→1) = 0.042 >= 0.035 → cắt sau index 0? Không — tìm gap SỚM NHẤT đủ lớn.
     *   Thực tế: cắt sau phần "cụm đầu" tách biệt hẳn với phần còn lại.
     */
    @Value("${search.embedding.score-gap-threshold:0.035}")
    private double scoreGapThreshold;

    /** Số kết quả tối đa trả về */
    @Value("${search.embedding.max-results:10}")
    private int maxResults;

    @Value("${search.embedding.min-results:1}")
    private int minResults;

    public record ScoredAnimeVector(AnimeVector animeVector, Double similarityScore) {}

    public record SearchPageResult(List<ScoredAnimeVector> items, int totalCount) {}

    public Mono<SearchPageResult> searchPaged(List<Double> queryVector, int page, int perPage) {
        int fetchLimit = Math.min(perPage * 5, limit);

        return Mono.fromCallable(() -> {
            Document vectorSearchStage = new Document("$vectorSearch", new Document()
                    .append("index", vectorIndexName)
                    .append("path", "descriptionVector")
                    .append("queryVector", queryVector)
                    .append("numCandidates", numCandidates)
                    .append("limit", fetchLimit)
            );

            Document addScoreStage = new Document("$addFields", new Document()
                    .append("similarityScore", new Document("$meta", "vectorSearchScore"))
            );

            Document projectStage = new Document("$project", new Document()
                    .append("descriptionVector", 0)
            );

            List<Document> pipeline = List.of(vectorSearchStage, addScoreStage, projectStage);

            List<Document> rawDocs = mongoTemplate
                    .getCollection("anime_vectors")
                    .aggregate(pipeline)
                    .into(new ArrayList<>());

            List<ScoredAnimeVector> allResults = rawDocs.stream()
                    .map(doc -> {
                        Double score = doc.getDouble("similarityScore");
                        doc.remove("similarityScore");
                        AnimeVector entity = mongoTemplate.getConverter()
                                .read(AnimeVector.class, doc);
                        return new ScoredAnimeVector(entity, score != null ? score : 0.0);
                    })
                    .collect(Collectors.toList());

            List<ScoredAnimeVector> filtered = smartFilter(allResults);

            log.info("✅ Vector search: {} raw → {} filtered (gap={}, threshold={}, max={})",
                    allResults.size(), filtered.size(),
                    scoreGapThreshold, similarityThreshold, maxResults);

            return new SearchPageResult(filtered, filtered.size());

        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Smart filter — 4 bước:
     *
     * 1. Loại score < similarityThreshold (noise rõ ràng)
     * 2. Gap detection: tìm điểm gãy đầu tiên trong score list
     *    → Cắt tại đó nếu gap >= scoreGapThreshold
     * 3. Cap maxResults
     * 4. Đảm bảo tối thiểu 3 kết quả
     *
     * Ví dụ với "nezuko demon slayer":
     *   raw scores: [0.871, 0.829, 0.789, 0.788, 0.781, ...]
     *   → Bước 1: tất cả > 0.75 → giữ nguyên
     *   → Bước 2: gap(0.871→0.829) = 0.042 >= 0.035 → cắt sau index 1?
     *             Không — gap đầu tiên ĐỦ LỚN giữa 0.829 và 0.789 = 0.040 → cắt sau index 1
     *   → Kết quả: [Kimetsu (0.871), Mugen Train (0.829)]  ← 2 bộ, đúng nhất
     *   → Bước 4: < 3 → expand lên 3: thêm Attack on Titan
     */
    private List<ScoredAnimeVector> smartFilter(List<ScoredAnimeVector> results) {

        if (results.isEmpty()) return results;

        // Bước 1: Loại noise
        List<ScoredAnimeVector> aboveMin = results.stream()
                .filter(r -> r.similarityScore() >= similarityThreshold)
                .collect(Collectors.toList());

        if (aboveMin.isEmpty()) {
            return results.subList(0, Math.min(3, results.size()));
        }

        // Bước 2: Gap detection — tìm gap đầu tiên đủ lớn
        int cutIndex = findFirstSignificantGap(aboveMin);

        List<ScoredAnimeVector> gapFiltered = (cutIndex > 0)
                ? aboveMin.subList(0, cutIndex)
                : aboveMin;

        // Bước 3: Cap maxResults
        List<ScoredAnimeVector> capped = gapFiltered.size() > maxResults
                ? gapFiltered.subList(0, maxResults)
                : gapFiltered;

        // Bước 4: Đảm bảo tối thiểu 3 kết quả
        if (capped.size() < minResults) {
            int expandTo = Math.min(minResults, aboveMin.size());
            capped = aboveMin.subList(0, expandTo);
        }

        log.debug("smartFilter: raw={} → aboveMin={} → cutAt={} → final={}",
                results.size(), aboveMin.size(), cutIndex, capped.size());

        return capped;
    }

    /**
     * Tìm gap đầu tiên đủ lớn trong score list đã sort DESC.
     * Trả về index cắt (exclusive) — giữ phần [0, cutIndex).
     * Trả về 0 nếu không tìm thấy gap nào đủ lớn (→ không cắt).
     *
     * Ưu tiên gap SỚM: nếu top-2 đã tách biệt hẳn nhóm còn lại
     * thì cắt sớm, không chờ gap sau.
     */
    private int findFirstSignificantGap(List<ScoredAnimeVector> sorted) {
        if (sorted.size() <= 1) return 0;

        for (int i = 1; i < sorted.size(); i++) {
            double prev = sorted.get(i - 1).similarityScore();
            double curr = sorted.get(i).similarityScore();
            double gap = prev - curr;

            if (gap >= scoreGapThreshold) {
                log.debug("Gap found: [{:.4f}] → [{:.4f}] = {:.4f} → cut at {}",
                        prev, curr, gap, i);
                return i;
            }
        }
        return 0; // Không có gap → không cắt
    }
}