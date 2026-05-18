package com.animeflix.aisearchservice.service;

import com.animeflix.aisearchservice.dto.kafka.AnimeUpdatedEvent;
import com.animeflix.aisearchservice.Entity.AnimeVector;
import com.animeflix.aisearchservice.Repository.AnimeVectorRepository;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmbeddingBatchService {

    private final AnimeVectorRepository animeVectorRepository;
    private final EmbeddingService embeddingService;
    private final WebClient catalogWebClient;

    @Value("${search.embedding.batch-size:50}")
    private int batchSize;

    @Value("${search.embedding.rate-limit-delay-ms:200}")
    private long rateLimitDelayMs;

    private volatile boolean isRunning = false;

    /**
     * Batch embed toàn bộ anime chưa embed (embedded=false hoặc null).
     */
    public Mono<String> runBatch() {
        if (isRunning) {
            return Mono.just("⚠️ Batch đang chạy");
        }
        isRunning = true;

        return animeVectorRepository.countByEmbeddedFalseOrEmbeddedIsNull()
                .flatMap(count -> {
                    if (count == 0) {
                        isRunning = false;
                        return Mono.just("✅ Không có anime nào cần embed");
                    }

                    log.info("🚀 Batch embed {} anime...", count);

                    // Không truyền count vào PageRequest nữa
                    // Dùng cursor-style: fetch từng page nhỏ liên tiếp
                    return Flux.range(0, (int) Math.ceil((double) count / batchSize))
                            .concatMap(pageIndex ->
                                    animeVectorRepository
                                            .findByEmbeddedFalseOrEmbeddedIsNull(
                                                    PageRequest.of(pageIndex, batchSize))
                                            .collectList()
                                            .flatMap(this::processBatch)
                                            .delayElement(Duration.ofMillis(rateLimitDelayMs))
                            )
                            .doOnComplete(() -> {
                                isRunning = false;
                                log.info("🎉 Batch embed hoàn thành!");
                            })
                            .doOnError(e -> {
                                isRunning = false;
                                log.error("❌ Batch lỗi: {}", e.getMessage());
                            })
                            .then(Mono.just("✅ Batch embed hoàn thành cho " + count + " anime"));
                });
    }

    /**
     * Seed anime từ catalog service vào anime_vectors.
     *
     * - Anime đã tồn tại (dù embedded=true hay false) → SKIP, không ghi đè
     * - Anime mới → tạo với embedded=false để batch job embed sau
     *
     * Gọi nhiều lần với page khác nhau để lấy toàn bộ catalog:
     *   POST /api/admin/embedding/seed?page=1&perPage=200
     *   POST /api/admin/embedding/seed?page=2&perPage=200
     */
    public Mono<String> seedFromCatalog(int page, int perPage) {
        log.info("📥 Seeding từ catalog service page={}, perPage={}", page, perPage);

        return catalogWebClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/internal/embed-data")   // endpoint trả AnimeEmbedDTO
                        .queryParam("page", page)
                        .queryParam("perPage", perPage)
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .flatMap(response -> {
                    // Catalog trả về ApiResponse<List<AnimeEmbedDTO>>
                    JsonNode dataList = response.path("data");

                    if (!dataList.isArray() || dataList.size() == 0) {
                        return Mono.just("⚠️ Không có data từ catalog (page=" + page + ")");
                    }

                    // Collect IDs từ response để check trùng 1 lần (batch)
                    List<String> ids = new ArrayList<>();
                    dataList.elements().forEachRemaining(node ->
                            ids.add(node.path("id").asText()));

                    log.info("📋 Catalog trả {} anime, checking trùng...", ids.size());

                    // Lấy danh sách ID đã tồn tại trong Atlas
                    return animeVectorRepository.findAllById(ids)
                            .map(AnimeVector::getId)
                            .collectList()
                            .flatMap(existingIds -> {
                                List<AnimeVector> newVectors = new ArrayList<>();

                                dataList.elements().forEachRemaining(node -> {
                                    String id = node.path("id").asText();

                                    // Chỉ thêm anime CHƯA tồn tại
                                    if (existingIds.contains(id)) {
                                        log.debug("⏭️ Skip (đã tồn tại): {}", id);
                                        return;
                                    }

                                    AnimeVector av = AnimeVector.builder()
                                            .id(id)
                                            .titleRomaji(node.path("titleRomaji").asText(null))
                                            .titleEnglish(node.path("titleEnglish").asText(null))
                                            .titleUserPreferred(node.path("titleUserPreferred").asText(null))
                                            .description(node.path("description").asText(null))
                                            .genres(parseStringList(node.path("genres")))
                                            .coverImageLarge(node.path("coverImageLarge").asText(null))
                                            .coverImageExtraLarge(node.path("coverImageExtraLarge").asText(null))
                                            .bannerImage(node.path("bannerImage").asText(null))
                                            .averageScore(node.path("averageScore").asInt(0))
                                            .popularity(node.path("popularity").asInt(0))
                                            .status(node.path("status").asText(null))
                                            .format(node.path("format").asText(null))
                                            .season(node.path("season").asText(null))
                                            .seasonYear(node.path("seasonYear").asInt(0))
                                            .embedded(false)  // chưa embed
                                            .build();
                                    newVectors.add(av);
                                });

                                if (newVectors.isEmpty()) {
                                    return Mono.just("✅ Tất cả " + ids.size()
                                            + " anime đã tồn tại, không cần seed thêm (page=" + page + ")");
                                }

                                return animeVectorRepository.saveAll(newVectors).collectList()
                                        .map(saved -> String.format(
                                                "✅ Seeded %d anime mới (bỏ qua %d đã có) từ catalog page=%d",
                                                saved.size(), ids.size() - saved.size(), page));
                            });
                })
                .onErrorResume(e -> {
                    log.error("❌ Seed error: {}", e.getMessage());
                    return Mono.just("❌ Lỗi seed: " + e.getMessage());
                });
    }

    public boolean isRunning() {
        return isRunning;
    }

    private Mono<Void> processBatch(List<AnimeVector> batch) {
        log.info("⚙️ Processing batch {} anime...", batch.size());

        return Flux.fromIterable(batch)
                .concatMap(av -> {
                    AnimeUpdatedEvent event = AnimeUpdatedEvent.builder()
                            .animeId(av.getId())
                            .titleRomaji(av.getTitleRomaji())
                            .titleEnglish(av.getTitleEnglish())
                            .description(av.getDescription())
                            .genres(av.getGenres())
                            .tags(av.getTags())
                            .coverImageLarge(av.getCoverImageLarge())
                            .coverImageExtraLarge(av.getCoverImageExtraLarge())
                            .bannerImage(av.getBannerImage())
                            .averageScore(av.getAverageScore())
                            .popularity(av.getPopularity())
                            .status(av.getStatus())
                            .format(av.getFormat())
                            .season(av.getSeason())
                            .seasonYear(av.getSeasonYear())
                            .build();

                    return embeddingService.embedAndSave(event)
                            .onErrorResume(e -> {
                                log.error("❌ Skip anime {} do lỗi: {}", av.getId(), e.getMessage());
                                return Mono.empty();
                            });
                })
                .then();
    }

    private List<String> parseStringList(JsonNode node) {
        List<String> list = new ArrayList<>();
        if (node != null && node.isArray()) {
            node.elements().forEachRemaining(n -> list.add(n.asText()));
        }
        return list.isEmpty() ? null : list;
    }
}