package com.animeflix.aisearchservice.service;

import com.animeflix.aisearchservice.dto.request.SearchRequestDTO;
import com.animeflix.aisearchservice.dto.response.AnimeSearchResultDTO;
import com.animeflix.aisearchservice.dto.response.ParsedQueryDTO;
import com.animeflix.aisearchservice.dto.response.SearchResponseDTO;
import com.animeflix.aisearchservice.mapper.AnimeVectorMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Main orchestrator - điều phối toàn bộ flow:
 *
 * Query → [QueryParser] → confidence >= threshold?
 *   YES → [AniListAPI] → [UserHistory] → [ReRank Structured] → Response
 *   NO  → [GeminiEmbed] → [VectorSearch] → [UserHistory] → [ReRank Semantic] → Response
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SearchOrchestratorService {

    private final QueryParserService queryParserService;
    private final AniListApiService aniListApiService;
    private final EmbeddingSearchService embeddingSearchService;
    private final UserHistoryService userHistoryService;
    private final ReRankService reRankService;
    private final AnimeVectorMapper animeVectorMapper;
    private final com.animeflix.aisearchservice.client.GeminiClient geminiClient;

    public Mono<SearchResponseDTO> search(SearchRequestDTO request) {
        log.info("🔎 Search request: query='{}', userId='{}'",
                request.getQuery(), request.getUserId());

        return queryParserService.parse(request.getQuery())
                .flatMap(parsed -> {
                    if (Boolean.FALSE.equals(parsed.getFallbackToEmbedding())
                            && parsed.getConfidence() != null
                            && parsed.getConfidence() >= 0.75) {

                        log.info("🎯 Using STRUCTURED path (confidence={})", parsed.getConfidence());
                        return structuredPath(request, parsed);

                    } else {
                        log.info("🧠 Using SEMANTIC path (confidence={})", parsed.getConfidence());
                        return semanticPath(request, parsed);
                    }
                });
    }

    // ===================== STRUCTURED PATH =====================
    private Mono<SearchResponseDTO> structuredPath(SearchRequestDTO request, ParsedQueryDTO parsed) {
        return Mono.zip(
                aniListApiService.search(parsed, request.getPage(), request.getPerPage()),
                userHistoryService.getGenrePreference(request.getUserId())
        ).map(tuple -> {
            AniListApiService.SearchResult searchResult = tuple.getT1();
            Map<String, Integer> userPreference = tuple.getT2();

            List<AnimeSearchResultDTO> reranked = reRankService.rerankStructured(
                    searchResult.items(), userPreference);

            return SearchResponseDTO.builder()
                    .results(reranked)
                    .totalCount(reranked.size())  // ← số thực sau rerank, không lấy từ AniList
                    .page(request.getPage())
                    .perPage(request.getPerPage())
                    .searchType("STRUCTURED")
                    .parsedQuery(parsed)
                    .build();
        });
    }

    // ===================== SEMANTIC PATH =====================
    private Mono<SearchResponseDTO> semanticPath(SearchRequestDTO request, ParsedQueryDTO parsed) {
        String queryToEmbed = (parsed.getReasoning() != null
                && !parsed.getReasoning().isBlank())
                ? parsed.getReasoning()
                : request.getQuery();

        log.info("🧠 Embedding: '{}'", queryToEmbed);
        return geminiClient.embed(queryToEmbed)
                .flatMap(queryVector ->
                        Mono.zip(
                                embeddingSearchService.searchPaged(
                                        queryVector, request.getPage(), request.getPerPage()),
                                userHistoryService.getGenrePreference(request.getUserId())
                        )
                )
                .map(tuple -> {
                    EmbeddingSearchService.SearchPageResult pageResult = tuple.getT1();
                    Map<String, Integer> userPreference = tuple.getT2();

                    // Map ScoredAnimeVector → DTO với similarityScore đúng
                    List<AnimeSearchResultDTO> dtos = pageResult.items().stream()
                            .map(scored -> animeVectorMapper.toSearchResult(
                                    scored.animeVector(), scored.similarityScore()))
                            // Chỉ giữ lại anime có similarityScore >= 0.78
                            .filter(dto -> dto.getSimilarityScore() != null
                                    && dto.getSimilarityScore() >= 0.78)
                            .collect(toList());

                    // Rerank toàn bộ fetch
                    List<AnimeSearchResultDTO> reranked =
                            reRankService.rerankSemantic(dtos, userPreference);

                    // Paginate sau rerank
                    int fromIndex = (request.getPage() - 1) * request.getPerPage();
                    int toIndex = Math.min(fromIndex + request.getPerPage(), reranked.size());
                    List<AnimeSearchResultDTO> paginated =
                            (fromIndex < reranked.size())
                                    ? reranked.subList(fromIndex, toIndex)
                                    : List.of();

                    return SearchResponseDTO.builder()
                            .results(paginated)
                            .totalCount(pageResult.totalCount())
                            .page(request.getPage())
                            .perPage(request.getPerPage())
                            .searchType("SEMANTIC")
                            .parsedQuery(parsed)
                            .build();
                });
    }
}