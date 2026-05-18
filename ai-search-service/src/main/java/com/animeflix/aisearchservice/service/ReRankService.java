package com.animeflix.aisearchservice.service;

import com.animeflix.aisearchservice.dto.response.AnimeSearchResultDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Re-ranking service: tính final score cho mỗi anime trước khi trả về FE.
 *
 * STRUCTURED path:
 *   finalScore = 0.35 * popularity + 0.30 * score + 0.20 * trending + 0.15 * personalized
 *
 * SEMANTIC path (tăng cosine weight để phân biệt rõ kết quả):
 *   finalScore = 0.70 * cosine + 0.15 * popularity + 0.10 * score + 0.05 * personalized
 *
 * Lý do tăng cosine lên 0.70:
 * - Cosine similarity từ 0.78 → 0.87 (range 0.09) rất hẹp
 * - Popularity có thể lên đến 500k-900k, át mất signal từ cosine
 * - Muốn "vua hải tặc" → ONE PIECE top 1, không phải anime phổ biến nhất
 */
@Service
@Slf4j
public class ReRankService {

    // Weights structured path
    @Value("${search.rerank.weights.structured.popularity:0.35}")
    private double wStructuredPopularity;
    @Value("${search.rerank.weights.structured.score:0.30}")
    private double wStructuredScore;
    @Value("${search.rerank.weights.structured.trending:0.20}")
    private double wStructuredTrending;
    @Value("${search.rerank.weights.structured.personalized:0.15}")
    private double wStructuredPersonalized;

    // Weights semantic path — cosine dominant
    @Value("${search.rerank.weights.semantic.cosine-similarity:0.70}")
    private double wSemanticCosine;
    @Value("${search.rerank.weights.semantic.popularity:0.15}")
    private double wSemanticPopularity;
    @Value("${search.rerank.weights.semantic.score:0.10}")
    private double wSemanticScore;
    @Value("${search.rerank.weights.semantic.personalized:0.05}")
    private double wSemanticPersonalized;

    private static final double MAX_POPULARITY = 500_000.0;
    private static final double MAX_SCORE = 100.0;

    /**
     * Rerank kết quả structured path (AniList API).
     */
    public List<AnimeSearchResultDTO> rerankStructured(
            List<AnimeSearchResultDTO> results,
            Map<String, Integer> userGenrePreference) {

        int maxGenreCount = userGenrePreference.values().stream()
                .mapToInt(Integer::intValue).max().orElse(1);

        return results.stream()
                .map(anime -> {
                    double popularityScore = normalize(
                            anime.getPopularity() != null ? anime.getPopularity() : 0,
                            MAX_POPULARITY);
                    double scoreScore = normalize(
                            anime.getAverageScore() != null ? anime.getAverageScore() : 0,
                            MAX_SCORE);
                    double trendingBoost = "RELEASING".equals(anime.getStatus()) ? 1.0 : 0.5;
                    double personalizedScore = calcPersonalizedScore(
                            anime.getGenres(), userGenrePreference, maxGenreCount);

                    double finalScore =
                            wStructuredPopularity * popularityScore +
                                    wStructuredScore * scoreScore +
                                    wStructuredTrending * trendingBoost +
                                    wStructuredPersonalized * personalizedScore;

                    return anime.toBuilder().rankScore(finalScore).build();
                })
                .sorted(Comparator.comparingDouble(AnimeSearchResultDTO::getRankScore).reversed())
                .collect(Collectors.toList());
    }

    /**
     * Rerank kết quả semantic path (Vector Search).
     *
     * Cosine score chiếm 70% để đảm bảo relevance thắng popularity.
     * Popularity/score chỉ là tiebreaker khi cosine gần bằng nhau.
     */
    public List<AnimeSearchResultDTO> rerankSemantic(
            List<AnimeSearchResultDTO> results,
            Map<String, Integer> userGenrePreference) {

        int maxGenreCount = userGenrePreference.values().stream()
                .mapToInt(Integer::intValue).max().orElse(1);

        return results.stream()
                .map(anime -> {
                    double cosineScore = anime.getSimilarityScore() != null
                            ? anime.getSimilarityScore() : 0.0;
                    double popularityScore = normalize(
                            anime.getPopularity() != null ? anime.getPopularity() : 0,
                            MAX_POPULARITY);
                    double scoreScore = normalize(
                            anime.getAverageScore() != null ? anime.getAverageScore() : 0,
                            MAX_SCORE);
                    double personalizedScore = calcPersonalizedScore(
                            anime.getGenres(), userGenrePreference, maxGenreCount);

                    double finalScore =
                            wSemanticCosine * cosineScore +
                                    wSemanticPopularity * popularityScore +
                                    wSemanticScore * scoreScore +
                                    wSemanticPersonalized * personalizedScore;

                    log.debug("Anime={}, cosine={:.3f}, popularity={:.3f}, finalScore={:.3f}",
                            anime.getTitleRomaji(), cosineScore, popularityScore, finalScore);

                    return anime.toBuilder().rankScore(finalScore).build();
                })
                .sorted(Comparator.comparingDouble(AnimeSearchResultDTO::getRankScore).reversed())
                .collect(Collectors.toList());
    }

    private double normalize(double value, double max) {
        if (max == 0) return 0;
        return Math.min(value / max, 1.0);
    }

    private double calcPersonalizedScore(
            List<String> animeGenres,
            Map<String, Integer> preference,
            int maxCount) {

        if (animeGenres == null || animeGenres.isEmpty()
                || preference == null || preference.isEmpty()) {
            return 0.0;
        }
        double totalScore = animeGenres.stream()
                .mapToInt(genre -> preference.getOrDefault(genre, 0))
                .sum();
        return normalize(totalScore, (double) animeGenres.size() * maxCount);
    }
}