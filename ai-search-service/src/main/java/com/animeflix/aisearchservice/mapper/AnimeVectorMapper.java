package com.animeflix.aisearchservice.mapper;

import com.animeflix.aisearchservice.Entity.AnimeVector;
import com.animeflix.aisearchservice.dto.response.AnimeSearchResultDTO;
import org.springframework.stereotype.Component;

/**
 * Mapper để chuyển AnimeVector entity → AnimeSearchResultDTO
 * Dùng trong semantic search path
 */
@Component
public class AnimeVectorMapper {

    /**
     * Map AnimeVector từ MongoDB sang DTO cho search result
     * Bao gồm cả similarity score từ vector search
     */
    public AnimeSearchResultDTO toSearchResult(AnimeVector animeVector) {
        if (animeVector == null) {
            return null;
        }

        return AnimeSearchResultDTO.builder()
                .id(animeVector.getId())
                .titleRomaji(animeVector.getTitleRomaji())
                .titleEnglish(animeVector.getTitleEnglish())
                .titleUserPreferred(animeVector.getTitleUserPreferred())
                .coverImage(animeVector.getCoverImageLarge())
                .bannerImage(animeVector.getBannerImage())
                .genres(animeVector.getGenres())
                .averageScore(animeVector.getAverageScore())
                .popularity(animeVector.getPopularity())
                .status(animeVector.getStatus())
                .format(animeVector.getFormat())
                .season(animeVector.getSeason())
                .seasonYear(animeVector.getSeasonYear())
                // similarityScore sẽ được set trong EmbeddingSearchService
                // nên tạm set null, sẽ được update sau
                .similarityScore(null)
                .rankScore(null)
                .build();
    }

    /**
     * Overload method để accept animeVector + similarity score
     */
    public AnimeSearchResultDTO toSearchResult(AnimeVector animeVector, Double similarityScore) {
        AnimeSearchResultDTO dto = toSearchResult(animeVector);
        if (dto != null) {
            dto.setSimilarityScore(similarityScore);
        }
        return dto;
    }
}