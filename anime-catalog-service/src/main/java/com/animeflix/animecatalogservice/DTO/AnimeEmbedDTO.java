package com.animeflix.animecatalogservice.DTO;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class AnimeEmbedDTO {
    private String id;
    private String titleRomaji;
    private String titleEnglish;
    private String titleUserPreferred;
    private String description;
    private List<String> genres;
    private List<String> tags;
    private String coverImageLarge;
    private String coverImageExtraLarge;
    private String bannerImage;
    private Integer averageScore;
    private Integer popularity;
    private String status;
    private String format;
    private String season;
    private Integer seasonYear;
}