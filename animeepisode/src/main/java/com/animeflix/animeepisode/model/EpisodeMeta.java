package com.animeflix.animeepisode.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class EpisodeMeta {
    @JsonProperty("number") private Integer number;
    @JsonProperty("episode") private Integer episode;
    @JsonProperty("img") private String img;
    @JsonProperty("image") private String image;
    @JsonProperty("title") private String title;  // Simplified from object {en, x-jat}
    @JsonProperty("description") private String description;
    @JsonProperty("overview") private String overview;
    @JsonProperty("summary") private String summary;
}
