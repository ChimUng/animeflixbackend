package com.animeflix.animeepisode.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnifyEpisode {
    @JsonProperty("id") private String id;
    @JsonProperty("episodeId") private String episodeId;
    @JsonProperty("number") private Integer number;
    @JsonProperty("title") private String title;
    @JsonProperty("isFiller") private Boolean isFiller;
    @JsonProperty("image") private String image;
    @JsonProperty("description") private String description;
}