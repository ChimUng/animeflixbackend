package com.animeflix.animeepisode.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class EpisodeMeta {
    @JsonProperty("number") private Integer number;
    @JsonProperty("episode") private Integer episode;
    @JsonProperty("img") private String img;
    @JsonProperty("image") private String image;
    @JsonProperty("title") private String title;  // Simplified
    @JsonProperty("titles") private Map<String, String> titles = new HashMap<>();  // NEW: Multi-lang { "ja": "...", "en": "...", "x-jat": "..." }
    @JsonProperty("description") private String description;
    @JsonProperty("overview") private String overview;
    @JsonProperty("summary") private String summary;
    @JsonProperty("airDate") private String airDate;
    @JsonProperty("airDateUtc") private String airDateUtc;
    @JsonProperty("runtime") private Integer runtime;
    @JsonProperty("rating") private String rating;
    @JsonProperty("anidbEid") private Integer anidbEid;
    @JsonAnySetter private Map<String, Object> dynamicFields = new HashMap<>();
}
