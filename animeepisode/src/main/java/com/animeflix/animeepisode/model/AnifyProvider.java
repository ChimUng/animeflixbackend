package com.animeflix.animeepisode.model;

import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class AnifyProvider {
    private String providerId;
    private Map<String, List<AnifyEpisode>> episodes;  // or List<AnifyEpisode>
}