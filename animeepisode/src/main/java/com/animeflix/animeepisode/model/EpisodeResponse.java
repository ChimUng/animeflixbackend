package com.animeflix.animeepisode.model;

import java.util.List;
import lombok.Data;

@Data
public class EpisodeResponse {
    private List<Provider> providers;
    private List<EpisodeMeta> meta;
}
