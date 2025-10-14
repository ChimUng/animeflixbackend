package com.animeflix.animeepisode.service;

import com.animeflix.animeepisode.exception.EpisodeFetchException;
import com.animeflix.animeepisode.model.EpisodeMeta;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Component
public class AniZipClient {

    private final WebClient animappingWebClient;
    private final ObjectMapper objectMapper;

    public AniZipClient(WebClient animappingWebClient, ObjectMapper objectMapper) {
        this.animappingWebClient = animappingWebClient;
        this.objectMapper = objectMapper;
    }

    public Mono<Map<String, String>> fetchTitles(String anilistId) {
        return fetchMappings(anilistId)
                .map(response -> {
                    // Extract titles from response
                    JsonNode titlesNode = response.path("titles");
                    Map<String, String> titles = objectMapper.convertValue(titlesNode, Map.class);
                    return titles;
                })
                .onErrorMap(e -> new EpisodeFetchException("AniZip titles fetch failed for ID: " + anilistId, e));
    }

    public Mono<String> fetchMalId(String anilistId) {
        return fetchMappings(anilistId)
                .map(response -> {
                    JsonNode mappingsNode = response.path("mappings");
                    return mappingsNode.path("mal_id").asText(null);
                })
                .onErrorMap(e -> new EpisodeFetchException("AniZip MAL ID fetch failed for ID: " + anilistId, e));
    }

    public Mono<List<EpisodeMeta>> fetchEpisodesMeta(String anilistId) {
        return fetchMappings(anilistId)
                .map(response -> {
                    JsonNode episodesNode = response.path("episodes");
                    List<EpisodeMeta> metas = new ArrayList<>();
                    Iterator<Map.Entry<String, JsonNode>> fields = episodesNode.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fields.next();
                        String epKey = entry.getKey();  // e.g., "1" or "S2"
                        JsonNode epNode = entry.getValue();

                        EpisodeMeta meta = objectMapper.convertValue(epNode, EpisodeMeta.class);
                        // Set episode from key if not set (though JSON has "episode" as String)
                        if (meta.getEpisode() == null) {
                            meta.setEpisode(epKey);
                        }

                        // Set number: Try parse from epKey; set null for specials like "S2"
                        try {
                            meta.setNumber(Integer.valueOf(epKey));
                        } catch (NumberFormatException e) {
                            meta.setNumber(null);  // Skip parse error for specials
                        }

                        meta.setTitles(extractTitlesFromEp(epNode.path("title")));  // Multi-lang titles per ep

                        // Set description: Prefer "summary" (most entries), fallback "overview" (early only)
                        String desc = epNode.path("summary").asText(null);
                        if (desc == null || desc.isBlank()) {
                            desc = epNode.path("overview").asText("");
                        }
                        meta.setDescription(desc);

                        meta.setImage(epNode.path("image").asText(""));  // Only in early eps

                        metas.add(meta);
                    }
                    return metas;
                })
                .defaultIfEmpty(List.of())
                .onErrorMap(e -> new EpisodeFetchException("AniZip episodes meta fetch failed for ID: " + anilistId, e));
    }

    private Mono<JsonNode> fetchMappings(String anilistId) {
        String uri = "?anilist_id=" + anilistId;  // Append query to base
        return animappingWebClient.get().uri(uri).retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorMap(e -> new EpisodeFetchException("AniZip mappings fetch failed for ID: " + anilistId, e));
    }

    private Map<String, String> extractTitlesFromEp(JsonNode titleNode) {
        // Similar to global titles: convert to Map { "ja": "...", "en": "...", "x-jat": "..." }
        return objectMapper.convertValue(titleNode, Map.class);
    }
}