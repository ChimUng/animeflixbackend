package com.animeflix.animeepisode.service;

import com.animeflix.animeepisode.model.Episode;
import com.animeflix.animeepisode.model.Provider;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Component
public class GogoanimeClient {

    private final WebClient gogoWebClient;  // From config, base anitaku.to or zoro

    public GogoanimeClient(WebClient gogoWebClient) {
        this.gogoWebClient = gogoWebClient;
    }

    public Mono<Provider> fetchGogoanime(String subUrl, String dubUrl) {
        // Fetch from subUrl/dubUrl using WebClient
        return Mono.just(new Provider("gogoanime", "gogoanime", false,
                Map.of("sub", List.of(new Episode()), "dub", List.of(new Episode()))));  // Placeholder
    }
}
