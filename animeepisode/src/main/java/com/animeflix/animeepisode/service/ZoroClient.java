package com.animeflix.animeepisode.service;

import com.animeflix.animeepisode.model.Episode;
import com.animeflix.animeepisode.model.Provider;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Component
public class ZoroClient {

    private final WebClient zoroWebClient;

    public ZoroClient(WebClient zoroWebClient) {
        this.zoroWebClient = zoroWebClient;
    }

    public Mono<Provider> fetchZoro(String zoroUrl) {
        // Fetch from zoroUrl
        return Mono.just(new Provider("zoro", "zoro", false, Map.of("sub", List.of(new Episode()))));
    }
}