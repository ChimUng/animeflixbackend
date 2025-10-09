package com.animeflix.animetranslate.controller;

import com.animeflix.animetranslate.model.TranslationResponse;
import com.animeflix.animetranslate.service.GeminiClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/testtranslate")
public class TranslateTestController {

    private final GeminiClient geminiClient;

    public TranslateTestController(GeminiClient geminiClient) {
        this.geminiClient = geminiClient;
    }

    @GetMapping
    public Mono<List<TranslationResponse>> testTranslate() {
        List<Map<String, String>> mockPrompts = List.of(
                Map.of("anilistId", "1", "prompt", "Title: One Piece\nDescription: The story of a boy who wants to become the Pirate King.")
        );
        return geminiClient.translate(mockPrompts);
    }
}
