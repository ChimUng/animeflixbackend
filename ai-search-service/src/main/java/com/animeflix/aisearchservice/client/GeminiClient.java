package com.animeflix.aisearchservice.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// GeminiClient.java

@Component
@RequiredArgsConstructor
@Slf4j
public class GeminiClient {

    private final WebClient geminiWebClient;
    private final ObjectMapper objectMapper;

    @Value("${gemini.api.key}")
    private String apiKey;
    @Value("${gemini.api.embedding-model}")
    private String embeddingModel;
    @Value("${gemini.api.chat-model}")
    private String chatModel;
    @Value("${search.embedding.output-dimensionality:768}")
    private int outputDimensionality;

    // Circuit breaker state cho chat (không dùng cho embed)
    private volatile boolean chatCircuitOpen = false;
    private volatile long circuitOpenUntil = 0L;
    private static final long CIRCUIT_COOLDOWN_MS = 60_000L; // 1 phút

    @PostConstruct
    public void init() {
        log.info("Gemini API key: OK (length={})", apiKey != null ? apiKey.length() : 0);
        log.info("Embedding model: {}", embeddingModel);
        log.info("Chat model: {}", chatModel);
    }

    public Mono<List<Double>> embed(String text) {
        String url = "/models/" + embeddingModel + ":embedContent?key=" + apiKey;

        Map<String, Object> body = Map.of(
                "model", "models/" + embeddingModel,
                "content", Map.of("parts", List.of(Map.of("text", text))),
                "outputDimensionality", outputDimensionality
        );

        return geminiWebClient.post()
                .uri(url)
                .bodyValue(body)
                .retrieve()
                // Embed ít bị 429 hơn, nhưng vẫn nên handle
                .onStatus(status -> status.value() == 429,
                        resp -> resp.bodyToMono(String.class)
                                .flatMap(body2 -> {
                                    log.warn("Gemini embed 429 - backing off");
                                    return Mono.error(new RuntimeException("Embed rate limited"));
                                }))
                .bodyToMono(JsonNode.class)
                .map(response -> {
                    JsonNode valuesNode = response.path("embedding").path("values");
                    List<Double> vector = new ArrayList<>();
                    valuesNode.elements().forEachRemaining(v -> vector.add(v.asDouble()));
                    log.debug("Embedded text, vector size: {}", vector.size());
                    return vector;
                })
                // Embed: retry 2 lần với delay dài hơn
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(5))
                        .filter(e -> e.getMessage() != null &&
                                e.getMessage().contains("rate limited"))
                        .doBeforeRetry(s -> log.warn("Retrying embed, attempt: {}",
                                s.totalRetries() + 1)))
                .doOnError(e -> log.error("Gemini embed error: {}", e.getMessage()));
    }

    public Mono<String> chat(String systemPrompt, String userMessage) {
        // Circuit breaker check - nếu đang open thì skip ngay, không gọi API
        if (chatCircuitOpen && System.currentTimeMillis() < circuitOpenUntil) {
            long remainingSec = (circuitOpenUntil - System.currentTimeMillis()) / 1000;
            log.warn("Chat circuit OPEN - skipping Gemini chat, {}s remaining. " +
                    "Falling back to embedding path.", remainingSec);
            return Mono.error(new RuntimeException("Circuit open - rate limited"));
        } else if (chatCircuitOpen) {
            // Reset circuit
            chatCircuitOpen = false;
            log.info("Chat circuit CLOSED - resuming Gemini chat calls");
        }

        String url = "/models/" + chatModel + ":generateContent?key=" + apiKey;

        Map<String, Object> body = Map.of(
                "system_instruction", Map.of(
                        "parts", List.of(Map.of("text", systemPrompt))
                ),
                "contents", List.of(
                        Map.of("role", "user",
                                "parts", List.of(Map.of("text", userMessage)))
                ),
                "generationConfig", Map.of(
                        "temperature", 0.1,
                        "responseMimeType", "application/json"
                )
        );

        return geminiWebClient.post()
                .uri(url)
                .bodyValue(body)
                .retrieve()
                // Handle 429 riêng - KHÔNG retry, mở circuit breaker ngay
                .onStatus(status -> status.value() == 429,
                        resp -> resp.bodyToMono(String.class)
                                .flatMap(b -> {
                                    openCircuit();
                                    return Mono.error(
                                            new RateLimitException("Gemini chat 429"));
                                }))
                .bodyToMono(JsonNode.class)
                .map(response -> {
                    String text = response
                            .path("candidates").path(0)
                            .path("content").path("parts").path(0)
                            .path("text").asText();
                    log.debug("Gemini chat response: {}", text);
                    return text;
                })
                // Chỉ retry với lỗi network, KHÔNG retry 429
                .retryWhen(Retry.backoff(1, Duration.ofSeconds(3))
                        .filter(e -> !(e instanceof RateLimitException))
                        .doBeforeRetry(s -> log.warn("Retrying Gemini chat (network error), " +
                                "attempt: {}", s.totalRetries() + 1)))
                .doOnError(e -> {
                    if (!(e instanceof RateLimitException)) {
                        log.error("Gemini chat error: {}", e.getMessage());
                    }
                });
    }

    private void openCircuit() {
        chatCircuitOpen = true;
        circuitOpenUntil = System.currentTimeMillis() + CIRCUIT_COOLDOWN_MS;
        log.warn("Chat circuit OPENED - will skip chat calls for {}s",
                CIRCUIT_COOLDOWN_MS / 1000);
    }

    // Sentinel exception để phân biệt 429 vs lỗi network
    public static class RateLimitException extends RuntimeException {
        public RateLimitException(String msg) { super(msg); }
    }
}