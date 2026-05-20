package com.animeflix.authservice.filter;

import com.animeflix.authservice.service.DeveloperService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class ApiKeyAuthenticationFilter implements WebFilter {

    private final DeveloperService developerService;

    public ApiKeyAuthenticationFilter(DeveloperService developerService) {
        this.developerService = developerService;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().toString();

        if (path.startsWith("/api/auth/internal/") ||
                path.startsWith("/api/auth/dev/register") ||
                path.startsWith("/api/auth/dev/login") ||
                path.startsWith("/api/auth/user/") ||
                path.contains("/actuator")) {
            return chain.filter(exchange);
        }

        String apiKey = exchange.getRequest().getHeaders().getFirst("X-API-KEY");

        if (apiKey == null || apiKey.isBlank()) {
            log.warn("Missing X-API-KEY for path: {}", path);
            return writeErrorResponse(exchange, HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", "Missing API Key in Headers");
        }

        return developerService.validateApiKey(apiKey)
                .flatMap(dev -> {
                    log.info("API Key valid: {} -> Tiến vào hệ thống để tìm Router: {}", apiKey, path);
                    return chain.filter(exchange);
                })
                .onErrorResume(err -> {
                    log.error("Filter Caught Error for path [{}]: {}", path, err.getMessage());

                    if (err.getMessage() != null && (err.getMessage().contains("Unable to connect")
                            || err.getMessage().contains("Redis")
                            || err instanceof org.springframework.data.redis.RedisConnectionFailureException)) {
                        return writeErrorResponse(exchange, HttpStatus.SERVICE_UNAVAILABLE, "REDIS_ERROR", "Hệ thống bộ nhớ đệm tạm thời gián đoạn.");
                    }

                    if (err.getMessage() != null && err.getMessage().contains("Rate limit")) {
                        return writeErrorResponse(exchange, HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMIT_EXCEEDED", err.getMessage());
                    }

                    String errorMsg = err.getMessage() != null ? err.getMessage() : "Invalid API Key";
                    return writeErrorResponse(exchange, HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", errorMsg);
                });
    }

    /**
     * Hàm ép Filter trả về JSON đẹp đẽ ra Postman tập trung
     */
    private Mono<Void> writeErrorResponse(ServerWebExchange exchange, HttpStatus status, String errorCode, String errorMessage) {
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

        String jsonBody = String.format(
                "{\"error\":\"%s\",\"message\":\"%s\"}",
                errorCode, errorMessage
        );

        return exchange.getResponse().writeWith(
                Mono.just(exchange.getResponse().bufferFactory().wrap(jsonBody.getBytes()))
        );
    }
}