package com.animeflix.apigateway.filter;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;

@Component
public class AuthenticationFilter extends AbstractGatewayFilterFactory<AuthenticationFilter.Config> {

    private final WebClient authServiceWebClient;

    public AuthenticationFilter(@Qualifier("authServiceWebClient") WebClient authServiceWebClient) {
        super(Config.class);
        this.authServiceWebClient = authServiceWebClient;
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            ServerHttpRequest request = exchange.getRequest();
            String path = request.getPath().toString();

            if (path.contains("/api/auth/dev/register") ||
                    path.contains("/api/auth/dev/login") ||
                    path.contains("/actuator")) {
                return chain.filter(exchange);
            }

            String apiKey = request.getHeaders().getFirst("X-API-KEY");
            if (apiKey == null || apiKey.isBlank()) {
                exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                return exchange.getResponse().setComplete();
            }

            return authServiceWebClient.post()
                    .uri("/api/auth/internal/validate-key")
                    .header("X-API-KEY", apiKey)
                    .retrieve()
                    .toBodilessEntity()
                    .flatMap(response -> chain.filter(exchange))
                    .onErrorResume(e -> {
                        HttpStatus status = HttpStatus.UNAUTHORIZED;
                        if (e.getMessage() != null && e.getMessage().contains("429")) {
                            status = HttpStatus.TOO_MANY_REQUESTS;
                        }
                        exchange.getResponse().setStatusCode(status);
                        return exchange.getResponse().setComplete();
                    });
        };
    }

    public static class Config {}
}