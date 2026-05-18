package com.animeflix.authservice.filter;

import com.animeflix.authservice.Entity.User;
import com.animeflix.authservice.service.UserService;
import com.animeflix.authservice.util.JwtUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.security.web.server.context.ServerSecurityContextRepository;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter implements WebFilter {

    private final JwtUtil jwtUtil;
    private final UserService userService;
    private final ServerSecurityContextRepository securityContextRepository;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String token = resolveToken(exchange);

        if (token == null || !jwtUtil.isValid(token, false)) {
            return chain.filter(exchange);
        }

        String userId = jwtUtil.extractUserId(token, false);
        String username = jwtUtil.extractUsername(token);
        String email = jwtUtil.extractEmail(token);
        User principalUser = new User();
        principalUser.setId(userId);
        principalUser.setUsername(username);
        principalUser.setEmail(email);

        var auth = new UsernamePasswordAuthenticationToken(
                principalUser,
                null,
                List.of(new SimpleGrantedAuthority("ROLE_USER"))
        );
        return chain.filter(exchange)
                .contextWrite(ReactiveSecurityContextHolder.withAuthentication(auth));
    }

    private String resolveToken(ServerWebExchange exchange) {
        // Ưu tiên header
        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            return authHeader.substring(7);
        }

        // Fallback: cookie
        return Optional.ofNullable(exchange.getRequest().getCookies().getFirst("access_token"))
                .map(HttpCookie::getValue)
                .orElse(null);
    }
}