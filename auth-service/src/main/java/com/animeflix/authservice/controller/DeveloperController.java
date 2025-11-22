package com.animeflix.authservice.controller;

import com.animeflix.authservice.DTO.LoginDevRequest;
import com.animeflix.authservice.DTO.LoginDevResponse;
import com.animeflix.authservice.DTO.RegisterDevRequest;
import com.animeflix.authservice.DTO.RegisterDevResponse;
import com.animeflix.authservice.service.DeveloperService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class DeveloperController {

    private final DeveloperService authService;

    @PostMapping("/dev/register")
    public Mono<ResponseEntity<RegisterDevResponse>> registerDev (@RequestBody RegisterDevRequest req) {
        return authService.registerDeveloper(req)
                .map(resp -> ResponseEntity.status(HttpStatus.CREATED).body(resp));
    }

    @PostMapping("/dev/login")
    public Mono<ResponseEntity<LoginDevResponse>> loginDev(@RequestBody LoginDevRequest req) {
        return authService.loginDeveloper(req)
                .map(ResponseEntity::ok);
    }
}