// DeveloperMapper.java → ĐÃ HOÀN THIỆN
package com.animeflix.authservice.mapper;

import com.animeflix.authservice.DTO.RegisterDevRequest;
import com.animeflix.authservice.DTO.RegisterDevResponse;
import com.animeflix.authservice.Entity.Developer;
import org.springframework.stereotype.Component;

@Component
public class DeveloperMapper {

    public Developer toEntity(RegisterDevRequest req) {
        Developer dev = new Developer();
        dev.setAppId(req.getAppId());
        dev.setAppName(req.getAppName());
        dev.setUsername(req.getUsername());
        dev.setEmail(req.getEmail());
        return dev;
    }

    public RegisterDevResponse toResponse(Developer dev) {
        return RegisterDevResponse.builder()
                .clientId(dev.getClientId())
                .clientSecret(dev.getClientSecret())
                .apiKey(dev.getApiKey())
                .build();
    }
}