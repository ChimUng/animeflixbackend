package com.animeflix.authservice.service;

import com.animeflix.authservice.DTO.LoginDevRequest;
import com.animeflix.authservice.DTO.LoginDevResponse;
import com.animeflix.authservice.DTO.RegisterDevRequest;
import com.animeflix.authservice.DTO.RegisterDevResponse;
import com.animeflix.authservice.Entity.Developer;
import com.animeflix.authservice.Repository.DeveloperRepository;
import com.animeflix.authservice.mapper.DeveloperMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeveloperService {

    private final DeveloperRepository devRepo;
    private final PasswordEncoder passwordEncoder;
    private final DeveloperMapper devMapper;

    /**
     * ĐĂNG KÝ DEVELOPER – TRẢ VỀ API KEY NGAY
     */
    public Mono<RegisterDevResponse> registerDeveloper(RegisterDevRequest req) {
        log.info("Registering developer: appId={}, email={}", req.getAppId(), req.getEmail());

        return devRepo.findByAppId(req.getAppId())
                .flatMap(exists -> Mono.<Developer>error(new RuntimeException("App ID đã tồn tại")))
                .switchIfEmpty(devRepo.findByEmail(req.getEmail())
                        .flatMap(exists -> Mono.<Developer>error(new RuntimeException("Email đã được dùng")))
                        .switchIfEmpty(Mono.defer(() -> {
                            // DÙNG MAPPER TẠO ENTITY GỐC
                            Developer dev = devMapper.toEntity(req);

                            // CHỈ CÒN CÁI NÀY LÀM Ở SERVICE
                            dev.setPasswordHash(passwordEncoder.encode(req.getPassword()));
                            dev.setClientId("cli_" + RandomStringUtils.randomAlphanumeric(14));
                            dev.setClientSecret("sec_" + RandomStringUtils.randomAlphanumeric(32));
                            dev.setApiKey("api_" + RandomStringUtils.randomAlphanumeric(28));
                            dev.setLastUsedAt(null);

                            log.info("Saving new developer: {}", dev.getAppId());
                            return devRepo.save(dev);
                        })))
                .map(devMapper::toRegisterResponse);
    }

    public Mono<LoginDevResponse> loginDeveloper(LoginDevRequest req) {
        return devRepo.findByUsername(req.getUsername())
                .switchIfEmpty(Mono.error(new RuntimeException("Invalid email or password")))
                .flatMap(dev -> {
                    if (!passwordEncoder.matches(req.getPassword(), dev.getPasswordHash())) {
                        return Mono.error(new RuntimeException("Invalid username or password"));
                    }
                    if (!dev.isActive()) {
                        return Mono.error(new RuntimeException("Developer account is disabled"));
                    }
                    dev.setLastUsedAt(LocalDateTime.now());
                    return devRepo.save(dev)
                            .map(devMapper::toLoginResponse);
                });
    }
    /**
     * XÁC THỰC API KEY KHI DEV GỌI API
     */
    public Mono<Developer> validateApiKey(String apiKey) {
        return devRepo.findByApiKey(apiKey)
                .switchIfEmpty(Mono.error(new RuntimeException("Invalid API Key")))
                .flatMap(dev -> {
                    if (!dev.isActive()) {
                        return Mono.error(new RuntimeException("API Key đã bị khóa"));
                    }
                    dev.setLastUsedAt(LocalDateTime.now());
                    return devRepo.save(dev).thenReturn(dev);
                });
    }
}