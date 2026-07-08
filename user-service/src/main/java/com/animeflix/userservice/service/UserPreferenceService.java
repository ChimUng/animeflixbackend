package com.animeflix.userservice.service;

import com.animeflix.userservice.dto.request.UpdatePreferencesRequest;
import com.animeflix.userservice.dto.response.UserPreferenceResponse;
import com.animeflix.userservice.entity.UserPreference;
import com.animeflix.userservice.mapper.UserPreferenceMapper;
import com.animeflix.userservice.repository.UserPreferenceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserPreferenceService {

    private final UserPreferenceRepository preferenceRepo;
    private final UserPreferenceMapper mapper;

    // Lấy preferences, tự tạo default nếu user chưa có
    public Mono<UserPreferenceResponse> getPreferences(String userId) {
        return preferenceRepo.findByUserId(userId).switchIfEmpty(createDefaultPreferences(userId)).map(mapper::toResponse);
    }

    // Update preferences, tự tạo default trước nếu chưa có rồi mới apply update
    public Mono<UserPreferenceResponse> updatePreferences(String userId, UpdatePreferencesRequest request) {
        return preferenceRepo.findByUserId(userId)
                .switchIfEmpty(createDefaultPreferences(userId))
                .flatMap(preference -> applyUpdate(preference, request))
                .map(mapper::toResponse);
    }

    private Mono<UserPreference> applyUpdate(UserPreference preference, UpdatePreferencesRequest request) {
        mapper.updateEntity(request, preference);
        preference.setUpdatedAt(LocalDateTime.now());
        return preferenceRepo.save(preference);
    }

    // Tạo default preferences cho user mới
    private Mono<UserPreference> createDefaultPreferences(String userId) {
        log.info("Creating default preferences for user: {}", userId);

        UserPreference defaultPrefs = UserPreference.builder()
                .userId(userId)
                .enableNotifications(true)
                .notifyOnlyFavorites(false)
                .notifyRecommendations(true)
                .autoPlayNext(true)
                .preferredQuality("1080p")
                .preferredLanguage("sub")
                .publicProfile(false)
                .showWatchHistory(true)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        return preferenceRepo.save(defaultPrefs);
    }

    // Check user có bật notification không — dùng ở EpisodeEventConsumer trước khi tạo notification
    public Mono<Boolean> isNotificationEnabled(String userId) {
        return preferenceRepo.findByUserId(userId).map(UserPreference::getEnableNotifications).defaultIfEmpty(true);
    }
}