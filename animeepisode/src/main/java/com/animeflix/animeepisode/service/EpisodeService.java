package com.animeflix.animeepisode.service;

import com.animeflix.animeepisode.exception.EpisodeFetchException;
import com.animeflix.animeepisode.model.*;
import com.animeflix.animeepisode.repository.RedisEpisodeRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class EpisodeService {

    private final ConsumetClient consumetClient;
    private final AnifyClient anifyClient;
    private final MalSyncClient malSyncClient;
    private final GogoanimeClient gogoanimeClient;
    private final ZoroClient zoroClient;
    private final EpisodeMetaClient episodeMetaClient;
    private final AniZipClient aniZipClient;
    private final RedisEpisodeRepository redisRepository;
    private final ObjectMapper objectMapper;
    private static final Map<String, String> FALLBACK_ZORO = Map.of("11061", "hunter-x-hunter-2");

    public EpisodeService(ConsumetClient consumetClient, AnifyClient anifyClient, MalSyncClient malSyncClient,
                          GogoanimeClient gogoanimeClient, ZoroClient zoroClient, EpisodeMetaClient episodeMetaClient,
                          AniZipClient aniZipClient, RedisEpisodeRepository redisRepository, ObjectMapper objectMapper) {
        this.consumetClient = consumetClient;
        this.anifyClient = anifyClient;
        this.malSyncClient = malSyncClient;
        this.gogoanimeClient = gogoanimeClient;
        this.zoroClient = zoroClient;
        this.episodeMetaClient = episodeMetaClient;
        this.aniZipClient = aniZipClient;
        this.redisRepository = redisRepository;
        this.objectMapper = objectMapper;
    }

    public Mono<EpisodeResponse> getEpisodes(String id, boolean releasing, boolean refresh) {
        long cacheTime = releasing ? Duration.ofHours(3).getSeconds() : Duration.ofDays(45).getSeconds();

        Mono<String> cachedMono = redisRepository.getCachedData("episode:" + id);
        Mono<String> metaMono = redisRepository.getCachedData("info:" + id);

        return Mono.zip(cachedMono, metaMono)
                .filter(tuple -> !refresh)
                .flatMap(tuple -> {
                    List<Provider> providers = parseJsonToList(tuple.getT1(), Provider.class);
                    List<EpisodeMeta> metas = parseJsonToList(tuple.getT2(), EpisodeMeta.class);
                    if (providers.isEmpty()) redisRepository.deleteKey("episode:" + id).subscribe();
                    if (metas.isEmpty()) redisRepository.deleteKey("info:" + id).subscribe();
                    return Mono.just(combineEpisodeMeta(providers, metas));
                })
                .switchIfEmpty(performFreshFetch(id, cacheTime));
    }

    private Mono<EpisodeResponse> performFreshFetch(String id, long cacheTime) {
        return malSyncClient.fetchMalSync(id)
                .flatMap(malsync -> {
                    if (malsync != null && malsync.length > 0) {
                        MalSyncEntry gogo = Arrays.stream(malsync)
                                .filter(e -> "gogoanime".equalsIgnoreCase(e.getProviderId()))
                                .findFirst()
                                .orElse(null);

                        MalSyncEntry zoro = Arrays.stream(malsync)
                                .filter(e -> "zoro".equalsIgnoreCase(e.getProviderId()))
                                .findFirst()
                                .orElse(null);

                        Flux<Provider> gogoFlux = gogo != null
                                ? monoToProviderFluxSafe(gogoanimeClient.fetchGogoanime(gogo.getSub(), gogo.getDub()))
                                : Flux.empty();

                        Flux<Provider> zoroFlux = zoro != null
                                ? monoToProviderFluxSafe(zoroClient.fetchZoro(zoro.getSub()))
                                : Flux.empty();

                        return Flux.merge(gogoFlux, zoroFlux).collectList();
                    } else {
                        Flux<Provider> fallbackFlux = Flux.merge(
                                monoToProviderFluxSafe(consumetClient.fetchConsumet(id)),
                                monoToProviderFluxSafe(anifyClient.fetchAnify(id)),
                                FALLBACK_ZORO.containsKey(id)
                                        ? monoToProviderFluxSafe(zoroClient.fetchZoro(FALLBACK_ZORO.get(id)))
                                        : Flux.empty()
                        );
                        return fallbackFlux.collectList();
                    }
                })
                .flatMap(providers -> {
                    // Filter providers that actually have episodes
                    List<Provider> validProviders = providers == null ? List.of() :
                            providers.stream().filter(this::hasEpisodes).collect(Collectors.toList());

                    // Fetch metadata (AniZip primary, then EpisodeMetaClient fallback)
                    return aniZipClient.fetchEpisodesMeta(id)
                            .switchIfEmpty(episodeMetaClient.fetchEpisodeMeta(id, false))
                            .flatMap(metaList -> aniZipClient.fetchTitles(id)
                                    .defaultIfEmpty(Collections.emptyMap())
                                    .map(titles -> {
                                        // Attach titles to meta entries (merge safely)
                                        if (metaList != null) {
                                            for (EpisodeMeta m : metaList) {
                                                if (m.getTitles() == null) m.setTitles(titles);
                                                else {
                                                    Map<String, String> merged = new HashMap<>(titles);
                                                    merged.putAll(m.getTitles());
                                                    m.setTitles(merged);
                                                }
                                            }
                                        }
                                        return metaList;
                                    })
                            )
                            .map(meta -> {
                                EpisodeResponse response = combineEpisodeMeta(validProviders, meta);

                                // Cache both provider list and meta (assumes repository.setCachedData is generic)
                                redisRepository.setCachedData("episode:" + id, validProviders, cacheTime)
                                        .then(redisRepository.setCachedData("info:" + id, meta, cacheTime))
                                        .doOnError(err -> {
                                            // log or handle cache write errors (avoid silent fails)
                                            System.err.println("Cache write failed for id " + id + ": " + err.getMessage());
                                        })
                                        .subscribe();

                                return response;
                            });
                })
                .onErrorMap(e -> new EpisodeFetchException("Fresh fetch failed for ID: " + id, e));
    }

    /**
     * Convert any Mono that may emit:
     *  - Provider
     *  - List<Provider>
     * into a Flux<Provider> safely.
     */
    @SuppressWarnings("unchecked")
    private Flux<Provider> monoToProviderFluxSafe(Mono<?> mono) {
        if (mono == null) return Flux.empty();
        return mono.flatMapMany(obj -> {
            if (obj == null) return Flux.empty();
            if (obj instanceof Provider) {
                return Flux.just((Provider) obj);
            } else if (obj instanceof List<?>) {
                // assume list of Provider
                return Flux.fromIterable((List<Provider>) obj);
            } else {
                // unknown shape -> try to convert via ObjectMapper as a last resort
                try {
                    // attempt to convert object to Provider (if it's a map)
                    Provider p = objectMapper.convertValue(obj, Provider.class);
                    return p == null ? Flux.empty() : Flux.just(p);
                } catch (Exception ex) {
                    return Flux.empty();
                }
            }
        });
    }

    /**
     * Combine episode list from providers with image/meta data.
     * Handles providers whose .episodes is either:
     *   - List<Episode>
     *   - Map<String, List<Episode>> (sub/dub)
     */
    private EpisodeResponse combineEpisodeMeta(List<Provider> episodeData, List<EpisodeMeta> imageData) {
        Map<Integer, EpisodeMeta> episodeImages = new HashMap<>();
        if (imageData != null) {
            imageData.forEach(image -> {
                Integer key = image.getNumber() != null ? image.getNumber() : image.getEpisode();
                if (key != null) episodeImages.put(key, image);
            });
        }

        if (episodeData == null) episodeData = List.of();

        for (Provider provider : episodeData) {
            if (provider == null) continue;

            // Extract all episodes for this provider as a flat list
            List<Episode> allEpisodes = extractAllEpisodes(provider);

            for (Episode episode : allEpisodes) {
                if (episode == null) continue;
                EpisodeMeta imageInfo = episodeImages.get(episode.getNumber());
                if (imageInfo != null) {
                    // safe image
                    episode.setImg(imageInfo.getImg() != null ? imageInfo.getImg() :
                            (imageInfo.getImage() != null ? imageInfo.getImage() : episode.getImg()));

                    // safe title
                    String preferredTitle = null;
                    Map<String, String> titles = imageInfo.getTitles();
                    if (titles != null) {
                        preferredTitle = titles.getOrDefault("x-jat", titles.getOrDefault("en", null));
                    }
                    if (preferredTitle == null) preferredTitle = "EPISODE " + (episode.getNumber() != null ? episode.getNumber() : episode.getEpisodeId());
                    episode.setTitle(preferredTitle);

                    // safe description
                    String desc = imageInfo.getOverview() != null ? imageInfo.getOverview() : imageInfo.getSummary();
                    if (desc != null && !desc.isBlank()) episode.setDescription(desc);
                }
            }

            // If provider.episodes was a Map (sub/dub) but provider model typed as Map, we keep it as-is.
            // No need to convert structure here — frontend expects provider.episodes in original shape.
        }

        return new EpisodeResponse(episodeData, imageData);
    }

    @SuppressWarnings("unchecked")
    private List<Episode> extractAllEpisodes(Provider provider) {
        Object eps = provider.getEpisodes();
        if (eps == null) return Collections.emptyList();

        // Nếu eps thực sự là List<Episode>
        if (eps instanceof List<?>) {
            try {
                return (List<Episode>) eps;
            } catch (ClassCastException ex) {
                // nếu không thể cast trực tiếp, thử convert qua ObjectMapper (fallback)
                try {
                    return objectMapper.convertValue(eps, new TypeReference<List<Episode>>() {});
                } catch (Exception e) {
                    return Collections.emptyList();
                }
            }
        }

        // Nếu eps là Map (sub/dub hoặc các key khác)
        if (eps instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) eps;
            List<Episode> all = new ArrayList<>();

            Object subObj = map.get("sub");
            if (subObj instanceof List<?>) {
                all.addAll((List<Episode>) subObj);
            }

            Object dubObj = map.get("dub");
            if (dubObj instanceof List<?>) {
                all.addAll((List<Episode>) dubObj);
            }

            // Nếu provider dùng các key số hoặc khác để chứa danh sách, flatten thêm
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                Object val = entry.getValue();
                if (val instanceof List<?> && !"sub".equals(key) && !"dub".equals(key)) {
                    all.addAll((List<Episode>) val);
                }
            }
            return all;
        }

        // Fallback: cố gắng convert bằng ObjectMapper
        try {
            return objectMapper.convertValue(eps, new TypeReference<List<Episode>>() {});
        } catch (Exception ex) {
            return Collections.emptyList();
        }
    }


    private <T> List<T> parseJsonToList(String json, Class<T> clazz) {
        if (json == null || json.isBlank()) return List.of();
        try {
            return objectMapper.readValue(json, objectMapper.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            // wrap to keep behavior consistent
            throw new EpisodeFetchException("Parse JSON failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean hasEpisodes(Provider provider) {
        if (provider == null) return false;
        Object eps = provider.getEpisodes();
        if (eps == null) return false;

        if (eps instanceof List<?>) {
            List<?> list = (List<?>) eps;
            return !list.isEmpty();
        }

        if (eps instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) eps;
            Object subObj = map.get("sub");
            Object dubObj = map.get("dub");

            if (subObj instanceof List<?> && !((List<?>) subObj).isEmpty()) return true;
            if (dubObj instanceof List<?> && !((List<?>) dubObj).isEmpty()) return true;

            // kiểm tra các entry khác chứa List
            return map.values().stream().anyMatch(v -> (v instanceof List<?>) && !((List<?>) v).isEmpty());
        }

        return false;
    }
}
