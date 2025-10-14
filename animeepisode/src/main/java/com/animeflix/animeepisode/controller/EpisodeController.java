package com.animeflix.animeepisode.controller;
import com.animeflix.animeepisode.model.EpisodeResponse;
import com.animeflix.animeepisode.service.EpisodeService;
import org.springframework.web.bind.annotation.*;
import reactor .core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("api/episodes")
@CrossOrigin(origins = "*")
public class EpisodeController {
    private final EpisodeService episodeService;

    public EpisodeController(EpisodeService episodeService) {
        this.episodeService = episodeService;
    }

    /**
     * Lấy danh sách tập phim theo ID anime.
     * Ví dụ: GET /api/episodes/20605?releasing=true&refresh=false
     */
    @GetMapping("/{id}")
    public Mono<EpisodeResponse> getEpisodes(
            @PathVariable String id,
            @RequestParam(defaultValue = "false") boolean releasing,
            @RequestParam(defaultValue = "false") boolean refresh
    ) {
        return episodeService.getEpisodes(id, releasing, refresh);
    }

    /**
     * Endpoint test để ép refetch dữ liệu và làm mới cache.
     * Ví dụ: POST /api/episodes/20605/refresh
     */
    @PostMapping("/{id}/refresh")
    public Mono<EpisodeResponse> refreshEpisodes(@PathVariable String id) {
        // refresh=true ép lấy mới
        return episodeService.getEpisodes(id, false, true);
    }
}
