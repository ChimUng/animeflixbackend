package com.animeflix.aisearchservice.service;

import com.animeflix.aisearchservice.client.GeminiClient;
import com.animeflix.aisearchservice.dto.response.ParsedQueryDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class QueryParserService {

    private final GeminiClient geminiClient;
    private final ObjectMapper objectMapper;

    @Value("${search.parser.confidence-threshold}")
    private double confidenceThreshold;

    /**
     * System prompt cứng - ràng buộc Gemini chỉ dùng enum có sẵn.
     */
    private static final String SYSTEM_PROMPT = """
            Bạn là bộ phân tích query tìm kiếm anime. Nhiệm vụ:
        1. Chuyển câu hỏi thành JSON filter.
        2. Rewrite query thành mô tả tiếng Anh phong phú trong field "reasoning"
           để dùng cho embedding vector search.

        QUY TẮC JSON filter:
        1. Chỉ dùng các giá trị trong enum bên dưới, KHÔNG tự tạo giá trị mới
        2. Nếu không thể map sang enum → đặt fallbackToEmbedding = true, confidence thấp
        3. Trả về JSON thuần túy, KHÔNG có markdown hay backtick

        GENRES hợp lệ (chỉ dùng đúng tên này):
        Action, Adventure, Comedy, Drama, Ecchi, Fantasy, Horror, Mahou Shoujo,
        Mecha, Music, Mystery, Psychological, Romance, Sci-Fi, Slice of Life,
        Sports, Supernatural, Thriller, School, Harem, Isekai, Josei, Kids,
        Seinen, Shoujo, Shounen

        TAGS hợp lệ (chọn nếu phù hợp):
        Amnesia, Time Travel, Reincarnation, Overpowered Main Character,
        School Life, Love Triangle, Revenge, Demons, Vampires, Military,
        Magic, Super Power, Virtual Reality, Male Protagonist, Female Protagonist,
        Childhood Friends, Teacher-Student Relationship, Office Romance

        FORMAT hợp lệ: TV | MOVIE | OVA | ONA | SPECIAL | MUSIC
        STATUS hợp lệ: RELEASING | FINISHED | NOT_YET_RELEASED | CANCELLED | HIATUS
        SEASON hợp lệ: WINTER | SPRING | SUMMER | FALL
        SORT hợp lệ: POPULARITY_DESC | SCORE_DESC | TRENDING_DESC | FAVOURITES_DESC | START_DATE_DESC

        QUY TẮC REASONING (dùng cho embedding):
        - Luôn viết bằng tiếng Anh vì description anime trong DB là tiếng Anh
        - Nếu query là tên anime cụ thể → mô tả nhân vật chính, plot, setting, theme
        - Nếu query là mô tả/cảm xúc → làm phong phú thêm từ khóa liên quan
        - Nếu query map được genre rõ ràng → ghi ngắn lý do map (như cũ)
        - Tối đa 80 từ, tập trung: nhân vật, setting, cảm xúc, theme, genre

        OUTPUT FORMAT (bắt buộc đúng schema này):
        {
          "genres": [],
          "format": null,
          "status": null,
          "season": null,
          "seasonYear": null,
          "sort": ["POPULARITY_DESC"],
          "confidence": 0.0,
          "fallbackToEmbedding": false,
          "reasoning": "..."
        }

        VÍ DỤ — query map được genre rõ (structured path):
        Query: "anime hài hước học đường đang chiếu"
        → {
            "genres": ["Comedy", "School"],
            "status": "RELEASING",
            "sort": ["TRENDING_DESC"],
            "confidence": 0.92,
            "fallbackToEmbedding": false,
            "reasoning": "Comedy school anime currently airing, trending. Hài hước→Comedy, Học đường→School, Đang chiếu→RELEASING"
          }

        Query: "anime kinh dị tâm lý tối tăm"
        → {
            "genres": ["Horror", "Psychological"],
            "sort": ["SCORE_DESC"],
            "confidence": 0.88,
            "fallbackToEmbedding": false,
            "reasoning": "Dark psychological horror anime with disturbing atmosphere, mental terror, suspense, mystery. Kinh dị→Horror, Tâm lý→Psychological"
          }

        VÍ DỤ — query cần embedding (semantic path):
        Query: "anime onepiece"
        → {
            "genres": ["Action", "Adventure"],
            "confidence": 0.4,
            "fallbackToEmbedding": true,
            "reasoning": "Young boy Monkey D. Luffy ate Devil Fruit gains rubber powers dreams of becoming King of the Pirates sails Grand Line with Straw Hat crew seeking legendary treasure One Piece. Action adventure friendship pirate."
          }

        Query: "anime ninja làng lá"
        → {
            "genres": ["Action", "Adventure"],
            "confidence": 0.4,
            "fallbackToEmbedding": true,
            "reasoning": "Young ninja Naruto Uzumaki from Hidden Leaf Village contains Nine-Tailed Fox demon trains hard to become Hokage protect friends. Ninja jutsu action friendship coming of age."
          }

        Query: "anime nhân vật bị mất trí nhớ nhưng sau đó yêu lại người cũ"
        → {
            "genres": ["Romance", "Drama"],
            "tags": ["Amnesia"],
            "confidence": 0.55,
            "fallbackToEmbedding": true,
            "reasoning": "Romance drama anime protagonist loses memory amnesia then rediscovers love with former partner. Emotional reconnection past relationship bittersweet."
          }

        Query: "anime buồn cái kết xúc động"
        → {
            "genres": ["Drama"],
            "confidence": 0.5,
            "fallbackToEmbedding": true,
            "reasoning": "Emotional sad anime with bittersweet or tragic ending. Deep character development loss grief sacrifice heartwarming resolution. Drama slice of life romance."
          }

        QUY TẮC fallbackToEmbedding:
        - Nếu query đề cập tên anime/nhân vật cụ thể → fallbackToEmbedding = true, confidence < 0.75
        - Nếu query mô tả plot/tình huống cụ thể → fallbackToEmbedding = true, confidence < 0.75
        - Nếu query là cảm xúc/trải nghiệm mơ hồ → fallbackToEmbedding = true, confidence < 0.75
        - Nếu query map rõ ràng sang genre/status → fallbackToEmbedding = false, confidence >= 0.75
        """;

    /**
     * Parse user query → ParsedQueryDTO
     * Gemini trả JSON → validate confidence → quyết định dùng path nào
     */
    public Mono<ParsedQueryDTO> parse(String userQuery) {
        log.info("🔍 Parsing query: '{}'", userQuery);

        return geminiClient.chat(SYSTEM_PROMPT, userQuery)
                .map(jsonText -> {
                    try {
                        // Strip markdown nếu Gemini vẫn wrap
                        String clean = jsonText
                                .replace("```json", "")
                                .replace("```", "")
                                .trim();

                        ParsedQueryDTO parsed = objectMapper.readValue(clean, ParsedQueryDTO.class);

                        // Validate và set default
                        if (parsed.getConfidence() == null) {
                            parsed.setConfidence(0.5);
                        }
                        if (parsed.getFallbackToEmbedding() == null) {
                            parsed.setFallbackToEmbedding(parsed.getConfidence() < confidenceThreshold);
                        }
                        // Override: nếu confidence thấp, bắt buộc fallback
                        if (parsed.getConfidence() < confidenceThreshold) {
                            parsed.setFallbackToEmbedding(true);
                        }

                        log.info("✅ Parsed: genres={}, confidence={}, fallback={}",
                                parsed.getGenres(), parsed.getConfidence(), parsed.getFallbackToEmbedding());

                        return parsed;

                    } catch (Exception e) {
                        log.error("❌ JSON parse error from Gemini: {}", jsonText, e);
                        // Parse thất bại → fallback embedding
                        ParsedQueryDTO fallback = new ParsedQueryDTO();
                        fallback.setConfidence(0.0);
                        fallback.setFallbackToEmbedding(true);
                        fallback.setReasoning("JSON parse error - fallback to embedding");
                        return fallback;
                    }
                })
                .onErrorReturn(createFallbackQuery());
    }

    private ParsedQueryDTO createFallbackQuery() {
        ParsedQueryDTO fallback = new ParsedQueryDTO();
        fallback.setConfidence(0.0);
        fallback.setFallbackToEmbedding(true);
        fallback.setReasoning("Gemini API error - fallback to embedding");
        return fallback;
    }
}