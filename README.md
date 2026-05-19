<p align="center">
  <div align="center">
    <a href="https://animeflixnow.vercel.app/">
      <img alt="Animeflix" src="https://animeflixnow.vercel.app/icon-192x192.png" width="180"/>
    </a>
  </div>
  <h2 align="center">Animeflix — Backend Microservices</h2>
  <p align="center">
    Java Spring Boot · MongoDB · Redis · Kafka · Gemini AI · Docker
  </p>
</p>

---

## Tổng quan kiến trúc

Animeflix backend được xây dựng theo mô hình **microservice**, giao tiếp qua API Gateway và Kafka message broker.

```
Client (Next.js)
      │
      ▼
 API Gateway :4004          ← Route + Auth filter (X-API-KEY)
      │
      ├── Auth Service :8085       ← JWT, session, API key management
      ├── Anime Catalog :8080      ← Sync AniList, Redis cache, schedule
      ├── Anime Episode :8084      ← Multi-provider episode fetching
      ├── User Service :8081       ← Profile, watchlist, notification
      └── AI Search Service        ← Gemini embed + MongoDB Vector Search
```

**Luồng AI Search:**
```
Query → QueryParserService (Gemini)
           │
     ┌─────┴─────┐
 confidence≥0.75   fallback
     │                │
AniList API      Gemini Embed → MongoDB Vector Search
     │                │
     └──── ReRankService ────┘
                 │
           SearchResponse
```

---

## Yêu cầu môi trường

| Công cụ | Phiên bản |
|---|---|
| Java | 21+ |
| Maven | 3.9+ |
| Docker & Docker Compose | 24+ |
| MongoDB | 7.0 (via Docker) |
| Node.js (optional, cho frontend) | 18+ |

**External services cần có:**
- [Upstash Redis](https://upstash.com/) — free tier đủ dùng
- [Google Gemini API Key](https://aistudio.google.com/app/apikey) — embedding + chat
- MongoDB Atlas hoặc local (cần tạo **Vector Search Index** cho AI Search)

---

## Cài đặt nhanh

### 1. Clone repo

```bash
git clone https://github.com/ChimUng/Animeflix.git
cd Animeflix
```

### 2. Khởi động infrastructure (Kafka + MongoDB + Kafka UI)

```bash
docker-compose up -d
```

Kafka UI sẽ chạy tại `http://localhost:8090`

### 3. Cấu hình biến môi trường

Mỗi service đọc config từ `application.yml` / `application.properties`. Các biến cần override:

**`auth-service`** (`auth-service/src/main/resources/application.yml`)
```yaml
JWT_ACCESS_SECRET: <your-secret>
JWT_REFRESH_SECRET: <your-secret>
REDIS_URL: rediss://default:<password>@<host>:<port>
```

**`anime-catalog-service`** (`anime-catalog-service/src/main/resources/application.yml`)
```yaml
REDIS_URL: rediss://default:<password>@<host>:<port>
# MongoDB mặc định: mongodb://localhost:27017/animeflix_catalog
```

**`ai-search-service`** (`ai-search-service/src/main/resources/application.properties`)
```properties
GEMINI_API_KEY=<your-gemini-api-key>
spring.data.mongodb.uri=mongodb://localhost:27017/animeflix_vector
search.embedding.vector-index-name=anime_vector_index
search.embedding.num-candidates=100
search.embedding.limit=20
search.embedding.similarity-threshold=0.75
search.embedding.score-gap-threshold=0.035
search.embedding.max-results=10
search.embedding.min-results=1
search.parser.confidence-threshold=0.75
```

**`animeepisode`** (`animeepisode/src/main/resources/application.properties`)
```properties
REDIS_URL=rediss://default:<password>@<host>:<port>
# Các provider URI giữ mặc định hoặc tự host
```

### 4. Chạy từng service

Mở terminal riêng cho từng service:

```bash
# Auth Service
cd auth-service && mvn spring-boot:run

# Anime Catalog Service
cd anime-catalog-service && mvn spring-boot:run

# Anime Episode Service
cd animeepisode && mvn spring-boot:run

# User Service
cd user-service && mvn spring-boot:run

# AI Search Service
cd ai-search-service && mvn spring-boot:run

# API Gateway (chạy sau cùng)
cd api-gateway && mvn spring-boot:run
```

---

## Thiết lập MongoDB Vector Search Index

AI Search Service dùng MongoDB Atlas Vector Search. Sau khi có data trong collection `anime_vectors`, tạo index sau trên Atlas UI hoặc CLI:

```json
{
  "name": "anime_vector_index",
  "type": "vectorSearch",
  "definition": {
    "fields": [
      {
        "type": "vector",
        "path": "descriptionVector",
        "numDimensions": 768,
        "similarity": "cosine"
      }
    ]
  }
}
```

> Nếu dùng MongoDB local, cần MongoDB 7.0+ và cấu hình Atlas-compatible hoặc dùng Atlas free tier.

---

## API Reference

### Authentication (qua API Gateway)

Tất cả request tới các service (trừ `/api/auth/**`) phải có header:

```
X-API-KEY: api_<your-key>
```

Lấy API key bằng cách đăng ký developer account:

```bash
POST http://localhost:8085/api/auth/dev/register
Content-Type: application/json

{
  "appId": "my-app",
  "email": "dev@example.com",
  "username": "mydev",
  "password": "password123"
}
```

---

### Auth Service — `localhost:8085`

| Method | Endpoint | Mô tả |
|---|---|---|
| POST | `/api/auth/signup` | Đăng ký user |
| POST | `/api/auth/login` | Đăng nhập, nhận JWT |
| POST | `/api/auth/refresh` | Refresh access token |
| POST | `/api/auth/logout` | Thu hồi refresh token |
| POST | `/api/auth/dev/register` | Đăng ký developer (nhận API key) |
| POST | `/api/auth/dev/login` | Đăng nhập developer |

---

### Anime Catalog Service — `localhost:8080`

Tự động sync dữ liệu từ AniList khi khởi động và mỗi 6 tiếng.

| Method | Endpoint | Mô tả |
|---|---|---|
| GET | `/{id}` | Chi tiết anime (lazy load nếu thiếu) |
| GET | `/popular?page=1&perPage=20` | Anime phổ biến |
| GET | `/trending?page=1&perPage=20` | Anime trending |
| GET | `/movies?page=1&perPage=20` | Anime movie |
| GET | `/season/current?page=1&perPage=20` | Anime mùa hiện tại |
| GET | `/season/next?page=1&perPage=20` | Anime mùa sau |
| GET | `/top100?page=1&perPage=20` | Top 100 |
| GET | `/schedule` | Lịch phát sóng 7 ngày tới |
| GET | `/search?search=naruto&genre=Action&format=TV` | Tìm kiếm nâng cao |

---

### Anime Episode Service — `localhost:8084`

| Method | Endpoint | Mô tả |
|---|---|---|
| GET | `/episodes/{animeId}?releasing=true` | Lấy tập phim từ nhiều provider |
| GET | `/episodes/{animeId}?refresh=true` | Force refresh cache |

Hỗ trợ provider: **Zoro/HiAnime**, **Gogoanime**, **AnimePahe**, **9anime**, **Consumet**, **Anify**

---

### AI Search Service

| Method | Endpoint | Mô tả |
|---|---|---|
| POST | `/search` | AI-powered anime search |

```bash
POST http://localhost:<ai-search-port>/search
Content-Type: application/json

{
  "query": "anime ninja làng lá",
  "userId": "user123",
  "page": 1,
  "perPage": 10
}
```

**Response:**
```json
{
  "results": [...],
  "totalCount": 5,
  "page": 1,
  "perPage": 10,
  "searchType": "SEMANTIC",
  "parsedQuery": {
    "genres": ["Action", "Adventure"],
    "confidence": 0.4,
    "fallbackToEmbedding": true,
    "reasoning": "Young ninja Naruto..."
  }
}
```

**Hai search path:**
- **STRUCTURED** (confidence ≥ 0.75): Query được map sang genre/filter rõ ràng → gọi AniList API
- **SEMANTIC** (confidence < 0.75): Gemini embed query → MongoDB Vector Search → ReRank

---

## Luồng dữ liệu chính

### Data sync (Anime Catalog)
```
App startup / cron 6h
  → AniList GraphQL API
  → Merge với data cũ (giữ Characters, Relations nếu đã có)
  → Save MongoDB
  → Publish Kafka event → AI Search Service embeds → MongoDB Vector
```

### Episode fetch
```
Request /episodes/{id}
  → Check Redis cache
  → Cache miss → MalSync (lấy provider IDs)
  → Parallel fetch: Zoro + Gogoanime + AnimePahe + ...
  → Merge với AniZip metadata (title, thumbnail)
  → Cache Redis (3h nếu đang chiếu, 45 ngày nếu đã kết thúc)
```

---

## Kafka Topics

| Topic | Producer | Consumer | Mô tả |
|---|---|---|---|
| `anime.updated` | Anime Catalog | AI Search | Trigger re-embed khi anime update |
| `anime.episode.new` | Anime Catalog | User Service | Thông báo tập mới cho user đang follow |

---

## Kiểm tra hoạt động

```bash
# 1. Kiểm tra Gateway
curl http://localhost:4004/actuator/health

# 2. Test không có API key (phải trả 401)
curl http://localhost:4004/anime/popular

# 3. Test với API key
curl -H "X-API-KEY: api_yourkey" http://localhost:4004/anime/popular?page=1&perPage=5

# 4. Test AI Search
curl -X POST http://localhost:<ai-port>/search \
  -H "Content-Type: application/json" \
  -d '{"query":"anime buồn cái kết xúc động","page":1,"perPage":5}'
```

---

## Cấu trúc thư mục

```
Animeflix/
├── api-gateway/              # Spring Cloud Gateway, AuthFilter
├── auth-service/             # JWT, session, API key, rate limit
├── anime-catalog-service/    # AniList sync, Redis cache, schedule
├── animeepisode/             # Multi-provider episode aggregator
├── user-service/             # Profile, watchlist, Kafka consumer
├── ai-search-service/        # Gemini embed, vector search, rerank
└── docker-compose.yml        # Kafka + Zookeeper + MongoDB + Kafka UI
```

---

## Lưu ý khi phát triển

- Service `anime-catalog` sync AniList **ngay khi khởi động** — lần đầu chạy sẽ tốn vài giây.
- AI Search cần data đã được embed trước. Chạy batch embed nếu cần hoặc để Kafka consumer tự embed khi có event.
- Rate limit mặc định theo `Developer.rateLimit` trong DB (đơn vị: req/hour).
- Các secret trong `application.yml` chỉ là **default dev values** — thay bằng biến môi trường khi deploy production.

---

## License

MIT License — see [LICENSE](LICENSE) for details.

<h3 align="center">Leave a ⭐ if this project helps you!</h3>
