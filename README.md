# CoveServer

A private, Instagram-style media-sharing server built in Rust with gRPC. Deployment target is Railway, with Supabase for PostgreSQL and object storage.

## Architecture

CoveServer is a **modular monolith** with clearly separated domain modules, backed by:

- **Rust** — API server and background workers
- **gRPC + Protobuf** — client-server communication (13 service definitions)
- **Supabase PostgreSQL** — relational data (users, posts, follows, comments, likes, notifications, feed)
- **Redis** — feed caching, rate limiting, ephemeral state
- **Supabase Storage** — media object storage
- **Railway + Docker** — container deployment

## Project Structure

```
CoveServer/
├── cove-proto/          # Protobuf definitions and generated gRPC code
│   └── proto/cove/      # 14 .proto files defining the full API surface
├── cove-common/         # Shared types: strongly-typed IDs, errors, auth context
├── cove-server/         # Main gRPC API server
│   └── src/
│       ├── auth/        # Authentication (invite-only register, login, JWT sessions)
│       ├── users/       # User account management
│       ├── profiles/    # Profile retrieval and updates
│       ├── social_graph/# Follow/unfollow, pending requests, follower lists
│       ├── posts/       # Post creation, retrieval, deletion, caption editing
│       ├── media/       # Upload initiation, completion, presigned access URLs
│       ├── feed/        # Chronological home feed with Redis caching
│       ├── comments/    # Threaded comments with authorization
│       ├── likes/       # Like/unlike with count management
│       ├── sharing/     # In-app post sharing between users
│       ├── search/      # User and post search via pg_trgm
│       ├── notifications/ # Notification CRUD and unread counts
│       ├── admin/       # Invite management, user moderation, system health
│       ├── audit/       # Audit logging for privileged actions
│       ├── crypto/      # Argon2id passwords, JWT tokens, AES-256-GCM encryption
│       ├── storage/     # Redis cache service, S3 object store helpers
│       ├── jobs/        # Job queue interface (enqueue for background processing)
│       └── config/      # Configuration from env vars (COVE_ prefix) or TOML
├── cove-worker/         # Background worker binary
│   └── src/
│       ├── main.rs      # Job poller with retry/backoff/dead-letter
│       └── handlers.rs  # Feed fanout, media processing, notifications
├── migrations/          # PostgreSQL schema (single migration for v1)
├── config/              # Default configuration (cove.toml)
├── docker-compose.yml   # Local server/worker + Redis stack
└── Dockerfile           # Multi-stage Rust build
```

## gRPC Services

| Service | Description |
|---------|-------------|
| `AuthService` | Register (invite-only), login, token refresh, session management |
| `UserService` | Account retrieval, email updates, password changes, deactivation |
| `ProfileService` | Profile viewing (privacy-aware), updates, grid retrieval |
| `FollowService` | Follow/unfollow, accept/reject requests, follower/following lists |
| `PostService` | Create, read, delete posts; edit captions |
| `FeedService` | Chronological home feed with cursor pagination |
| `CommentService` | Add, list, delete comments (threaded) |
| `LikeService` | Like/unlike posts, status checks |
| `ShareService` | Share posts with other users |
| `SearchService` | Search users and posts (pg_trgm similarity) |
| `NotificationService` | List, mark read, unread counts |
| `MediaService` | Upload initiation, completion, authorized access |
| `AdminService` | Invites, user suspension, system health, audit logs |

## Security

- **Invite-only** registration
- **Argon2id** password hashing with unique salts
- **JWT** access tokens (15-min TTL) with SHA256-hashed refresh tokens (7-day rotation)
- **AES-256-GCM** envelope encryption for media at rest
- **Authorization checks** on every operation (ownership, follow status, visibility)
- **EXIF stripping** during media processing
- **Presigned URLs** for time-limited media access
- **Audit logging** for all privileged admin actions
- **Rate limiting** via Redis
- **Private-by-default** accounts and posts

## Quick Start

```bash
# 1. Copy and configure environment
cp .env.example .env
# Edit .env with Supabase + Redis values

# 2. Start server + worker + Redis locally
docker compose up -d

# 3. The gRPC API is available on port 50051
#    Prometheus metrics on port 9090
```

## Configuration

All settings can be configured via:
- **Environment variables** with `COVE_` prefix (e.g., `COVE_DATABASE__URL`)
- **Railway/Supabase standard envs** (`PORT`, `DATABASE_URL`, `SUPABASE_STORAGE_*`, `SUPABASE_SECRET_KEY`, `REDIS_URL`, `JWT_SECRET`)
- **TOML config file** at `config/cove.toml`

Environment variables take precedence over the config file.

## Development

Requires:
- Rust 1.82+
- `protoc` (Protocol Buffers compiler)
- Redis 7+
- Supabase project (Postgres + Storage)

```bash
# Build
cargo build --workspace

# Run the server (with local services running)
cargo run --bin cove-server

# Run the worker
cargo run --bin cove-worker
```

## Railway Deployment

Deploy as two Railway services from this repo:

1. `cove-server` service with start command `cove-server`
2. `cove-worker` service with start command `cove-worker`

Required environment variables for both services:

- `DATABASE_URL`
- `SUPABASE_STORAGE_ENDPOINT`
- `SUPABASE_STORAGE_BUCKET`
- `SUPABASE_SECRET_KEY`
- `REDIS_URL`
- `JWT_SECRET`

Notes:
- Railway sets `PORT`; the server now binds to that automatically.
- The server runs SQL migrations on startup.
- The Supabase storage bucket is auto-created on startup if missing.

## License

GPL-3.0
