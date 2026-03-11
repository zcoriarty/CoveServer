# CoveServer

A private, Instagram-style media-sharing server built in Rust with gRPC. Designed for self-hosted deployment on infrastructure like Mac minis, serving a known group of friends and family with security and privacy as top priorities.

## Architecture

CoveServer is a **modular monolith** with clearly separated domain modules, backed by:

- **Rust** — API server and background workers
- **gRPC + Protobuf** — client-server communication (13 service definitions)
- **PostgreSQL** — relational data (users, posts, follows, comments, likes, notifications, feed)
- **Redis** — feed caching, rate limiting, ephemeral state
- **S3-compatible object storage** (MinIO) — encrypted media storage
- **Docker Compose** — single-host deployment with Traefik for TLS termination

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
├── docker-compose.yml   # Full deployment stack
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
# 1. Create secrets directory with a master encryption key
mkdir -p secrets
openssl rand 32 > secrets/master.key

# 2. Copy and configure environment
cp .env.example .env
# Edit .env with production-strength secrets

# 3. Start everything
docker compose up -d

# 4. The gRPC API is available on port 443 (TLS via Traefik)
#    Prometheus metrics on port 9090
```

## Configuration

All settings can be configured via:
- **Environment variables** with `COVE_` prefix (e.g., `COVE_DATABASE_URL`)
- **TOML config file** at `config/cove.toml` or path in `COVE_CONFIG`

Environment variables take precedence over the config file.

## Development

Requires:
- Rust 1.82+
- `protoc` (Protocol Buffers compiler)
- PostgreSQL 16+
- Redis 7+
- S3-compatible storage (MinIO)

```bash
# Build
cargo build --workspace

# Run the server (with local services running)
cargo run --bin cove-server

# Run the worker
cargo run --bin cove-worker
```

## License

GPL-3.0
