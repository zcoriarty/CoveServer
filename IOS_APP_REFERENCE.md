# CoveServer Reference for iOS Implementation

A markdown outline of CoveServer architecture and APIs to use when building the iOS app.

---

## 1. Project Structure

```
CoveServer/
├── cove-proto/                 # Protobuf & gRPC codegen
│   ├── proto/cove/
│   │   ├── common.proto        # Pagination, UserSummary, MediaReference, enums
│   │   ├── auth.proto
│   │   ├── user.proto
│   │   ├── profile.proto
│   │   ├── follow.proto
│   │   ├── post.proto
│   │   ├── feed.proto
│   │   ├── comment.proto
│   │   ├── like.proto
│   │   ├── share.proto
│   │   ├── search.proto
│   │   ├── notification.proto
│   │   ├── media.proto
│   │   └── admin.proto
│   └── src/lib.rs
│
├── cove-common/                # Shared types (AuthContext, CoveError, IDs, pagination)
├── cove-server/                # Main gRPC API server
├── cove-worker/                # Background jobs (feed fanout, media processing, notifications)
├── migrations/                 # PostgreSQL schema
├── config/cove.toml            # Default configuration
└── .env.example
```

---

## 2. gRPC API Services & RPCs

All APIs are **gRPC** over HTTP/2. Package: `cove.*`.

| Service | RPCs |
|---------|------|
| **AuthService** | `Register`, `Login`, `RefreshToken`, `Logout`, `RevokeSession`, `ListSessions`, `ValidateInvite` |
| **UserService** | `GetUser`, `UpdateUser`, `ChangePassword`, `DeactivateAccount` |
| **ProfileService** | `GetProfile`, `UpdateProfile`, `GetProfileGrid` |
| **FollowService** | `Follow`, `Unfollow`, `AcceptFollowRequest`, `RejectFollowRequest`, `GetFollowers`, `GetFollowing`, `GetPendingRequests`, `GetFollowStatus` |
| **PostService** | `CreatePost`, `GetPost`, `DeletePost`, `EditCaption` |
| **FeedService** | `GetHomeFeed` |
| **CommentService** | `AddComment`, `ListComments`, `DeleteComment` |
| **LikeService** | `LikePost`, `UnlikePost`, `GetLikeStatus` |
| **ShareService** | `SharePost`, `GetSharedPosts` |
| **SearchService** | `SearchUsers`, `SearchPosts` |
| **NotificationService** | `ListNotifications`, `MarkRead`, `GetUnreadCount` |
| **MediaService** | `UploadMedia` (streaming), `DownloadMedia` (streaming), `GetMediaStatus` |
| **AdminService** | `CreateInvite`, `ListInvites`, `RevokeInvite`, `SuspendUser`, `UnsuspendUser`, `GetSystemHealth`, `GetAuditLog` |

---

## 3. Shared Concepts

### Pagination

- **Request**: `PaginationRequest` with `page_size` (1–50) and `cursor` (empty for first page)
- **Response**: `PaginationResponse` with `next_cursor`, `has_more`, `total_count`
- Use `next_cursor` for the next page; stop when `has_more` is false

### Common Types

| Type | Notes |
|------|-------|
| `UserSummary` | `user_id`, `username`, `display_name`, `avatar_url`, `is_following` |
| `MediaReference` | `media_id`, `media_type`, `url`, `width`, `height`, `aspect_ratio`, `duration_seconds`, `thumbnail_url` |

### Enums

| Enum | Values |
|------|--------|
| `Visibility` | `FOLLOWERS`, `PRIVATE` |
| `MediaType` | `IMAGE`, `VIDEO` |
| `MediaVariant` | `ORIGINAL`, `THUMBNAIL`, `FEED`, `DISPLAY` |
| `FollowState` | `NONE`, `PENDING`, `ACCEPTED`, `BLOCKED` |

---

## 4. Authentication Flow

### Registration (Invite-Only)

1. `AuthService.ValidateInvite(invite_code)` — validate before showing signup form
2. `AuthService.Register(invite_code, username, email, password, display_name)`
3. Store `access_token`, `refresh_token`, `expires_at` locally

### Login

1. `AuthService.Login(username_or_email, password, device_id, device_name)`
2. Store tokens same as registration

### Authenticated Requests

- Attach header: `Authorization: Bearer <access_token>`
- On `UNAUTHENTICATED`, call `RefreshToken` with stored `refresh_token`
- If refresh fails, redirect to login

### Refresh & Logout

- **Refresh**: `AuthService.RefreshToken(refresh_token)` — returns new access + refresh
- **Logout**: `AuthService.Logout(session_id)` and clear local tokens

### Token Details

- Access JWT TTL: 15 min
- Refresh token TTL: 7 days (stored hashed in DB)
- JWT claims: `user_id`, `session_id`, `is_admin`, `exp`, `iat`

---

## 5. Media

### Upload

- `MediaService.UploadMedia(stream)` — send metadata first, then chunks
- Max upload size: 50 MiB
- Max video duration: 60 s
- Allowed types: `image/jpeg`, `image/png`, `image/webp`, `video/mp4`, `video/quicktime`

### Download

- `MediaService.DownloadMedia(media_id, variant)` — streaming response
- Variants: `ORIGINAL`, `THUMBNAIL`, `FEED`, `DISPLAY`
- Use `GetMediaStatus` to poll while `processing_state` ≠ `completed`

---

## 6. Database Models (Reference)

| Table | Key Fields |
|-------|------------|
| `users` | id (UUID v7), username, email, password_hash, display_name, is_admin, account_state |
| `sessions` | id, user_id, refresh_token_hash, device_id, device_name |
| `invites` | code, created_by, max_uses, use_count, expires_at |
| `profiles` | user_id (PK), bio, avatar_media_id, is_private, follower_count, following_count, post_count |
| `follows` | follower_id, followee_id, state (pending/accepted/blocked) |
| `posts` | id, author_id, caption, visibility, post_type (photo/video/carousel), is_deleted |
| `media_items` | id, post_id, media_type, *_key (original/thumbnail/feed/display), processing_state |
| `feed_entries` | user_id, post_id, created_at |
| `comments` | id, post_id, author_id, parent_id, body, reply_count |
| `likes` | user_id, post_id |
| `notifications` | recipient_id, actor_id, notification_type, target_type, target_id, is_read |

---

## 7. Error Handling (gRPC Status Codes)

| Code | Meaning | Suggested iOS Action |
|------|---------|----------------------|
| `UNAUTHENTICATED` | Invalid/expired token | Refresh token or prompt login |
| `PERMISSION_DENIED` | Not allowed | Show permission denied UI |
| `NOT_FOUND` | Resource missing | Show not found / 404 UI |
| `INVALID_ARGUMENT` | Bad request | Show validation errors |
| `UNAVAILABLE` | Server down | Retry with backoff / show offline UI |

---

## 8. Configuration & Environment

| Env Var | Purpose |
|---------|---------|
| `DATABASE_URL` | PostgreSQL connection string |
| `SUPABASE_STORAGE_ENDPOINT` / `SUPABASE_STORAGE_BUCKET` / `SUPABASE_SECRET_KEY` | Supabase Storage access |
| `JWT_SECRET` or `COVE_AUTH__JWT_SECRET` | JWT signing secret |
| `PORT` / `COVE_SERVER__PORT` | Server bind address |
| `REDIS_URL` | Redis for caching/jobs |

iOS app connects to the Railway public domain over HTTPS (HTTP/2).

---

## 9. iOS Integration Checklist

- [x] Add gRPC Swift v2 SPM packages: `grpc-swift-2` (GRPCCore), `grpc-swift-nio-transport` (GRPCNIOTransportHTTP2), `grpc-swift-protobuf` (GRPCProtobuf)
- [ ] Generate Swift gRPC client from proto files using `protoc` + `protoc-gen-swift` + `protoc-gen-grpc-swift`
- [ ] Implement auth flow: ValidateInvite → Register/Login → store tokens
- [ ] Attach `Authorization: Bearer <token>` to all authenticated RPCs
- [ ] Implement token refresh before expiry
- [ ] Use cursor pagination for feeds, comments, followers, etc.
- [ ] Stream media upload/download via MediaService
- [ ] Poll `GetMediaStatus` for processing state when needed
- [ ] Map gRPC status codes to app error states

---

*Generated from CoveServer architecture. See `README.md` and `cove_server_implementation_guide-2.md` for more detail.*
