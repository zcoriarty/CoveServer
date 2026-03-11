# CoveServer Implementation Guide

## 1. Overview

**Cove** is a private, Instagram-style media-sharing application for personal use by friends and family. The server for the system, **CoveServer**, should be designed with three priorities in this order:

1. **Security and privacy**
2. **Performance and responsiveness**
3. **Operational simplicity and maintainability**

CoveServer should support the core feature set expected from an Instagram-like application while remaining small enough to operate safely on self-hosted infrastructure such as Mac minis.

This guide focuses only on the **server side**. It does not cover the iOS client beyond server-facing implications.

---

## 2. Goals

CoveServer should support the following product requirements:

- Home feed
- Profiles
- Following and followers
- Posts with photos in multiple aspect ratios
- Video posts
- Comments
- Likes
- Sharing
- Search
- Notifications

It should also meet the following technical requirements:

- Written in **Rust**
- Use **gRPC** for server-client communication
- Encrypt all data **in transit** and **at rest**
- Avoid third-party ownership of user media
- Avoid external analytics and ad-tech systems
- Deliver fast enough feed and media performance for daily use

End-to-end encryption is intentionally **out of scope for v1** in order to keep the system practical and maintainable.

---

## 3. Non-Goals for v1

The first version of CoveServer should explicitly avoid the following:

- Public-facing social graph or discovery beyond approved users
- Algorithmically heavy ranking or recommendation systems
- Full end-to-end encrypted media sharing
- Multi-region deployment
- Complex microservice sprawl
- Third-party CDN dependence unless later required
- General-purpose file hosting beyond app media needs

The goal is to build a secure, private, high-quality system for a known user group, not a public internet platform.

---

## 4. Design Principles

### 4.1 Privacy-first by default

- Private accounts and private content should be the default.
- Collect the minimum metadata needed to operate the system.
- Strip unnecessary EXIF and location metadata from uploaded media by default.
- Avoid third-party processors for analytics, search, observability, or media handling unless absolutely necessary.

### 4.2 Security through layered controls

- Use TLS for all network communication.
- Encrypt disks, databases, object storage, and backups.
- Separate secrets and keys from the application code and stored data.
- Build strong authentication, authorization, and auditability into the core architecture.

### 4.3 Fast reads, controlled writes

Instagram-style products are read-heavy. CoveServer should be designed so that:

- Feed reads are fast
- Profile reads are fast
- Media delivery is efficient
- Expensive work happens asynchronously in background jobs

### 4.4 Simplicity over novelty

Prefer proven infrastructure and operational patterns over custom protocols, custom cryptography, or unnecessary service decomposition.

---

## 5. Recommended High-Level Architecture

CoveServer should be built as a **modular monolith** in Rust for v1, with a few clearly separated infrastructure components.

### 5.1 Core components

1. **gRPC API service (Rust)**
   - Primary application server
   - Handles auth, profiles, follows, posts, feed reads, comments, likes, shares, search requests, and notifications API surfaces
   - Performs authorization checks
   - Coordinates with storage, cache, and background workers

2. **PostgreSQL**
   - Source of truth for relational data
   - Stores users, relationships, post metadata, comments, likes, notifications, visibility rules, and feed indices or feed pointers

3. **Object storage**
   - Stores media binaries: originals, thumbnails, resized variants, and transcoded video outputs
   - Should be encrypted at rest
   - Prefer S3-compatible self-hosted storage rather than storing blobs in PostgreSQL

4. **Redis**
   - Used for caching, short-lived state, and queue coordination where appropriate
   - Useful for feed caches, session-related ephemeral state, rate-limit tracking, and notification fanout support

5. **Background worker service (Rust)**
   - Handles asynchronous jobs such as image validation, thumbnail generation, video transcoding, EXIF stripping, search indexing updates, notification fanout, and feed updates

6. **Reverse proxy / ingress**
   - Terminates TLS
   - Enforces request limits and timeouts
   - Routes traffic to gRPC services
   - Can also serve or proxy media where appropriate

### 5.2 Deployment shape

For v1, CoveServer can run on a **single self-hosted Mac mini** using **Docker Compose**.

Recommended containers:

- **Reverse proxy / ingress**: terminates TLS and forwards gRPC traffic
- **CoveServer API**: main Rust gRPC application
- **Worker**: asynchronous jobs such as media processing and notification fanout
- **PostgreSQL**: primary relational database
- **Redis**: caching and ephemeral coordination
- **Object storage**: media storage if self-hosted in the same environment

Only the **reverse proxy** should be exposed externally. PostgreSQL, Redis, object storage, and internal application containers should remain on a private Docker network.

In this setup, external clients connect to the **Mac mini’s public IP or domain**, and Docker routes traffic from the exposed reverse proxy port to the correct internal service.

This is a practical and simple starting point, even though Docker on macOS runs inside a Linux VM layer. It is suitable for Cove’s initial scale and can later be migrated to dedicated Linux infrastructure if needed.

---

## 6. Why a Modular Monolith Is the Right Starting Point

A modular monolith is the best fit for CoveServer because:

- The user base is relatively small
- Operational simplicity matters
- Security is easier to reason about in one deployable application boundary
- gRPC interfaces can still be structured cleanly without forcing independent deployables
- Future service extraction remains possible if a real scaling or isolation need emerges

Internally, the application should still be broken into clean modules with strong boundaries.

---

## 7. Rust Architecture Guidance

Rust is a good fit for CoveServer because it supports:

- predictable performance
- memory safety
- strong type modeling
- safer concurrency
- lower runtime overhead compared with many garbage-collected alternatives

The value of Rust here is not only raw speed. It is also the ability to make dangerous states harder to represent in code.

### 7.1 Suggested project layout

The Rust codebase should be structured by domain rather than by technical layer alone.

Recommended top-level modules:

- `auth`
- `users`
- `profiles`
- `social_graph`
- `posts`
- `media`
- `feed`
- `comments`
- `likes`
- `sharing`
- `search`
- `notifications`
- `storage`
- `crypto`
- `jobs`
- `admin`
- `audit`
- `common`

Each module should contain:

- domain models
- service logic
- repository interfaces
- validation rules
- authorization helpers
- gRPC handler adapters where needed

### 7.2 Layering model

A strong internal layering model should look like this:

- **Transport layer**: gRPC handlers, request decoding, response encoding, error mapping
- **Application layer**: use cases and orchestration logic
- **Domain layer**: business rules, domain entities, access control rules
- **Infrastructure layer**: PostgreSQL, Redis, object storage, queues, metrics, filesystem, external process invocation

The transport layer should be thin. Business logic should not live directly inside gRPC handlers.

### 7.3 Best practices for Rust services

- Keep request handlers small and explicit
- Use strongly typed IDs rather than plain strings everywhere
- Separate public IDs from internal database IDs when useful
- Use explicit result/error types with well-defined mapping to gRPC status codes
- Avoid global mutable state
- Prefer dependency injection through constructors and traits/interfaces
- Keep async boundaries explicit and limited to I/O-heavy work
- Isolate unsafe or crash-prone media processing operations

### 7.4 Concurrency and async

Rust async should be used where it helps with I/O concurrency, not as an excuse to make every operation maximally complex.

Use async for:

- database access
- cache access
- object storage access
- gRPC request handling
- background job execution

Be careful with:

- CPU-heavy image/video processing inside async executors
- blocking external tooling invoked from request paths
- overusing shared locks in high-read paths

Heavy media work should be offloaded to worker processes or dedicated execution pools.

---

## 8. gRPC Architecture Guidance

gRPC is a strong fit for CoveServer if the client is controlled and the API contracts are stable.

### 8.1 Why gRPC fits this system

- Efficient binary protocol
- Strong contract definition via protobuf
- Good support for typed APIs and versioning
- Well suited to mobile-app-to-server communication
- Useful for internal service boundaries if parts of the system are later split out

### 8.2 gRPC best practices for CoveServer

- Define protobuf contracts around product domains, not database tables
- Keep APIs coarse enough to avoid excessive chattiness
- Support pagination on all list-returning methods
- Use explicit request and response message types, even for simple calls
- Avoid leaking internal schema design into API contracts
- Version APIs deliberately
- Use server-side deadlines, size limits, and validation consistently

### 8.3 Recommended gRPC service boundaries

Suggested service definitions:

- `AuthService`
- `UserService`
- `ProfileService`
- `FollowService`
- `PostService`
- `FeedService`
- `CommentService`
- `LikeService`
- `ShareService`
- `SearchService`
- `NotificationService`
- `MediaService`
- `AdminService`

These services can all live in one Rust binary at first.

### 8.4 Request design guidance

Avoid designs where the client must make many small round trips to build one screen. For example:

- Feed APIs should return all metadata necessary to render a feed page, including media references and user summary information needed for each entry.
- Profile APIs should return profile summary information plus paginated grid data or reels/video data when requested.
- Notification APIs should support pagination and acknowledgement state changes.

### 8.5 Streaming usage

gRPC streaming may be useful for:

- notification delivery
- large upload coordination workflows
- internal worker coordination in some cases

However, most core mobile product APIs should remain request-response unless streaming clearly simplifies the design.

---

## 9. Data Model Overview

PostgreSQL should hold the source of truth for application data. Media bytes should not live in PostgreSQL.

### 9.1 Main entities

Core entities likely include:

- users
- profiles
- devices or sessions
- follow relationships
- posts
- post media items
- comments
- likes
- shares
- notifications
- feed entries or feed pointers
- search index support tables
- audit events
- moderation/admin actions if ever needed

### 9.2 User and profile model

Separate identity from profile presentation.

**User** data includes:

- account ID
- login identifiers
- password hash or auth credential references
- invite status
- account state
- security preferences

**Profile** data includes:

- display name
- username/handle
- bio
- avatar reference
- privacy settings
- follower and following counts

This separation simplifies future changes to auth and presentation.

### 9.3 Follow graph

A follow relationship should support:

- follower ID
- followee ID
- relationship state
- created timestamp
- approved timestamp for private accounts if approval is required
- soft-delete or block state if needed later

Follower/following counts should be stored in a way that supports fast reads, with consistency maintained transactionally or through background reconciliation.

### 9.4 Post model

A post should include:

- post ID
- author ID
- caption
- visibility or audience settings
- post type
- created timestamp
- edited timestamp if editing is supported
- deletion state
- aggregate counts

A post can reference one or more media items.

### 9.5 Media model

Each media item should include:

- media ID
- owning post ID
- media type (photo or video)
- original object reference
- derived variant references
- width and height
- aspect ratio metadata
- duration for video
- processing state
- hash/checksum

This design supports multiple aspect ratios, carousels if added later, thumbnails, and multiple delivery sizes.

### 9.6 Comments, likes, and shares

Comments should support:

- comment ID
- post ID
- author ID
- parent comment ID if threaded comments are supported
- body
- creation timestamp
- deletion state

Likes should support:

- user ID
- target post ID
- timestamp

Shares should be modeled explicitly depending on product intent. If sharing means in-app reposting or direct send behavior, define separate semantics instead of overloading one table.

### 9.7 Notifications

Notification records should include:

- notification ID
- recipient user ID
- actor user ID if applicable
- type
- target entity references
- creation timestamp
- read timestamp
- delivery state

### 9.8 Search support

Search should likely index:

- usernames
- display names
- profile text
- possibly captions
- possibly hashtags if supported

Search must remain privacy-aware. Results should only include visible and authorized content.

---

## 10. Media Storage and Delivery

Media handling is one of the highest-risk parts of the system.

### 10.1 Storage strategy

Use object storage for media blobs. Do not store media binaries directly in PostgreSQL.

Store the following variants where appropriate:

- original upload
- thumbnail
- feed-sized image/video poster frame
- larger display variant
- transcoded video outputs at approved formats and resolutions

### 10.2 Multiple aspect ratios

CoveServer should support portrait, landscape, and square images. That means the media pipeline must:

- preserve original dimensions
- compute safe display metadata
- generate variants without breaking aspect ratio
- provide crop or fit metadata as needed for client rendering

The server should store canonical dimensions and let the client render appropriately rather than assuming one fixed ratio.

### 10.3 Video handling

Video support requires more care than images.

The pipeline should:

- validate file type and container
- extract safe metadata
- generate poster frames
- transcode into one or more delivery-friendly formats
- constrain maximum duration, bitrate, and file size for v1

Video processing should happen asynchronously through workers, not in the request path.

### 10.4 Secure media handling

Media uploads must be treated as untrusted input.

The system should:

- validate magic bytes and actual file format
- reject unsupported formats
- strip EXIF and unnecessary metadata from images
- generate randomized internal object keys
- avoid serving directly from writable upload locations
- isolate processing tools and workers from the main API path

### 10.5 Media access pattern

Media access should be authorized through the application, then delivered through short-lived access mechanisms.

A practical approach is:

1. Client requests feed or post details
2. Server verifies authorization
3. Server returns media references scoped for access
4. Client fetches authorized media variant

Whether this is implemented through signed object URLs, proxy delivery, or an authenticated media gateway depends on operational preference. For privacy and simplicity, a media gateway controlled by CoveServer is often easier to reason about at first.

---

## 11. Feed Design

Feed performance is one of the most important user experience requirements.

### 11.1 Recommended feed strategy

For CoveServer, use a **chronological home feed** in v1.

This choice is recommended because it is:

- predictable
- privacy-friendly
- simpler to implement
- easier to cache
- easier to debug

### 11.2 Feed data model

The feed should operate on lightweight metadata, not raw media.

Each feed entry should include enough data to render:

- post summary
- author summary
- media variant references
- counts and user-specific flags such as liked state
- timestamps

### 11.3 Fanout strategy

For a small user base, a simple feed fanout approach is reasonable.

When a user posts:

- insert the post
- create feed entries or feed references for followers who are allowed to see it
- cache relevant first-page views

This can be done synchronously for small graphs or delegated to background jobs for cleaner request latency.

### 11.4 Feed read path

A feed request should:

1. Authenticate user
2. Resolve feed page from cache if present
3. Fall back to feed entry retrieval from PostgreSQL if needed
4. Fetch associated post and profile summaries efficiently
5. Return compact feed-ready response objects

Avoid generating the home feed from expensive live joins across the entire social graph for every request.

### 11.5 Caching

Cache the most common and expensive read results:

- first page of home feed
- profile post grids
- frequently accessed post summaries
- counters that do not require strict real-time accuracy

Cache invalidation should be driven by post creation, deletes, likes, comments, and follow graph changes.

---

## 12. Search Design

Search should be useful but narrow in scope for v1.

### 12.1 Recommended search scope

Start with:

- username search
- display name search
- profile text search
- optional caption search if it remains performant and privacy-safe

### 12.2 Search implementation options

Initial search can likely be handled through PostgreSQL capabilities and carefully indexed queries.

If search needs grow later, search-specific infrastructure can be added. For v1, avoid introducing a separate search cluster unless real performance demands it.

### 12.3 Privacy-aware search

Search must respect:

- private accounts
- blocked relationships if later supported
- post visibility rules
- limited metadata exposure

The search system should never leak existence of unauthorized users or content more than the product intentionally allows.

---

## 13. Notifications Design

Notifications should be event-driven and asynchronous.

### 13.1 Notification events

CoveServer should support notifications for at least:

- follow requests or follow approvals if relevant
- new followers
- likes
- comments
- shares if product-defined
- possibly new posts from followed users depending on product choice

### 13.2 Delivery design

The server should:

- store notifications in PostgreSQL as durable records
- fan out notifications asynchronously
- expose paginated notification APIs
- support read and unread state transitions

Push delivery integration can be added later, but the internal notification model should exist from the beginning.

### 13.3 Avoiding noise

Batching or aggregation logic may be useful later, but v1 can begin with one notification per event as long as rate and spam controls are considered.

---

## 14. Authentication, Authorization, and Access Control

### 14.1 Authentication model

Because Cove is a private app, the system should be invite-only.

Authentication should support:

- password-based login with strong password hashing, or passkeys if introduced later
- session issuance and refresh flow
- device/session revocation
- optional multi-factor authentication for admins

### 14.2 Password handling

If passwords are used, they should be:

- hashed using a modern password hashing algorithm such as Argon2id
- uniquely salted
- protected with careful secret handling around any server-side pepper or secret material

### 14.3 Session model

Sessions should support:

- short-lived access credentials
- refresh credentials with rotation
- revocation on suspicious reuse or logout
- device awareness where useful

### 14.4 Authorization model

Authorization should be explicit and central.

Each operation should validate that the acting user is allowed to:

- view a post
- fetch a media item
- comment on a post
- like a post
- follow another account
- search for users or content
- view a profile

Authorization logic should not be duplicated ad hoc across handlers. It should live in reusable policy logic.

### 14.5 Privacy defaults

Recommended defaults:

- accounts are private by default or at least have strong visibility controls
- new posts default to a safe visibility mode
- search exposure is limited
- profile and follower data are only visible as intended by privacy settings

---

## 15. Encryption and Key Management

This is one of the most important parts of CoveServer.

### 15.1 Data in transit

All network communication should be encrypted.

Requirements:

- TLS everywhere
- no plaintext internal traffic if services are separated
- modern TLS configuration
- certificate rotation procedures
- strict ingress configuration

### 15.2 Data at rest

At-rest protection should exist at multiple layers:

- full-disk encryption on Mac minis
- encrypted database volumes
- encrypted object storage volumes
- encrypted backups

### 15.3 Application-level encryption

The most sensitive stored values should also be encrypted or protected at the application layer where appropriate, such as:

- refresh/session secrets
- invite secrets
- recovery codes
- particularly sensitive user-identifying fields if desired

### 15.4 Media encryption approach

For media objects, a practical approach is envelope encryption:

- media encrypted with a per-object or per-batch data key
- data key wrapped by a master key
- master key stored separately from media data

This improves blast-radius control and supports safer key rotation over time.

### 15.5 Key management principles

CoveServer should define clearly:

- where master keys live
- how services obtain runtime secrets
- how key rotation works
- how backups remain decryptable
- who can recover root key material
- how privileged key events are audited

Do not hardcode secrets or keep all critical secrets only on the machines that hold the data.

---

## 16. Background Jobs and Asynchronous Processing

A Rust worker service should handle asynchronous work.

### 16.1 Jobs suitable for async execution

- image validation and transformation
- EXIF stripping
- thumbnail generation
- video transcoding
- poster frame generation
- feed fanout updates
- notification fanout
- search index updates
- cleanup tasks
- reconciliation and consistency checks

### 16.2 Job system guidance

The job system should support:

- idempotent job handlers
- retries with backoff
- dead-letter handling or failure visibility
- observability for stuck or failed jobs
- safe deduplication where needed

### 16.3 Reliability concerns

Workers must be able to restart safely without corrupting data or duplicating side effects. Design all async jobs with explicit idempotency and state transitions.

---

## 17. Database and Persistence Guidance

### 17.1 PostgreSQL as system of record

PostgreSQL should remain the source of truth for:

- user accounts
- privacy settings
- follows
- posts and captions
- comments
- likes
- notifications
- media metadata
- feed references
- audit logs

### 17.2 Indexing priorities

Careful indexing will matter more than language-level optimization for many operations.

Priority query patterns likely include:

- user lookup by handle or ID
- profile lookup
- follow/follower lookups
- feed entry retrieval by user and timestamp
- comment retrieval by post and timestamp
- notification retrieval by recipient and timestamp
- search by username/display name

### 17.3 Transactions

Use transactions for correctness around:

- post creation and media attachment metadata
- follow request acceptance
- like state changes and count updates
- comment creation and count updates
- notification creation tied to events

Do not overuse transactions for long-running operations involving media processing.

---

## 18. Caching Strategy

Redis should be used conservatively and intentionally.

### 18.1 Good cache candidates

- first feed page for a user
- profile summary
- profile grid page
- user summary cards
- short-lived authorization-derived media access state
- rate-limit counters

### 18.2 What not to cache as source of truth

- core relational data
- authoritative privacy rules
- anything that would create correctness issues if stale beyond tolerance

### 18.3 Cache invalidation triggers

Cache invalidation should occur on:

- new post
- delete or archive of post
- comment add/delete
- like/unlike
- follow/unfollow
- profile update
- visibility change

Keep invalidation paths explicit and test them.

---

## 19. Observability and Operations

A private app still needs production-grade operational discipline.

### 19.1 Logging

Use structured logs.

Logs should include:

- request identifiers
- actor IDs where safe
- operation type
- timing
- errors
- job lifecycle events

Avoid logging sensitive content, raw secrets, or unnecessary personal data.

### 19.2 Metrics

Track at minimum:

- request latency by endpoint/service
- error rates
- database latency
- cache hit rate
- job queue depth
- media processing time
- storage usage
- backup success/failure
- certificate expiration windows

### 19.3 Health checks

Expose health and readiness signals for:

- gRPC API
- PostgreSQL connectivity
- Redis connectivity
- object storage availability
- worker liveness

### 19.4 Alerts

Alert on:

- backup failures
- replication lag if replication is added
- storage pressure
- repeated job failures
- elevated auth failures
- certificate renewal problems
- sustained latency spikes

---

## 20. Backup and Disaster Recovery

Self-hosted private systems often fail here, so this must be part of the initial design.

### 20.1 Backup requirements

CoveServer should have:

- encrypted database backups
- encrypted media backups or object replication
- off-device backup copies
- documented restore procedures
- regular restore testing

### 20.2 Recovery goals

The team operating CoveServer should know:

- how to restore PostgreSQL
- how to restore media storage
- how to restore keys and secrets safely
- how to rebuild cache and derived indices
- how to recover from worker corruption or queue loss

### 20.3 Backup isolation

Backups should not rely solely on the same host or same room as production systems.

---

## 21. Security Hardening Checklist

CoveServer should include the following baseline hardening measures:

- invite-only account creation
- strong password hashing
- admin MFA
- TLS everywhere
- full-disk encryption on every host
- encrypted backups
- strict file type validation for uploads
- EXIF stripping by default
- short-lived media access scopes
- rate limiting for auth and upload endpoints
- minimal secret exposure in runtime environments
- audit logging for privileged actions
- regular dependency and OS patching
- firewalling and minimal public exposure

---

## 22. API and Domain Suggestions by Feature

### 22.1 Feed

The feed service should provide:

- paginated home feed retrieval
- efficient post summary hydration
- user-specific state such as liked/saved if added later

The server should optimize the first page heavily.

### 22.2 Profiles

The profile service should provide:

- public-safe profile summary
- privacy-aware profile retrieval
- profile grid retrieval
- follower/following lists where permitted

### 22.3 Following and followers

The follow service should provide:

- follow request
- follow acceptance if private accounts require approval
- unfollow
- follower list retrieval
- following list retrieval

### 22.4 Posts

The post service should provide:

- create post
- attach media metadata
- retrieve post details
- delete or archive post
- edit caption if supported

### 22.5 Comments and likes

These services should provide:

- add comment
- list comments
- delete comment where authorized
- like post
- unlike post
- like state retrieval where useful

### 22.6 Sharing

Define sharing semantics clearly. Possibilities include:

- sharing a post with another in-app user
- generating a private share reference for authorized viewers
- repost-like behavior if ever desired

Sharing must remain privacy-aware and should not become a shortcut that bypasses audience controls.

### 22.7 Search

The search service should provide:

- user search
- profile search
- optional post/caption search if included in scope

### 22.8 Notifications

The notification service should provide:

- list notifications
- mark read/unread
- optionally stream or poll for updates

---

## 23. Suggested Implementation Phases

### Phase 1: Secure foundation

- repository setup
- protobuf and gRPC contract design
- auth and session model
- PostgreSQL schema foundations
- object storage integration
- encryption and key management foundations
- basic observability and logging

### Phase 2: Core social product

- user and profile services
- follow graph
- post creation with photos
- feed generation
- comments and likes
- notification records

### Phase 3: Media maturity

- image variants
- EXIF stripping
- video upload and processing
- poster frames and transcoding
- feed/profile caching improvements

### Phase 4: Search and operational hardening

- privacy-aware search
- backup automation
- restore drills
- admin tooling
- reliability improvements and security review

---

## 24. Recommended v1 Technology Direction

A practical v1 stack for CoveServer is:

- **Rust** for API and workers
- **gRPC + protobuf** for transport
- **PostgreSQL** for relational state
- **Redis** for caching and short-lived state
- **S3-compatible object storage** for media
- **Reverse proxy with TLS** for ingress
- **Mac minis with disk encryption enabled** as the initial deployment environment

This stack fits the scale, security goals, and operational realities of the project.

---

## 25. Biggest Risks to Watch

The main technical risks are not likely to be Rust performance.

The real risks are:

- weak key management
- unsafe media processing
- poor backup and restore discipline
- overcomplicated service architecture too early
- accidental metadata overcollection
- slow feed queries caused by poor schema and indexing choices
- chatty gRPC API design that creates poor mobile performance

These should be treated as first-class design concerns from the start.

---

## 26. Final Recommendation

CoveServer should be built as a **security-first modular monolith in Rust**, using **gRPC** for client-server communication, **PostgreSQL** as the system of record, **object storage** for encrypted media, **Redis** for carefully chosen caching needs, and **Rust background workers** for all heavy asynchronous processing.

The right strategy is not to chase maximum architectural sophistication. It is to build a system that is:

- private by default
- secure by design
- fast on the read path
- simple enough to operate reliably
- structured cleanly enough to evolve over time

That will give Cove the best chance of becoming a trustworthy, long-lived private media platform for the people it is meant to serve.

