# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
dotnet build Creditbus.Facade.sln

# Run
dotnet run --project Creditbus.Facade/Creditbus.Facade.csproj

# Run tests (once test projects are added)
dotnet test Creditbus.Facade.sln

# Publish
dotnet publish Creditbus.Facade/Creditbus.Facade.csproj -c Release

# Build Docker image (from repo root)
docker build -f Creditbus.Facade/Dockerfile -t creditbus-facade .
```

## Architecture

Single-project .NET 10 Web API targeting Linux containers. Entry point is `Creditbus.Facade/Program.cs` using top-level statements and Minimal API.

The Dockerfile uses a multi-stage build: `sdk:10.0` for build/publish, `aspnet:10.0` as the final base image (no SDK in production). Ports `8080` (HTTP) and `8081` (HTTPS) are exposed. The `.dockerignore` is linked into the project via `Creditbus.Facade.csproj` so Docker picks it up from the solution root.

### Folder Structure — Vertical Slices + DDD

The project follows Vertical Slice Architecture with DDD building blocks, organized by feature (bounded context). All separation is by namespace/folder within the single `Creditbus.Facade` project.

```
Creditbus.Facade/
├── Shared/
│   ├── Domain/          # Base classes: Entity<T>, AggregateRoot<T>, ValueObject, IDomainEvent
│   ├── Infrastructure/  # Shared DI extensions, logging, HTTP client factory setup
│   └── Api/             # Global middlewares, exception handlers, common filters
│
├── Features/
│   └── <FeatureName>/   # One folder per bounded context / feature
│       ├── Domain/      # Aggregates, Entities, Value Objects, Domain Events, IRepository interfaces
│       ├── Application/ # Use cases: Commands, Queries, Handlers, Validators, Response DTOs
│       ├── Infrastructure/ # Repository implementations, external HTTP clients, messaging adapters
│       └── Api/         # Minimal API endpoints or Controllers for this feature
│
└── Program.cs           # Composition root: registers all DI, maps all endpoints
```

#### Rules

- **Dependency direction:** `Api → Application → Domain`. `Infrastructure` implements interfaces defined in `Domain`/`Application` and is wired in `Program.cs` — it never leaks into other layers.
- **Cross-feature communication:** features must not reference each other directly. Use domain events or shared DTOs placed in `Shared/` when coordination is needed.
- **Shared/ must stay thin:** only truly cross-cutting abstractions belong there. If something is only used by one feature, it stays inside that feature.
- **New features:** add a new folder under `Features/` following the same four-subfolder layout (`Domain`, `Application`, `Infrastructure`, `Api`).

#### Current features

| Feature | Description |
|---|---|
| `CardsIngestion` | Ingestion pipeline for card data |
