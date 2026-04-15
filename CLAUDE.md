# CLAUDE.md

Este arquivo fornece orientações ao Claude Code (claude.ai/code) ao trabalhar com o código neste repositório.

## Idioma

- Toda documentação, comentários de código, mensagens de commit e arquivos Markdown devem ser escritos em **Português do Brasil**.

## Comandos

```bash
# Build
dotnet build Creditbus.Facade.sln

# Executar
dotnet run --project Creditbus.Facade/Creditbus.Facade.csproj

# Executar testes (quando projetos de teste forem adicionados)
dotnet test Creditbus.Facade.sln

# Publicar
dotnet publish Creditbus.Facade/Creditbus.Facade.csproj -c Release

# Construir imagem Docker (a partir da raiz do repositório)
docker build -f Creditbus.Facade/Dockerfile -t creditbus-facade .
```

## Teste de Carga (Load Test)

O projeto `Creditbus.Facade.LoadTests` publica mensagens `PortfolioDataUpdatedEvent` aleatórias e realistas no Kafka para teste de carga do `CardsIngestionKafkaConsumer`.

### Pré-requisito

O Kafka precisa estar rodando. Use o Docker Compose incluído na solution:

```bash
docker compose up -d
```

A Kafka UI fica disponível em `http://localhost:8090`.

### Executar o teste completo (aplicação + publisher)

```bash
# 1. Subir o Kafka
docker compose up -d

# 2. Subir a aplicação em background
dotnet run --project Creditbus.Facade/Creditbus.Facade.csproj > facade.log 2>&1 &

# 3. Rodar o publisher
dotnet run --project Creditbus.Facade.LoadTests -- --rate 1000 --workers 8 --duration 5m
```

### Parâmetros do publisher

| Parâmetro | Descrição | Padrão |
|---|---|---|
| `--rate` | Mensagens por segundo | `100` |
| `--workers` | Workers paralelos de publicação | `4` |
| `--duration` | Duração total (ex: `30s`, `2m`, `1h`) | roda até Ctrl+C |
| `--broker` | Bootstrap server Kafka | `localhost:9092` |
| `--topic` | Tópico Kafka de destino | `creditbus.ingestion` |


> **Header obrigatório:** as mensagens devem conter o header `message-type: CardsIngestionEvent` para que o `PartitionWorker` roteie corretamente ao consumer. Sem esse header, as mensagens vão para o DLQ.

---

## Arquitetura

Web API .NET 10 de projeto único com alvo em containers Linux. O ponto de entrada é `Creditbus.Facade/Program.cs`, utilizando top-level statements e Minimal API.

O Dockerfile utiliza um build multi-estágio: `sdk:10.0` para build/publicação, `aspnet:10.0` como imagem final (sem SDK em produção). As portas `8080` (HTTP) e `8081` (HTTPS) são expostas. O `.dockerignore` é vinculado ao projeto via `Creditbus.Facade.csproj` para que o Docker o localize a partir da raiz da solução.

### Estrutura de Pastas — Vertical Slices + DDD

O projeto segue a Arquitetura de Fatias Verticais com blocos de construção DDD, organizado por funcionalidade (bounded context). Toda a separação é feita por namespace/pasta dentro do único projeto `Creditbus.Facade`.

```
Creditbus.Facade/
├── Shared/
│   ├── Domain/          # Classes base: Entity<T>, AggregateRoot<T>, ValueObject, IDomainEvent
│   ├── Infrastructure/  # Extensões de DI compartilhadas, logging, configuração do HTTP client factory
│   └── Api/             # Middlewares globais, tratadores de exceção, filtros comuns
│
├── Features/
│   └── <NomeDaFeature>/ # Uma pasta por bounded context / funcionalidade
│       ├── Domain/      # Agregados, Entidades, Value Objects, Eventos de Domínio, interfaces IRepository
│       ├── Application/ # Casos de uso: Commands, Queries, Handlers, Validators, DTOs de resposta
│       ├── Infrastructure/ # Implementações de repositório, clientes HTTP externos, adaptadores de mensageria
│       └── Api/         # Endpoints Minimal API ou Controllers desta funcionalidade
│
└── Program.cs           # Raiz de composição: registra todo DI, mapeia todos os endpoints
```

#### Regras

- **Direção de dependência:** `Api → Application → Domain`. `Infrastructure` implementa interfaces definidas em `Domain`/`Application` e é configurado em `Program.cs` — nunca vaza para outras camadas.
- **Comunicação entre features:** as features não devem referenciar umas às outras diretamente. Utilize eventos de domínio ou DTOs compartilhados em `Shared/` quando for necessária coordenação.
- **Shared/ deve ser enxuto:** apenas abstrações verdadeiramente transversais pertencem a esse local. Se algo é utilizado por apenas uma feature, deve permanecer dentro dela.
- **Novas features:** adicionar uma nova pasta em `Features/` seguindo o mesmo layout de quatro subpastas (`Domain`, `Application`, `Infrastructure`, `Api`).

#### Convenções de nomenclatura

- **Consumers Kafka:** classes que implementam `IKafkaMessageHandler` devem ser nomeadas `<NomeDaFeature>KafkaConsumer` e residir em `Features/<NomeDaFeature>/Infrastructure/`. Exemplo: `CardsIngestionKafkaConsumer`.

#### Features atuais

| Feature | Descrição |
|---|---|
| `CardsIngestion` | Pipeline de ingestão de dados de cartões |
