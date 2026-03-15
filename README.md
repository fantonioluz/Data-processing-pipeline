# Data Pipeline Local — Kafka → MinIO

Pipeline de dados local rodando em Kubernetes (Kind) com um único comando.

```
Producer → Kafka → Consumer → MinIO (bronze layer)
```

## Visão Geral

O producer gera eventos sintéticos de vendas **com dados intencionalmente sujos** (preços nulos, valores negativos, formatos de data inconsistentes, e-mails inválidos), simulando um ambiente real. O consumer lê o tópico Kafka e persiste os dados em formato JSON na camada bronze do MinIO, particionados por partição Kafka.

## Arquitetura

```
┌─────────────┐     Kafka      ┌──────────────┐     S3/MinIO    ┌─────────────────────────────────┐
│  Producer   │ ──────────── ▶ │   Consumer   │ ─────────────▶  │  datalake/bronze/events/        │
│  (Python)   │  raw-events    │   (Python)   │  batch (50 msg  │    raw-events/partition=N/      │
└─────────────┘                └──────────────┘   ou 30s)       │      part-<offset>.json         │
                                                                 └─────────────────────────────────┘
```

### Infraestrutura

| Componente         | Tecnologia                      | Namespace   |
|--------------------|---------------------------------|-------------|
| Cluster local      | Kind (Kubernetes in Docker)     | —           |
| Message broker     | Apache Kafka 4.2 (Strimzi KRaft)| `ingestion` |
| Object storage     | MinIO (S3-compatible)           | `storage`   |
| Kafka UI           | Kafdrop                         | `ingestion` |

## Pré-requisitos

| Ferramenta  | Instalação                                               |
|-------------|----------------------------------------------------------|
| Docker      | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| Kind        | `brew install kind` / `choco install kind`               |
| kubectl     | `brew install kubectl` / `choco install kubernetes-cli`  |
| Helm        | `brew install helm` / `choco install kubernetes-helm`    |
| just        | `brew install just` / `choco install just`               |

## Uso

```bash
# Verifica se todas as dependências estão instaladas
just check-deps

# Sobe o ambiente completo (cluster + Kafka + MinIO + Consumer)
just up

# Abre os dashboards no navegador
just ui

# Inicia o producer (Ctrl+C para parar)
just producer-run
```

### Comandos disponíveis

```
just --list
```

| Comando             | Descrição                                              |
|---------------------|--------------------------------------------------------|
| `check-deps`        | Verifica dependências instaladas                       |
| `up`                | Sobe o ambiente completo                               |
| `down`              | Derruba o cluster                                      |
| `ui`                | Abre MinIO Console e Kafdrop via port-forward          |
| `status`            | Lista todos os pods e seus status                      |
| `wait`              | Aguarda todos os pods ficarem Ready                    |
| `logs <ns> <pod>`   | Exibe logs de um pod específico                        |
| `producer-run`      | Roda o producer em foreground                          |
| `consumer-logs`     | Exibe logs do consumer em tempo real                   |
| `kafka-topic`       | Re-aplica o manifesto do tópico Kafka                  |
| `rebuild`           | Reconstrói as imagens Docker e recarrega no Kind       |
| `helm-update`       | Atualiza os repositórios Helm                          |

## Dashboards

Após `just up`, rode `just ui` e acesse:

| Dashboard        | URL                        | Credenciais       |
|------------------|----------------------------|-------------------|
| MinIO Console    | http://localhost:9090       | admin / admin123  |
| Kafdrop (Kafka)  | http://localhost:9002       | —                 |

## Estrutura do Projeto

```
.
├── justfile                  # Automação de tarefas (just)
├── infra/
│   ├── kind/
│   │   └── cluster.yaml      # Configuração do cluster Kind (4 nós)
│   └── namespaces.yaml       # Namespaces Kubernetes
├── helm/
│   └── values/
│       ├── strimzi.yaml      # Valores do Strimzi Kafka Operator
│       └── minio.yaml        # Valores do MinIO
├── k8s/
│   ├── kafka-cluster.yaml    # Cluster Kafka (Strimzi KRaft)
│   ├── kafka-topic.yaml      # Tópico raw-events
│   ├── consumer.yaml         # Deployment do consumer bronze
│   └── kafdrop.yaml          # Kafka UI
├── src/
│   ├── producer/             # Producer: gera eventos sintéticos de vendas
│   │   ├── producer.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── consumer/             # Consumer: Kafka → MinIO bronze (JSON)
│       ├── consumer.py
│       ├── Dockerfile
│       └── requirements.txt
└── scripts/
    └── seed.py               # Inicializa o bucket datalake no MinIO
```

## Dados Produzidos

O producer gera eventos com o seguinte schema:

```json
{
  "event_id": "uuid-v4",
  "produto": "Notebook | Teclado | Mouse | Monitor",
  "preco": 1500.00,
  "quantidade": 3,
  "data": "2024-01-15",
  "email": "usuario@exemplo.com"
}
```

Os dados incluem **erros intencionais** para simular qualidade de dados real:
- Preços nulos ou negativos
- Datas em formatos inconsistentes (`DD/MM/YYYY`, timestamps, etc.)
- E-mails inválidos
- Quantidades zeradas

## Fluxo dos Dados

1. **Producer** gera 1 evento a cada 0.5s e publica no tópico `raw-events`
2. **Consumer** lê o tópico e acumula mensagens por partição
3. A cada **50 mensagens** ou **30 segundos**, o consumer faz flush para MinIO
4. Os arquivos são salvos em:
   ```
   s3://datalake/bronze/events/raw-events/partition=<N>/part-<offset>.json
   ```

## Destruindo o Ambiente

```bash
just down
```

Remove o cluster Kind e todos os recursos associados.
