# justfile — Data Pipeline Local (Kind + Kafka + MinIO)
# Uso: just <recipe>   |   just --list

set shell := ["bash", "-c"]

CLUSTER_NAME := "data-pipeline"

# ─────────────────────────────────────────────
# Recipes públicos
# ─────────────────────────────────────────────

# Verifica se todas as dependências estão instaladas
[group('cluster')]
check-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    ok=true

    check() {
        local cmd=$1 hint=$2
        if command -v "$cmd" &>/dev/null; then
            printf "  ✓ %-12s %s\n" "$cmd" "($(command -v $cmd))"
        else
            printf "  ✗ %-12s não encontrado — %s\n" "$cmd" "$hint"
            ok=false
        fi
    }

    echo "Verificando dependências..."
    check docker       "https://docs.docker.com/get-docker/"
    check kind         "brew install kind  |  choco install kind"
    check kubectl      "brew install kubectl  |  choco install kubernetes-cli"
    check helm         "brew install helm  |  choco install kubernetes-helm"
    check just         "brew install just  |  choco install just"

    echo ""
    if [ "$ok" = true ]; then
        echo "Todas as dependências estão instaladas. Pode rodar 'just up'."
    else
        echo "Instale as dependências marcadas com ✗ antes de continuar."
        exit 1
    fi

# Sobe o ambiente completo com um único comando
[group('cluster')]
up: check-deps _cluster-create _namespaces _helm-repos _helm-install _kafka-cluster _kafka-topic _kafdrop _build-images _load-images _consumer-deploy
    @echo ""
    @echo "✓ Ambiente pronto! Execute 'just ui' para abrir os dashboards."
    @echo "  Inicie o producer: just producer-run"
    @echo "  Dados fluem automaticamente Kafka → MinIO bronze a cada 30s."

# Derruba o cluster inteiro
[group('cluster')]
down:
    #!/usr/bin/env bash
    kind delete cluster --name {{ CLUSTER_NAME }}
    echo "✓ Cluster '{{ CLUSTER_NAME }}' removido."

# Abre todos os dashboards via port-forward (Ctrl+C para fechar)
[group('observability')]
ui:
    #!/usr/bin/env bash
    echo ""
    echo "  MinIO Console  → http://localhost:9090  (admin / admin123)"
    echo "  Kafdrop (Kafka)→ http://localhost:9002"
    echo ""
    echo "Ctrl+C para encerrar todos os port-forwards."
    echo ""
    kubectl port-forward svc/minio-console 9090:9001 -n storage &
    kubectl port-forward svc/kafdrop       9002:9000 -n ingestion &
    wait

# Mostra o status de todos os pods
[group('observability')]
status:
    #!/usr/bin/env bash
    echo "=== ingestion ==="
    kubectl get pods -n ingestion
    echo ""
    echo "=== storage ==="
    kubectl get pods -n storage

# Aguarda todos os pods ficarem Ready (timeout 10min)
[group('observability')]
wait:
    #!/usr/bin/env bash
    echo "Aguardando ingestion..."
    kubectl wait --for=condition=Ready pods --all -n ingestion --timeout=600s
    echo "Aguardando storage..."
    kubectl wait --for=condition=Ready pods --all -n storage  --timeout=600s
    echo "✓ Todos os pods estão Ready."

# Exibe logs de um pod  |  uso: just logs <namespace> <pod>
[group('observability')]
logs namespace pod:
    #!/usr/bin/env bash
    kubectl logs -n {{ namespace }} {{ pod }} --tail=100 -f

# Aplica só o Kafka topic (útil após recriar o cluster)
[group('kafka')]
kafka-topic:
    #!/usr/bin/env bash
    kubectl apply -f k8s/kafka-topic.yaml

# Mostra logs do consumer Kafka → MinIO
[group('kafka')]
consumer-logs:
    #!/usr/bin/env bash
    kubectl logs -n ingestion deployment/bronze-consumer --tail=50 -f

# Roda o producer Kafka em foreground (Ctrl+C para parar)
[group('kafka')]
producer-run:
    #!/usr/bin/env bash
    kubectl run producer --image=pipeline/producer:local --image-pull-policy=Never \
        --restart=Never -n ingestion --rm -it

# Atualiza todos os repos Helm
[group('infra')]
helm-update:
    #!/usr/bin/env bash
    helm repo update

# Reconstrói as imagens Docker e carrega no Kind
[group('infra')]
rebuild: _build-images _load-images
    @echo "✓ Imagens recarregadas no cluster."

# ─────────────────────────────────────────────
# Recipes privados (prefixo _)
# ─────────────────────────────────────────────

[private]
_cluster-create:
    #!/usr/bin/env bash
    set -euo pipefail

    if kind get clusters 2>/dev/null | grep -q "^{{ CLUSTER_NAME }}$"; then
        echo "Cluster '{{ CLUSTER_NAME }}' já existe, pulando criação."
        kind export kubeconfig --name {{ CLUSTER_NAME }}
    else
        echo "Criando cluster Kind '{{ CLUSTER_NAME }}'..."
        kind create cluster --config infra/kind/cluster.yaml
    fi

    echo "Aguardando API do cluster ficar disponível..."
    for i in $(seq 1 30); do
        if kubectl cluster-info --request-timeout=3s &>/dev/null; then
            echo "✓ API acessível."
            break
        fi
        echo "  tentativa $i/30..."
        sleep 3
    done
    kubectl cluster-info

[private]
_namespaces:
    #!/usr/bin/env bash
    kubectl apply -f infra/namespaces.yaml

[private]
_helm-repos:
    #!/usr/bin/env bash
    helm repo add strimzi https://strimzi.io/charts/  2>/dev/null || true
    helm repo add minio   https://charts.min.io/       2>/dev/null || true
    helm repo update

[private]
_helm-install:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "--- Instalando Strimzi Operator ---"
    helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
        --namespace ingestion \
        --values helm/values/strimzi.yaml \
        --wait --timeout 5m

    echo "--- Instalando MinIO ---"
    helm upgrade --install minio minio/minio \
        --namespace storage \
        --values helm/values/minio.yaml \
        --wait --timeout 5m

[private]
_kafka-cluster:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl apply -f k8s/kafka-cluster.yaml
    echo "Aguardando Kafka cluster ficar Ready..."
    kubectl wait kafka/pipeline-kafka \
        --for=condition=Ready \
        --timeout=300s \
        -n ingestion

[private]
_kafka-topic:
    #!/usr/bin/env bash
    kubectl apply -f k8s/kafka-topic.yaml

[private]
_kafdrop:
    #!/usr/bin/env bash
    kubectl apply -f k8s/kafdrop.yaml
    kubectl wait deployment/kafdrop --for=condition=Available --timeout=120s -n ingestion

[private]
_build-images:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "--- Build: pipeline/producer ---"
    docker build -t pipeline/producer:local src/producer/
    echo "--- Build: pipeline/consumer ---"
    docker build -t pipeline/consumer:local src/consumer/

[private]
_load-images:
    #!/usr/bin/env bash
    echo "--- Carregando imagens no Kind ---"
    kind load docker-image pipeline/producer:local  --name {{ CLUSTER_NAME }}
    kind load docker-image pipeline/consumer:local  --name {{ CLUSTER_NAME }}

[private]
_consumer-deploy:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "--- Criando bucket datalake no MinIO ---"
    kubectl run bucket-init --image=python:3.11-slim --restart=Never -n storage --rm -i \
        -- python3 - < scripts/seed.py 2>/dev/null || true
    echo "--- Implantando consumer Kafka → MinIO bronze ---"
    kubectl apply -f k8s/consumer.yaml
    kubectl rollout status deployment/bronze-consumer -n ingestion --timeout=120s
    echo "✓ Consumer pronto. Dados fluem automaticamente para MinIO bronze."
