#!/usr/bin/env bash
set -e

PODMAN="${PODMAN:-sudo podman}"
KAFKA_IMAGE="${KAFKA_IMAGE:-docker.io/apache/kafka:latest}"
CLUSTER_ID="${CLUSTER_ID:-4L622nShTUiBenVKBpahA0}"
NETWORK="kafka-cluster-net"

ACTION="${1:-up}"

case "$ACTION" in
  up)
    echo "==> Creating container network '$NETWORK' if not exists..."
    $PODMAN network exists "$NETWORK" 2>/dev/null || $PODMAN network create "$NETWORK"

    echo "==> Pulling image $KAFKA_IMAGE..."
    $PODMAN pull -q "$KAFKA_IMAGE"

    echo "==> Starting 3-Broker KRaft Cluster on ports 9092, 9093, 9094..."
    if $PODMAN ps --format '{{.Names}}' | grep -Eq "^kafka-local$"; then
      echo "--> Stopping existing single-node 'kafka-local' container to free port 9092..."
      $PODMAN stop kafka-local 2>/dev/null || true
    fi

    # Common tuning for 1000+ MB/s
    PERF_OPTS=(
      -e "KAFKA_NUM_NETWORK_THREADS=8"
      -e "KAFKA_NUM_IO_THREADS=16"
      -e "KAFKA_SOCKET_SEND_BUFFER_BYTES=4194304"
      -e "KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=4194304"
      -e "KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600"
      -e "KAFKA_MESSAGE_MAX_BYTES=52428800"
      -e "KAFKA_REPLICA_FETCH_MAX_BYTES=52428800"
      -e "KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=9223372036854775807"
      -e "KAFKA_LOG_FLUSH_INTERVAL_MS=9223372036854775807"
      -e "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3"
      -e "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3"
      -e "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2"
      -e "KAFKA_DEFAULT_REPLICATION_FACTOR=3"
      -e "KAFKA_NUM_PARTITIONS=12"
      -e "KAFKA_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20"
    )

    VOTERS="1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093"

    for ID in 1 2 3; do
      NAME="kafka-$ID"
      EXT_PORT=$((9091 + ID)) # 9092, 9093, 9094

      if $PODMAN ps -a --format '{{.Names}}' | grep -Eq "^$NAME$"; then
        echo "--> Container $NAME already exists. Starting it..."
        $PODMAN start "$NAME"
      else
        echo "--> Creating & launching broker $NAME (external port: $EXT_PORT)..."
        $PODMAN run -d \
          --name "$NAME" \
          --network "$NETWORK" \
          -p "${EXT_PORT}:9092" \
          -e "KAFKA_NODE_ID=$ID" \
          -e "KAFKA_PROCESS_ROLES=broker,controller" \
          -e "CLUSTER_ID=$CLUSTER_ID" \
          -e "KAFKA_CONTROLLER_QUORUM_VOTERS=$VOTERS" \
          -e "KAFKA_LISTENERS=INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:29093" \
          -e "KAFKA_ADVERTISED_LISTENERS=INTERNAL://$NAME:19092,EXTERNAL://localhost:$EXT_PORT" \
          -e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT" \
          -e "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER" \
          -e "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL" \
          "${PERF_OPTS[@]}" \
          "$KAFKA_IMAGE"
      fi
    done

    echo "==> 3-Broker Kafka Cluster is running!"
    echo "    Broker 1: localhost:9092"
    echo "    Broker 2: localhost:9093"
    echo "    Broker 3: localhost:9094"
    echo "    Connection String: localhost:9092,localhost:9093,localhost:9094"
    ;;

  down)
    echo "==> Stopping 3-Broker Kafka Cluster..."
    for ID in 1 2 3; do
      NAME="kafka-$ID"
      echo "--> Stopping $NAME..."
      $PODMAN stop "$NAME" 2>/dev/null || true
      $PODMAN rm "$NAME" 2>/dev/null || true
    done
    $PODMAN network rm "$NETWORK" 2>/dev/null || true
    echo "==> Cluster stopped and cleaned up."
    ;;

  status)
    echo "==> 3-Broker Kafka Cluster Status:"
    $PODMAN ps -a --filter "name=^kafka-[123]$"
    ;;

  logs)
    TARGET="${2:-kafka-1}"
    echo "==> Following logs for $TARGET (pass kafka-1, kafka-2, or kafka-3)..."
    $PODMAN logs -f "$TARGET"
    ;;

  *)
    echo "Usage: $0 {up|down|status|logs [broker]}"
    exit 1
    ;;
esac
