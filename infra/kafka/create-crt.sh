#!/bin/bash

export K_PASSWORD="8vfUerfcHP"
USER_LIST=(
  k3-guest
  consumer
  k3-user
  produser
  k3-admin
)
CERT_DIR="./certs"
mkdir -p "$CERT_DIR"

# 🐳 Запуск контейнера JDK
docker rm -f jdk 2>/dev/null
docker run --name jdk -d -v "$(pwd)/certs:/certs" -e K_PASSWORD=$K_PASSWORD openjdk:23-jdk tail -f /dev/null

for user in "${USER_LIST[@]}"; do
  echo "🔧 Генерация ресурсов для пользователя: $user"

  # === 📄 Генерация KafkaUser YAML ===
  cat > "$CERT_DIR/k3-${user}-user.yaml" <<EOF
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: ${user}
  namespace: kafka
  labels:
    strimzi.io/cluster: k3
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
        operations:
          - All
      - resource:
          type: group
        operations:
          - All
      - resource:
          type: cluster
        operations:
          - All
EOF

  # === 📄 Генерация Certificate YAML ===
  cat > "$CERT_DIR/k3-${user}-tls.yaml" <<EOF
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${user}-tls
  namespace: kafka
spec:
  secretName: ${user}-tls
  duration: 87600h
  renewBefore: 7200h
  subject:
    organizations: ["Homelab"]
  commonName: ${user}
  isCA: false
  privateKey:
    algorithm: RSA
    encoding: PKCS8
    size: 4096
    rotationPolicy: Always
  usages:
    - digital signature
    - key encipherment
    - client auth
  issuerRef:
    name: homelab-ca-issuer
    kind: ClusterIssuer
EOF

  # ✅ Применение манифестов
  kubectl apply -f "$CERT_DIR/k3-${user}-user.yaml"
  sleep 10
  kubectl apply -f "$CERT_DIR/k3-${user}-tls.yaml"
  sleep 10

  # 🧾 Извлечение TLS
  kubectl get secret ${user}-tls -n kafka -o jsonpath='{.data.tls\.crt}' | base64 -d > "$CERT_DIR/${user}.crt"
  sleep 5
  kubectl get secret ${user}-tls -n kafka -o jsonpath='{.data.tls\.key}' | base64 -d > "$CERT_DIR/${user}.key"
  sleep 5

  # 🔍 Проверка сертификата
  openssl rsa -modulus -noout -in "$CERT_DIR/${user}.key" | openssl md5
  openssl x509 -modulus -noout -in "$CERT_DIR/${user}.crt" | openssl md5
  openssl verify -verbose -CAfile "$CERT_DIR/ca.crt" "$CERT_DIR/${user}.crt"
  openssl x509 -text -noout -in "$CERT_DIR/${user}.crt" | grep "CN ="

  # 📦 Создание .p12
  openssl pkcs12 -export \
    -in "$CERT_DIR/${user}.crt" \
    -inkey "$CERT_DIR/${user}.key" \
    -out "$CERT_DIR/${user}.p12" \
    -passout pass:$K_PASSWORD
  
  # 🛡️ Импорт в truststore и keystore
  docker exec jdk bash -c "cd /certs && keytool -delete -alias CARoot -keystore kafka.truststore.jks -storepass "$K_PASSWORD" || true"  
  docker exec jdk bash -c "cd /certs && keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca.crt -storepass '${K_PASSWORD}' -noprompt"
  
  docker exec jdk bash -c "cd /certs && keytool -importkeystore \
    -srckeystore ${user}.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass '${K_PASSWORD}' \
    -deststorepass '${K_PASSWORD}' \
    -destkeystore ${user}.jks \
    -noprompt"

  echo "✅ Пользователь $user завершён"
  echo "----------------------------"
done

echo "🎯 Все пользователи созданы и сертификаты выпущены"
