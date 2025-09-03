#!/bin/bash
set -e
NAMESPACE="kafka"
CLUSTER_NAME="k3"

# Имя секретов, созданных cert-manager
CERT_CLUSTER_CA_SECRET="kafka-test-cluster-ca-cert"
CERT_CLIENTS_CA_SECRET="kafka-test-clients-ca-cert"

# Директория для временных файлов
TMP_DIR="certs"

# 🔽 Извлечение из cert-manager secret
kubectl get secret ${CERT_CLUSTER_CA_SECRET} -n $NAMESPACE -o jsonpath='{.data.tls\.crt}' | base64 -d > "$TMP_DIR/cluster-tls.crt"
kubectl get secret ${CERT_CLUSTER_CA_SECRET} -n $NAMESPACE -o jsonpath='{.data.tls\.key}' | base64 -d > "$TMP_DIR/cluster-tls.key"
kubectl get secret ${CERT_CLUSTER_CA_SECRET} -n $NAMESPACE -o jsonpath='{.data.ca\.crt}' | base64 -d >>"$TMP_DIR/cluster-tls.crt"

openssl pkcs12 -export -inkey ${TMP_DIR}/cluster-tls.key -in ${TMP_DIR}/cluster-tls.crt -password file:ca.password -out ${TMP_DIR}/cluster-ca.p12

kubectl get secret ${CERT_CLIENTS_CA_SECRET} -n $NAMESPACE -o jsonpath='{.data.tls\.crt}' | base64 -d > "$TMP_DIR/clients-tls.crt"
kubectl get secret ${CERT_CLIENTS_CA_SECRET} -n $NAMESPACE -o jsonpath='{.data.tls\.key}' | base64 -d > "$TMP_DIR/clients-tls.key"
kubectl get secret ${CERT_CLIENTS_CA_SECRET} -n $NAMESPACE -o jsonpath='{.data.ca\.crt}' | base64 -d >> "$TMP_DIR/clients-tls.crt"

openssl pkcs12 -export -inkey ${TMP_DIR}/cluster-tls.key -in ${TMP_DIR}/cluster-tls.crt -password file:ca.password -out ${TMP_DIR}/clients-ca.p12

# 🔒 Создание YAML-файлов для Kubernetes Secrets
for TYPE in cluster clients; do
  # b64_ca=$(base64 -w 0 "$TMP_DIR/${TYPE}-ca.crt")
  b64_key=$(base64 -w 0 "$TMP_DIR/${TYPE}-tls.key")
  b64_crt=$(base64 -w 0 "$TMP_DIR/${TYPE}-tls.crt")
  b64_p12=$(base64 -w 0 "$TMP_DIR/${TYPE}-ca.p12")

  cat <<EOF > "${TYPE}-ca.yaml"
apiVersion: v1
kind: Secret
metadata:
  name: ${CLUSTER_NAME}-${TYPE}-ca
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/instance: ${CLUSTER_NAME}
    app.kubernetes.io/managed-by: strimzi-cluster-operator
    app.kubernetes.io/name: certificate-authority
    app.kubernetes.io/part-of: strimzi-${CLUSTER_NAME}
    strimzi.io/cluster: ${CLUSTER_NAME}
    strimzi.io/kind: Kafka
    strimzi.io/component-type: certificate-authority
  annotations:
    strimzi.io/ca-key-generation: "0"
type: Opaque
data:
  ca.key: ${b64_key}
EOF

  cat <<EOF > "${TYPE}-ca-cert.yaml"
apiVersion: v1
kind: Secret
metadata:
  name: ${CLUSTER_NAME}-${TYPE}-ca-cert
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/instance: ${CLUSTER_NAME}
    app.kubernetes.io/managed-by: strimzi-cluster-operator
    app.kubernetes.io/name: certificate-authority
    app.kubernetes.io/part-of: strimzi-${CLUSTER_NAME}
    strimzi.io/cluster: ${CLUSTER_NAME}
    strimzi.io/kind: Kafka
    strimzi.io/component-type: certificate-authority
  annotations:
    strimzi.io/ca-cert-generation: "0"
type: Opaque
data:
  ca.crt: ${b64_crt}
  ca.p12: ${b64_p12}
  ca.password: 8o37e97uZPx9
EOF
done

echo "✅ Secrets YAML created in current directory:"
echo "clients-ca.yaml"
echo ---
kubectl apply -f clients-ca.yaml
echo ---
echo "clients-ca-cert.yaml"
echo ---
kubectl apply -f clients-ca-cert.yaml
echo ---
echo "cluster-ca.yaml"
echo ---
kubectl apply -f cluster-ca.yaml
echo ---
echo cluster-ca-cert.yaml
echo ---
kubectl apply -f cluster-ca-cert.yaml
echo ---

# rm -rf "$TMP_DIR"
