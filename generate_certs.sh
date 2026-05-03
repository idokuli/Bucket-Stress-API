#!/bin/bash

# Directory where certs will be stored
CERT_DIR="."

# Generate self-signed certificate and key
echo "Generating self-signed SSL certificate..."
openssl req -x509 -newkey rsa:4096 -nodes \
    -out "$CERT_DIR/cert.pem" \
    -keyout "$CERT_DIR/key.pem" \
    -days 365 \
    -subj "/CN=localhost"

echo "Done! cert.pem and key.pem have been generated in $CERT_DIR"
echo "To use these in Kubernetes, run:"
echo "kubectl create secret tls flask-ssl --cert=cert.pem --key=key.pem"
