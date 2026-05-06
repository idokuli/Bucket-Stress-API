# 🛰️ Bucket & Stress API Hub

A unified Flask web application for managing AWS S3 storage, orchestrating Terraform infrastructure, and executing stress tests on remote instances.

## 🚀 Features

### 🛠️ Infrastructure Hub (Terraform)
- **Real-time Streaming**: Watch Terraform `init`, `plan`, `apply`, and `destroy` output live in your browser.
- **Automated Lifecycle**: Provision full stacks with one click.
- **Automatic Cleanup**: Single-button destruction for decommissioning all resources.

### 📦 S3 Explorer
- **Auto-Categorization**: Sorting into `/images`, `/documents`, or `/others`.
- **Versioning Control**: Enable/Disable bucket versioning and view history.
- **Secure Downloads**: Presigned URLs with Hebrew (UTF-8) support.

### ⚡ Stress Engine
- Trigger high-load scenarios to test Auto Scaling policies.

---

## ⚙️ Configuration Matrix

| Environment | Config File | Key Parameters |
| :--- | :--- | :--- |
| **Local Python** | `.env` | AWS Keys, Flask Secret Key |
| **Docker** | `.env` | Passed via `--env-file` |
| **Kubernetes (Helm)** | `aws-values.yaml` | AWS Keys (Injected via K8s Secrets) |

---

## 🏗️ Deployment Guide (Kubernetes & Helm)

This is the recommended production-ready deployment method.

### 1. Prerequisites
```bash
minikube start
minikube addons enable ingress
```

### 2. Configuration
Create a file named `stress-app/aws-values.yaml`:
```yaml
# stress-app/aws-values.yaml
AWS_ACCESS_KEY_ID: "YOUR_ACCESS_KEY"
AWS_SECRET_ACCESS_KEY: "YOUR_SECRET_KEY"
AWS_REGION: "us-east-1"
```

### 3. Deployment
```bash
cd stress-app
helm upgrade --install my-app -f values.yaml -f aws-values.yaml .
```

### 4. Networking & Access
1.  **Start Tunnel**: `minikube tunnel` (in a separate terminal).
2.  **Map Hostname**: Add `127.0.0.1 my-app.local` to your `/etc/hosts`.
3.  **Visit**: [https://my-app.local](https://my-app.local) (Secure Ingress) or [http://127.0.0.1:5001](http://127.0.0.1:5001) (Standard).

---

## 🏗️ Architecture: SSL Termination
The project uses **SSL Termination** at the Ingress level.
- **The Ingress (Port 443)**: Handles SSL and decrypts traffic.
- **The Application (Port 5001)**: Runs as plain HTTP inside the cluster for performance.

## 📂 Project Structure
- `main.py`: App entry point.
- `stress-app/`: Full Helm chart.
- `stress-app/aws-values.yaml`: (Ignored by Git) Stores AWS credentials.
- `tf_runner.py`: Python wrapper for Terraform.
- `s3_service.py`: logic for S3 interactions.

## 🛡️ Security
- **Credentials**: Handled via Kubernetes Secrets.
- **Exclusion**: `aws-values.yaml` and `.env` are both ignored by Git to prevent leaks.
