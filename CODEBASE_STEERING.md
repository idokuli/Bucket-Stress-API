# 🛰️ Bucket-Stress-API: Codebase Steering File

## 📌 Project Overview
This project is a hybrid infrastructure management and stress-testing tool. It provides a web-based Flask interface to:
1.  **Manage AWS S3**: List, upload, version, search, and set lifecycle policies.
2.  **Infrastructure Orchestration**: Run Terraform commands (`init`, `plan`, `apply`, `destroy`) via a web UI with real-time streaming output.
3.  **System Stress Testing**: Trigger CPU stress tests using `stress-ng`.
4.  **Kubernetes Deployment**: Deploy the entire application to a Kubernetes cluster using Helm.

---

## 🏗️ Core Architecture

### 1. Backend (Python/Flask)
-   **`main.py`**: Entry point. Now supports **SSL Termination**. It can run in plain HTTP mode (port 80) if `ENABLE_SSL=false` is set, or HTTPS (port 443) with auto-generated self-signed certificates.
-   **`s3_service.py`**: A clean wrapper for Boto3 S3 operations.
-   **`tf_runner.py`**: Orchestrates Terraform sub-processes and streams output back to the UI.

### 2. Infrastructure (Terraform)
-   **`Tasks3_4_5/Modules/Deployments`**: Contains the core Terraform logic.
-   Environment variables (like `AWS_REGION`) are passed to Terraform as `TF_VAR_*`.

### 3. Deployment (Docker & Helm)
-   **Docker**: A multi-tool image including Python 3.14, Terraform 1.9.5, and `stress-ng`.
-   **Helm Chart (`stress-app/`)**: 
    -   **SSL Termination**: Configured via Ingress (Nginx). The Ingress handles the public HTTPS connection and talks to the pod via HTTP on port 80.
    -   **Resources**: Configured with high limits (1.0 CPU, 1Gi RAM) to handle concurrent Terraform and Flask operations.
    -   **Dynamic Labels**: Centralized in `__helpers.tpl` for consistent tracking across all K8s resources.

---

## 🛠️ Key Technical Details

### Security & Networking
-   **Production Path**: Users -> Ingress (HTTPS) -> Service (HTTP:80) -> Pod (HTTP:80).
-   **Local Path**: Users -> Pod (HTTPS:443).
-   Self-signed certificates are generated on startup via `ensure_certs()` if they don't exist.

### Terraform Integration
-   Terraform is invoked via `subprocess.Popen` in `tf_runner.py`.
-   Output is streamed using Flask's `stream_with_context` for real-time browser updates.

---

## 🚀 Common Workflows
1.  **Local Run**: `python main.py` (Defaults to HTTPS:443).
2.  **K8s Deploy**: `helm upgrade --install stress-api-app ./stress-app`.
3.  **Sharing**: Use `ngrok http https://localhost:443` or the Ingress External IP.

## Answers to prompts
1. **To Me**: Explain the old archtecture and then the new one. Explain the changes, why you made them, and how you tested it.
2. **To End User**: When prompted, guide the end user to writing the changes in the code by referencing the documentation (e.g. `README.md`, `CODEBASE_STEERING.md`), and referencing in the documentation by the line number.