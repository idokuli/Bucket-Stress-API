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
-   **`main.py`**: Entry point. Configures HTTPS (port 443), registers blueprints, and handles environment variable mapping for Terraform.
-   **`s3_service.py`**: A clean wrapper for Boto3 S3 operations.
-   **`tf_runner.py`**: Orchestrates Terraform sub-processes and streams output back to the UI.
-   **`routes/`**:
    -   `s3_routes.py`: UI logic for S3 explorer and management.
    -   `stress_routes.py`: Logic for starting/stopping `stress-ng` processes.

### 2. Infrastructure (Terraform)
-   **`Tasks3_4_5/Modules/Deployments`**: Contains the core Terraform logic (`main.tf`, `variables.tf`, `output.tf`).
-   The Flask app passes environment variables (like `AWS_REGION`) to Terraform as `TF_VAR_*`.

### 3. Deployment (Docker & Helm)
-   **`dockerfile`**: A multi-tool image that includes Python 3.14-slim, Terraform 1.9.5, and necessary system dependencies.
-   **`stress-app/`**: Helm chart for K8s deployment.
    -   `values.yaml`: Main configuration.
    -   `templates/`: K8s manifests (Deployment, ConfigMap, Secret, etc.).

---

## 🛠️ Key Technical Details

### Security & Networking
-   The app runs on **HTTPS (443)**.
-   It generates self-signed certificates (`cert.pem`, `key.pem`) on startup if they don't exist.
-   S3 credentials are stored in the Flask session (`session['access']`, etc.) after login via the UI.

### Terraform Integration
-   Terraform is invoked via `subprocess.Popen` in `tf_runner.py`.
-   The output is streamed using Flask's `stream_with_context` to provide real-time updates in the browser.

### Stress Testing
-   Uses `stress-ng` binary (installed in the Docker image).
-   Allows targeting a specific number of CPU cores for a defined duration.

---

## 📂 File Map
-   `main.py`: App initialization & Hub route.
-   `s3_service.py`: AWS SDK logic.
-   `tf_runner.py`: Terraform subprocess logic.
-   `routes/`: Blueprint-specific routes.
-   `templates/`: Jinja2 templates for the UI.
-   `stress-app/`: Helm chart source.
-   `Tasks3_4_5/`: Terraform module source.

---

## 🚀 Common Workflows
1.  **Local Run**: `python main.py` (Requires `stress-ng` and `terraform` locally).
2.  **Docker Build**: `docker build . -t idokuli/stressapp`
3.  **K8s Deploy**: `helm install stress-app ./stress-app`
