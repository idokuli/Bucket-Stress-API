# Bucket & Stress API Hub

A unified Flask web application for managing AWS S3 storage, orchestrating Terraform infrastructure, and executing stress tests on remote instances.

## 🚀 Features

### 🛠️ Infrastructure Hub (Terraform)
- **Real-time Streaming**: Watch Terraform `init`, `plan`, `apply`, and `destroy` output live in your browser.
- **Automated Lifecycle**: Provision a full VPC, Auto Scaling Group, and Load Balancer with one click.
- **Dynamic AMI**: Automatically finds the latest Amazon Linux 2023 image for `us-east-1`.
- **Automatic Cleanup**: Single-button destruction for decommissioning all resources.

### 📦 S3 Explorer
- **Auto-Categorization**: Uploads are automatically sorted into `/images`, `/documents`, or `/others`.
- **Versioning Control**: Enable/Disable bucket versioning and view file history.
- **Secure Downloads**: Presigned URL generation with support for UTF-8 (Hebrew) filenames.
- **Lifecycle Management**: Apply 30-day auto-deletion policies.
- **Content Search**: Search for specific words within text files directly from S3.

### ⚡ Stress Engine
- Trigger high-load scenarios on your instances to test Auto Scaling policies.

---

## 🛠️ Setup & Installation

### Prerequisites
- **Python 3.9+**
- **Terraform 1.5+**
- **AWS CLI** configured with valid credentials.
- **OpenSSL** (for automatic HTTPS certificate generation).

### 1. Clone & Install Dependencies
```bash
git clone <repository-url>
cd Bucket-Stress-API
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Environment Configuration
Copy the example environment file and fill in your AWS credentials. Generate a secret key for Flask as well:
```bash
cp .env.example .env
# Generate a secret key and paste it into .env
python3 -c 'import secrets; print(secrets.token_hex(32))'
```

### 3. Run the Application
The application runs over HTTPS (Port 443) by default and will auto-generate developer certificates if missing.
```bash
sudo venv/bin/python main.py
```
*Note: `sudo` is required to bind to privileged port 443.*

---

## 🐳 Run with Docker

You can run the entire Hub in a containerized environment (includes Python, Terraform, and all dependencies).

### 1. Build the Image
```bash
docker build -t bucket-stress-api .
```

### 2. Run the Container
Pass your environment variables via the `.env` file and map port 443:
```bash
docker run -p 443:443 --env-file .env bucket-stress-api
```

---

## 📂 Project Structure
- `main.py`: Flask application entry point and routing hub.
- `tf_runner.py`: Python wrapper for streaming Terraform commands.
- `s3_service.py`: Core logic for S3 interactions.
- `Tasks3_4_5/`: Contains Terraform modules for VPC, Load Balancing, and Deployments.
- `templates/`: Jinja2 HTML templates with Tailwind CSS and Lucide icons.

## 🛡️ Security Note
- This application uses self-signed certificates for local development. Your browser will show a warning; you can safely click "Advanced -> Proceed".
- Never commit your `.env` or `.pem` files to version control.
