FROM python:3.14.4-slim

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Install Terraform (Architecture Aware)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then TF_ARCH="amd64"; else TF_ARCH="arm64"; fi && \
    curl -fsSL "https://releases.hashicorp.com/terraform/1.9.5/terraform_1.9.5_linux_${TF_ARCH}.zip" -o terraform.zip && \
    unzip terraform.zip && \
    mv terraform /usr/local/bin/ && \
    rm terraform.zip

# Set working directory
WORKDIR /app

# Copy application code
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 443

# Default command: Start the Flask app
CMD ["python", "main.py"]