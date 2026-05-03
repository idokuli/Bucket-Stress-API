FROM python:3.14.4-slim

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Install Terraform
ENV TERRAFORM_VERSION=1.9.5
RUN curl -fsSL https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip -o terraform.zip \
    && unzip terraform.zip -d /usr/local/bin/ \
    && rm terraform.zip

# Set working directory
WORKDIR /app

# Copy application code
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 443

# Default command: Start the Flask app
CMD ["python", "main.py"]