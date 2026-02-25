import os, secrets, subprocess, sys, multiprocessing, socket
import requests
from flask import Flask, render_template, Response, stream_with_context
from routes.s3_routes import s3_bp
from routes.stress_routes import stress_bp
from tf_runner import stream_terraform

app = Flask(__name__)

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
cert, key = os.path.join(BASE_DIR, 'cert.pem'), os.path.join(BASE_DIR, 'key.pem')

# Load .env file manually since python-dotenv is not installed
def load_env():
    env_path = os.path.join(BASE_DIR, '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    
    # Automate region mapping for Terraform
    region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION')
    if region:
        os.environ['TF_VAR_aws_region'] = region
        # Ensure standard AWS SDKs also see it
        os.environ.setdefault('AWS_DEFAULT_REGION', region)
        os.environ.setdefault('AWS_REGION', region)
        
    # Automate bucket name mapping for Terraform
    bucket = os.environ.get('S3_BUCKET_NAME')
    if bucket:
        os.environ['TF_VAR_bucket_name'] = bucket

load_env()

# Get secret key from .env or generate a random one
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# Register Blueprints
app.register_blueprint(s3_bp, url_prefix='/bucket')
app.register_blueprint(stress_bp, url_prefix='/stress')

# SSL Configuration exists at the top now

def get_ip_address():
    """Get the machine's public IP address"""
    try:
        response = requests.get("https://httpbin.org/ip", timeout=5)
        return response.json().get("origin", "Unknown")
    except Exception:
        return "Unknown"

@app.route('/')
def hub():
    return render_template('hub.html', cores=multiprocessing.cpu_count(), ip_address=get_ip_address())

@app.route('/health')
def health():
    return {"status": "healthy"}, 200

@app.route('/infra')
def infra_hub():
    return render_template('infra.html')

@app.route('/infra/<action>')
def trigger_infra(action):
    if action not in ['init', 'plan', 'apply', 'destroy']:
        return "Invalid action", 400
    return render_template('infra_result.html', action=action)

@app.route('/infra/stream/<action>')
def stream_infra(action):
    if action not in ['init', 'plan', 'apply', 'destroy']:
        return "Invalid action", 400
    
    def generate():
        # Using the Deployments module
        for line in stream_terraform(action, module_path="Tasks3_4_5/Modules/Deployments"):
            yield line
            
    return Response(stream_with_context(generate()), mimetype='text/plain')

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    # Ensure certs exist
    if not os.path.exists(cert):
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:4096", "-nodes", 
                       "-out", cert, "-keyout", key, "-days", "365", 
                       "-subj", "/CN=localhost"], check=True)
    
    print("--- 🛰️ HUB ONLINE [HTTPS:443] ---")
    run_simple('0.0.0.0', 443, app, ssl_context=(cert, key))