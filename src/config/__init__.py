import base64
import os

from dotenv import load_dotenv

# Load variables from .env into os.environ
load_dotenv()

# Validate required environment variables
required_vars = ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_HOST", "OPENAI_API_KEY"]
missing = [var for var in required_vars if not os.environ.get(var)]
if missing:
    raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

# Build Langfuse Basic Auth header
LANGFUSE_AUTH = base64.b64encode(
    f"{os.environ['LANGFUSE_PUBLIC_KEY']}:{os.environ['LANGFUSE_SECRET_KEY']}".encode()
).decode()

# Set OTEL exporter environment variables
os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = os.environ["LANGFUSE_HOST"] + "/api/public/otel"
os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = f"Authorization=Basic {LANGFUSE_AUTH}"
