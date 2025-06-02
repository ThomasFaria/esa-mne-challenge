from dotenv import load_dotenv

from .langfuse import setup_langfuse


def setup():
    """Global setup routine"""
    # Load variables from .env into os.environ
    load_dotenv()
    # Setup Langfuse
    setup_langfuse()
