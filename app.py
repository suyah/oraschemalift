"""Entry-point script – simply delegates to Uvicorn with the FastAPI app that
lives in the ``app`` package."""

import uvicorn

from app import app as fastapi_app, config  # type: ignore  # app object is created in package __init__


if __name__ == "__main__":
    # This script provides a production-style entry point.
    # For development, it's recommended to use the uvicorn command directly:
    # uvicorn app:app --reload --port 5001
    uvicorn.run(
        "app:app",  # module:variable path – now resolves correctly
        host=config.get('api', {}).get('host', "127.0.0.1"),
        port=config.get('api', {}).get('port', 5001),
        reload=config.get('api', {}).get('debug', False),
    )