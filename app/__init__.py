import os

from app.config import config
from .utils.logger import setup_logger

# Ensure the directory structure defined in settings.yaml exists at import
# time so that any service can safely assume the folders are present.
if 'base_dirs' in config:
    for key, path in config['base_dirs'].items():
        if key != 'prompts':
            os.makedirs(path, exist_ok=True)

# Also pre-create workspace sub-directories (``testdata``, ``userdata``, …)
if 'workspace' in config.get('base_dirs', {}) and 'workspace_sub_dirs' in config:
    workspace_root = config['base_dirs']['workspace']
    top_level_keys = {"samples", "extracts", "uploads"}

    for name, sub in config["workspace_sub_dirs"].items():
        if name not in top_level_keys:
            continue
        if os.path.sep in sub:
            continue
        os.makedirs(os.path.join(workspace_root, sub), exist_ok=True)

# Log once during package import so we know the package was initialised.
setup_logger('app_init').info('app package initialised with FastAPI backend.')

# ------------------------- FastAPI application ---------------------------

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Create the FastAPI instance
app = FastAPI(title="SQL Converter API", version=config.get('api', {}).get('version', 'v1'))

# Allow cross-origin requests from any origin (Streamlit runs on localhost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


from .api.routes import api_router

app.include_router(api_router)

route_logger = setup_logger('routes')
for route in app.routes:
    if hasattr(route, 'methods'):
        route_logger.info(f"{list(route.methods)}  {route.path}")
