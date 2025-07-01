import yaml
from pathlib import Path
import os
import logging

def load_config():
    """Load configuration from settings.yaml located in the app directory."""
    try:
        app_config_dir = Path(__file__).parent
        app_module_dir = app_config_dir.parent
        project_root = app_module_dir.parent
        
        settings_path = app_module_dir / 'settings.yaml'
        
        if not settings_path.exists():
            logging.error(f"Critical: settings.yaml not found at expected path: {settings_path}")
            raise FileNotFoundError(f"settings.yaml not found at {settings_path}")

        with open(settings_path) as f:
            config_data = yaml.safe_load(f)
        
        if not config_data:
            config_data = {}
            logging.warning(f"settings.yaml at {settings_path} is empty or invalid.")

        # Replace environment variables (e.g., for OCI_COMPARTMENT_ID)
        if 'llm' in config_data and 'compartment_id' in config_data['llm'] and \
           isinstance(config_data['llm']['compartment_id'], str) and \
           '${OCI_COMPARTMENT_ID}' in config_data['llm']['compartment_id']:
            compartment_id = os.getenv('OCI_COMPARTMENT_ID')
            if not compartment_id:
                logging.warning("OCI_COMPARTMENT_ID environment variable is referenced in settings.yaml but not set.")
                config_data['llm']['compartment_id'] = None
            else:
                config_data['llm']['compartment_id'] = compartment_id
        
        # Ensure base_dirs paths are absolute, resolved from project_root
        resolved_base_dirs = {}
        if 'base_dirs' in config_data:
            for key, path_str in config_data['base_dirs'].items():
                if isinstance(path_str, str):
                    if not os.path.isabs(path_str):
                        resolved_base_dirs[key] = str((project_root / path_str).resolve())
                    else:
                        resolved_base_dirs[key] = path_str
                else:
                    resolved_base_dirs[key] = path_str
            config_data['base_dirs'] = resolved_base_dirs
        else:
            logging.info("'base_dirs' not found in settings.yaml.")
            config_data['base_dirs'] = {}
        
        return config_data
            
    except FileNotFoundError as fnfe:
        logging.error(f"Configuration Error: {fnfe}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Error loading configuration from {settings_path if 'settings_path' in locals() else 'unknown path'}: {e}", exc_info=True)
        raise Exception(f"Failed to load application configuration: {e}") from e

# Load config at import time
try:
    config = load_config()
except Exception as e:
    logging.critical(f"CRITICAL FAILURE: Could not load application settings. Error: {e}", exc_info=True)
    raise SystemExit(f"Application cannot start due to configuration load failure: {e}")
