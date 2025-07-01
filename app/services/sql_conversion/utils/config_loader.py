import json
from pathlib import Path
from typing import Dict, Any
from app import config as app_global_config # To get app base directory
import logging # Added for default logger if none provided

def load_json_from_conversion_config(
    logger: Any,
    source_type: str,
    target_type: str,
    rules_subdirectory: str, # e.g., 'declarative_procedural_rules' or 'ddl_conversion_rules'
    config_filename: str
) -> Dict:
    """
    Loads a JSON configuration file from the structured conversion config directory.
    Expected path structure: app_base_dir/config/conversion/{source_type}_{target_type}/{rules_subdirectory}/{config_filename}
    """
    effective_logger = logger if logger is not None else logging.getLogger(__name__)
    # Define full_config_path early for use in except blocks if path construction fails
    full_config_path = "an unspecified path" # Default value
    try:
        app_base_dir = app_global_config.get('base_dirs', {}).get('app')
        if not app_base_dir:
            effective_logger.error("App base directory ('base_dirs'['app']) not found in global app_config.")
            return {}

        s_type = source_type.lower() if source_type else ''
        t_type = target_type.lower() if target_type else ''
        
        if not s_type or not t_type:
            effective_logger.error(f"Source type ('{source_type}') or target type ('{target_type}') is empty, cannot construct config path for {config_filename}.")
            return {}

        base_conversion_path = Path(app_base_dir) / 'config' / 'conversion' / f'{s_type}_{t_type}'
        # Assign to full_config_path once constructed successfully
        full_config_path = base_conversion_path / rules_subdirectory / config_filename
        
        if not full_config_path.exists():
            effective_logger.info(f"Configuration file not found (this may be expected): {full_config_path}")
            return {}

        with open(full_config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            effective_logger.debug(f"Successfully loaded configuration from {full_config_path}")
            return data
    except KeyError as ke:
        effective_logger.error(f"KeyError: '{ke}' - 'base_dirs' or 'app' key might be missing in global app_config. Cannot load {config_filename}.", exc_info=True)
        return {}
    except json.JSONDecodeError as jde:
        effective_logger.error(f"Error decoding JSON from {str(full_config_path)}: {jde}", exc_info=True)
        return {}
    except (IOError, OSError) as ioe:
        effective_logger.error(f"File system error (IOError/OSError) loading configuration file {str(full_config_path)}: {ioe}", exc_info=True)
        return {}
    except Exception as e:
        effective_logger.error(f"Unexpected error loading configuration file {str(full_config_path)}: {e}", exc_info=True)
        return {} 