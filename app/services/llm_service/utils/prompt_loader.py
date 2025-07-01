"""
Simple prompt loading utilities.
Simplified from class to functions for easier maintenance with small team.
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any
from app import config


def load_prompt(db_type: str) -> Dict[str, Any]:
    """Load and merge common and database-specific prompts"""
    try:
        # Construct the path to the new prompts directory
        app_base_path = Path(config['base_dirs']['app'])
        prompts_dir = app_base_path / 'config' / 'llm' / 'prompts'
        
        common_file = prompts_dir / 'common.yaml'
        db_file = prompts_dir / f'{db_type.lower()}.yaml'  # Ensure lowercase
        
        if not common_file.exists():
            raise FileNotFoundError(f"Common prompt file not found: {common_file}")
        
        # Load common prompts
        with open(common_file) as f:
            common_data = yaml.safe_load(f)
        
        # Load database-specific prompts if they exist
        if db_file.exists():
            with open(db_file) as f:
                db_data = yaml.safe_load(f)
                
            # Merge system prompts
            common_data['system'] = (
                common_data['system'] +
                "\n\n" +
                db_data['system']
            )
        
        return common_data
        
    except Exception as e:
        raise Exception(f"Error loading prompts for {db_type}: {str(e)}") 