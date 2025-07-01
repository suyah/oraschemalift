from langchain_community.chat_models import ChatOCIGenAI
import json
import os
import yaml
from pathlib import Path, PurePath
from datetime import datetime
from app import config
import re
from .utils.prompt_loader import load_prompt
from app.utils.logger import setup_logger
from typing import Dict, Optional, List, Any
from app.utils.file_utils import find_sql_files, setup_output_directory, create_processing_stats, make_relative_path, ensure_directory_exists, read_file_content, write_file_content
from langchain_core.prompts import PromptTemplate
from langchain.chains import LLMChain
from app.utils.path_utils import create_samples_run_dirs

class LLMService:
    """Handles LLM interactions and response parsing"""
    
    def __init__(self):
        """Initialize LLM service"""
        self.logger = setup_logger('app.services.llm_service')
        self.config = config
        self.llm = None
        self._initialization_logged = False  # Prevent duplicate log spam on reloads
        self._initialize_llm()

    def _check_oci_config(self):
        """Check if OCI configuration is available"""
        
        self.oci_compartment_id = os.getenv('OCI_COMPARTMENT_ID')

    def _initialize_llm(self):
        """Initialize the LLM with OCI configuration"""
        try:
            compartment_id = os.environ.get('OCI_COMPARTMENT_ID')
            if not compartment_id:
                if not self._initialization_logged:
                    self.logger.warning("No OCI compartment ID configured. LLM features will be unavailable.")
                    self._initialization_logged = True
                return

            config_model_id = self.config.get('llm', {}).get('model_id')
            model_id = config_model_id or os.environ.get('OCI_MODEL_ID', 'cohere.command-a-03-2025')
            
            service_endpoint = os.environ.get('OCI_SERVICE_ENDPOINT', 'https://inference.generativeai.us-chicago-1.oci.oraclecloud.com')
            max_tokens = int(os.environ.get('OCI_MAX_TOKENS', '4000'))
            temperature = float(os.environ.get('OCI_TEMPERATURE', '0.1'))

            if not self._initialization_logged:
                self.logger.info(f"Using model from config: {model_id}")

            self.llm = ChatOCIGenAI(
                model_id=model_id,
                service_endpoint=service_endpoint,
                compartment_id=compartment_id,
                model_kwargs={"max_tokens": max_tokens, "temperature": temperature}
            )
            
            if not self._initialization_logged:
                self.logger.info(f"LLM service initialized successfully with model: {model_id}")
                self._initialization_logged = True
            
        except Exception as e:
            if not self._initialization_logged:
                self.logger.error(f"Failed to initialize LLM: {e}")
                self._initialization_logged = True
            self.llm = None

    def get_available_prompts(self) -> Dict[str, str]:
        """Get list of available prompts with descriptions"""
        prompts = {}
        try:
            prompts_dir = os.path.join(self.config['base_dirs']['app'], 'config', 'llm', 'prompts')
            for file_path in Path(prompts_dir).glob("*.yaml"):
                if file_path.name != 'common.yaml':  # Skip common.yaml as it's merged with others
                    db_type = file_path.stem
                    prompts[db_type] = f"{db_type.title()} database prompt"
        except Exception as e:
            self.logger.error(f"Error loading available prompts: {e}")
        return prompts

    def _load_prompt_template(self, prompt_filename: str) -> Optional[str]:
        """Load a prompt template from file"""
        try:
            prompts_dir = os.path.join(self.config['base_dirs']['app'], 'config', 'llm', 'prompts')
            prompt_path = os.path.join(prompts_dir, prompt_filename)
            
            return read_file_content(prompt_path)
        except Exception as e:
            self.logger.error(f"Error loading prompt template {prompt_filename}: {e}")
            return None

    def _save_raw_response(self, response_text: str, run_base_dir: str):
        """Save raw LLM response exactly as received into the llm_logs subfolder of the run base directory."""
        llm_logs_dir = os.path.join(run_base_dir, 'llm_logs')
        ensure_directory_exists(llm_logs_dir)
        raw_file = os.path.join(llm_logs_dir, 'raw_llm_response.md')
        try:
            write_file_content(raw_file, response_text)
            self.logger.info(f"Saved raw LLM response to: {raw_file}")
        except Exception as e:
            self.logger.error(f"Failed to save raw LLM response to {raw_file}: {e}")

    def _create_testdata_run_dirs(self, source_type: str) -> tuple[str, str]:
        """Create timestamped directory structure for a testdata run 
           and returns (run_base_dir, actual_scripts_source_dir).
           run_base_dir is .../<timestamp>/
           actual_scripts_source_dir is .../<timestamp>/<scripts_parent_folder_name>/source/
        """
        run_base_dir, source_dir = create_samples_run_dirs(source_type)
        self.logger.info(f"Created samples run directory: {run_base_dir}")
        return str(run_base_dir), str(source_dir)

    def _save_llm_log(self, log_dir: str, prompt: str, response: str, metadata: Dict) -> str:
        """Save LLM interaction to log file in consolidated log directory"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
            log_filename = f"llm_interaction_{timestamp}.json"
            log_filepath = os.path.join(log_dir, log_filename)
            
            log_data = {
                "timestamp": datetime.now().isoformat(),
                "prompt": prompt,
                "response": response,
                "metadata": metadata
            }
            
            content = json.dumps(log_data, indent=2, ensure_ascii=False)
            write_file_content(log_filepath, content)
            
            self.logger.debug(f"LLM log saved: {log_filepath}")
            return log_filepath
            
        except Exception as e:
            self.logger.error(f"Error saving LLM log: {e}")
            return ""

    def generate_schema(self, source_type: str, table_count: int, oracle_version: str = "19c") -> tuple[str, str]:
        """Generate schema definition from LLM.
        
        Returns:
            tuple: (raw_llm_response, actual_scripts_source_dir) 
            actual_scripts_source_dir is .../<timestamp>/<scripts_parent_folder_name>/source/.
        """
        try:
            if self.llm is None:
                raise ValueError("LLM service is not properly configured. Please check your OCI credentials and model availability.")
            
            prompt_data = load_prompt(source_type.lower())
            
            run_base_dir, actual_scripts_source_dir = self._create_testdata_run_dirs(source_type)
            
            data_types_path = os.path.join(
                self.config['base_dirs']['app'], 
                'config', 'llm', 'rdbms',
                f'{source_type.lower()}/data_types.json'
            )
            content = read_file_content(data_types_path)
            if content:
                data_types = json.loads(content)
            else:
                raise FileNotFoundError(f"Could not read data types file: {data_types_path}")
            
            system_prompt = prompt_data['system'].format(
                source_type=source_type,
                table_count=table_count,
                data_types=json.dumps(data_types, indent=2)
            )
            
            self.logger.debug("\nFinal prompt sent to LLM:\n------------------------\n" + system_prompt + "\n------------------------\n")
            
            messages = [
                {"role": "system", "content": system_prompt}
            ]
            
            self.logger.info("Calling LLM...")
            response = self.llm.invoke(messages)
            self.logger.info("LLM response received")
            
            self._save_raw_response(response.content, run_base_dir)
            
            return response.content, actual_scripts_source_dir
            
        except FileNotFoundError as e:
            error_msg = f"Data types file not found for {source_type}: {e}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Error generating schema: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            if "Entity with key" in str(e) and "not found" in str(e):
                error_msg = f"LLM model not found. Please check if the model is available in your OCI region. Original error: {str(e)}"
            elif "compartment" in str(e).lower():
                error_msg = f"OCI compartment configuration issue. Please check your OCI_COMPARTMENT_ID. Original error: {str(e)}"
            elif "404" in str(e):
                error_msg = f"OCI service endpoint or model not found. Please check your configuration. Original error: {str(e)}"
            
            raise ValueError(error_msg)

    def _parse_json_response(self, response_text: str) -> dict:
        """Parse and validate LLM response"""
        try:
            if isinstance(response_text, bytes):
                response_text = response_text.decode('utf-8')
            
            if "```json" in response_text:
                pattern = r"```json\\n(.*?)\\n```"
                match = re.search(pattern, response_text, re.DOTALL)
                if match:
                    response_text = match.group(1).strip()

            response_text = re.sub(r',\\s*]', ']', response_text)
            response_text = re.sub(r',\\s*}', '}', response_text)

            try:
                return json.loads(response_text)
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON parsing error: {str(e)}\nResponse text after cleanup:\n{response_text}")
                raise
            
        except Exception as e:
            self.logger.error(f"Failed to parse response: {str(e)}\nRaw response:\n{response_text}", exc_info=True)
            raise ValueError(f"Invalid JSON response: {str(e)}")

    def _get_enabled_objects(self, config: dict) -> dict:
        """Get only enabled objects from config"""
        def filter_enabled(obj):
            if isinstance(obj, dict):
                if 'enabled' in obj and obj['enabled']:
                    if 'objects' in obj:
                        return {k: v for k, v in obj['objects'].items() if v.get('enabled', False)}
                    return obj
                return {k: filter_enabled(v) for k, v in obj.items() if filter_enabled(v)}
            return obj

        return filter_enabled(config)

    def convert_sql_with_prompt(self, input_path: str, source_type: str,
                               target_type: str, prompt_filename: str,
                               target_version: Optional[str] = None,
                               original_run_timestamp: Optional[str] = None) -> Dict:
        """Converts SQL files using LLM with a specified prompt file"""
        self.logger.info(f"Starting LLM conversion: {source_type} -> {target_type}")
        
        stats = create_processing_stats()
        
        if not self.oci_compartment_id:
            return {
                "status": "error", 
                "message": "No OCI_COMPARTMENT_ID configured",
                "stats": stats
            }
        
        prompt_template = self._load_prompt_template(prompt_filename)
        if not prompt_template:
            return {
                "status": "error",
                "message": f"Could not load prompt template: {prompt_filename}",
                "stats": stats
            }
        
        source_dir, output_dir, log_dir = setup_output_directory(input_path)
        
        sql_files = find_sql_files(source_dir)
        stats['total_files'] = len(sql_files)
        
        if not sql_files:
            return {
                "status": "completed", 
                "message": "No SQL files found to process",
                "stats": stats,
                "converted_output_dir": None,
                "converted_files": [],
                "conversion_log_directory": None,
                "original_run_timestamp": original_run_timestamp,
                "source_db_type": source_type
            }
        
        results = self._process_files_with_llm(
            sql_files, source_dir, output_dir, source_type, target_type,
            prompt_template, target_version, stats
        )
        
        return self._build_llm_response(
            results, stats, output_dir, log_dir,
            original_run_timestamp, source_type
        )

    def _process_files_with_llm(self, sql_files: List[str], source_dir: str, 
                               output_dir: str, source_type: str, target_type: str,
                               prompt_template: str, target_version: Optional[str],
                               stats: Dict) -> List[Dict]:
        """Process all files using LLM"""
        results = []
        
        for sql_file in sql_files:
            result = self._process_single_file_with_llm(
                sql_file, source_dir, output_dir, source_type, target_type,
                prompt_template, target_version, stats
            )
            results.append(result)
            
        return results

    def _process_single_file_with_llm(self, sql_file: str, source_dir: str,
                                     output_dir: str, source_type: str, target_type: str,
                                     prompt_template: str, target_version: Optional[str],
                                     stats: Dict) -> Dict:
        """Process a single file with LLM"""
        self.logger.info(f"Processing with LLM: {sql_file}")
        
        try:
            content = read_file_content(sql_file)
            if not content:
                stats['failed'] += 1
                return self._create_llm_error_result(sql_file, "Could not read file")
            
            prompt = self._create_prompt(content, source_type, target_type, 
                                       prompt_template, target_version)
            
            converted_content = self._call_llm_api(prompt)
            if not converted_content:
                stats['failed'] += 1
                return self._create_llm_error_result(sql_file, "LLM API call failed")
            
            output_file = self._get_output_path(sql_file, source_dir, output_dir)
            self._write_llm_output(output_file, converted_content)
            
            stats['converted'] += 1
            return {
                "file": sql_file,
                "status": "success",
                "message": "Converted with LLM",
                "output_file": output_file
            }
            
        except Exception as e:
            self.logger.error(f"Error processing {sql_file} with LLM: {e}")
            stats['failed'] += 1
            return self._create_llm_error_result(sql_file, str(e))

    def _create_prompt(self, content: str, source_type: str, target_type: str,
                      template: str, target_version: Optional[str]) -> str:
        """Create LLM prompt from template"""
        prompt = template.replace("{source_dialect}", source_type)
        prompt = prompt.replace("{target_dialect}", target_type)
        prompt = prompt.replace("{sql_content}", content)
        
        if target_version:
            prompt = prompt.replace("{target_version}", target_version)
            
        return prompt

    def _call_llm_api(self, prompt: str) -> Optional[str]:
        """Call OpenAI API (placeholder for actual implementation)"""
        # This would contain the actual LLM API call
        # For now, just return a placeholder
        self.logger.warning("LLM API call not implemented - returning placeholder")
        return f"-- Converted SQL (placeholder)\n{prompt[:100]}..."

    def _write_llm_output(self, output_file: str, content: str):
        """Write LLM output to file"""
        ensure_directory_exists(os.path.dirname(output_file))
        write_file_content(output_file, content)

    def _get_output_path(self, sql_file: str, source_dir: str, output_dir: str) -> str:
        """Get output file path maintaining directory structure"""
        rel_path = make_relative_path(sql_file, source_dir)
        return os.path.join(output_dir, rel_path)

    def _create_llm_error_result(self, file_path: str, message: str) -> Dict:
        """Create error result for LLM processing"""
        return {
            "file": file_path,
            "status": "error",
            "message": message,
            "output_file": None
        }

    def _build_llm_response(self, results: List[Dict], stats: Dict, output_dir: str,
                           log_dir: str, original_run_timestamp: Optional[str],
                           source_type: str) -> Dict:
        """Build final LLM response"""
        workspace_base = self.config['base_dirs']['workspace']
        
        if stats.get('failed', 0) > 0:
            status = "error"
            message = f"LLM conversion completed with {stats['failed']} failed file(s)"
        else:
            status = "completed"
            message = "LLM conversion completed successfully"
        
        converted_files = []
        for result in results:
            if (result.get("status") == "success" and 
                result.get("output_file") and 
                os.path.exists(result["output_file"])):
                rel_path = make_relative_path(result["output_file"], workspace_base)
                converted_files.append(rel_path)
        
        return {
            "status": status,
            "message": message,
            "converted_output_dir": make_relative_path(output_dir, workspace_base),
            "converted_files": converted_files,
            "conversion_log_directory": make_relative_path(log_dir, workspace_base),
            "original_run_timestamp": original_run_timestamp,
            "source_db_type": source_type,
            "stats": stats
        } 