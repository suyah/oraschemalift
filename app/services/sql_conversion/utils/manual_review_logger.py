"""
Manual Review Logger - Tracks items requiring manual conversion attention
Creates a separate, easily accessible log file for conversion items requiring manual review.
"""
import os
import json
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path


class ManualReviewLogger:
    """Handles logging of manual review items to a dedicated file."""
    
    def __init__(self, output_dir: str, logger=None):
        self.output_dir = output_dir
        self.logger = logger
        self.review_items = []
        self.log_file_path = None
        
    def log_manual_review_item(self, 
                              file_path: str, 
                              object_name: str, 
                              issue_type: str, 
                              message: str, 
                              severity: str = 'WARNING',
                              suggested_action: Optional[str] = None,
                              line_number: Optional[int] = None):
        """Log an item that requires manual review."""
        
        review_item = {
            'timestamp': datetime.now().isoformat(),
            'file_path': file_path,
            'object_name': object_name,
            'object_type': self._detect_object_type(object_name),
            'issue_type': issue_type,
            'severity': severity,
            'message': message,
            'suggested_action': suggested_action,
            'line_number': line_number,
            'status': 'PENDING_REVIEW'
        }
        
        self.review_items.append(review_item)
        
        # Also log to main logger if available
        if self.logger:
            log_msg = f"MANUAL REVIEW [{severity}] {file_path}::{object_name} - {issue_type}: {message}"
            if severity == 'ERROR':
                self.logger.error(log_msg)
            else:
                self.logger.warning(log_msg)
    
    def write_manual_review_log(self) -> str:
        """Write all manual review items to a dedicated log file."""
        if not self.review_items:
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"manual_review_required_{timestamp}.json"
        self.log_file_path = os.path.join(self.output_dir, log_filename)
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        summary_data = {
            'conversion_timestamp': timestamp,
            'total_items_requiring_review': len(self.review_items),
            'summary_by_type': self._create_summary_by_type(),
            'summary_by_severity': self._create_summary_by_severity(),
            'summary_by_file': self._create_summary_by_file(),
            'review_items': self.review_items,
            'instructions': {
                'overview': 'This file contains all conversion items that require manual review and cannot be automatically converted.',
                'next_steps': [
                    '1. Review each item in the review_items section',
                    '2. For each item, check the suggested_action if provided',
                    '3. Manually convert the identified patterns in your source files',
                    '4. Update the status field to COMPLETED when done',
                    '5. Re-run conversion if needed'
                ],
                'severity_levels': {
                    'ERROR': 'Critical issues that will prevent compilation/execution',
                    'WARNING': 'Issues that may cause runtime problems or performance degradation',
                    'INFO': 'Best practice recommendations or potential improvements'
                }
            }
        }
        
        try:
            with open(self.log_file_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            
            if self.logger:
                self.logger.info(f"Manual review log written to: {self.log_file_path}")
                self.logger.info(f"Total items requiring manual review: {len(self.review_items)}")
                
            return self.log_file_path
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error writing manual review log: {e}")
            return None
    
    def create_summary_report(self) -> str:
        """Create a human-readable summary report."""
        if not self.review_items:
            return "No manual review items found."
        
        report_lines = [
            "=" * 80,
            "MANUAL REVIEW REQUIRED - CONVERSION SUMMARY",
            "=" * 80,
            f"Total Items Requiring Review: {len(self.review_items)}",
            ""
        ]
        
        severity_summary = self._create_summary_by_severity()
        report_lines.extend([
            "BY SEVERITY:",
            *[f"  {severity}: {count} items" for severity, count in severity_summary.items()],
            ""
        ])
        
        type_summary = self._create_summary_by_type()
        report_lines.extend([
            "BY ISSUE TYPE:",
            *[f"  {issue_type}: {count} items" for issue_type, count in type_summary.items()],
            ""
        ])
        
        file_summary = self._create_summary_by_file()
        report_lines.extend([
            "BY FILE:",
            *[f"  {file_path}: {count} items" for file_path, count in file_summary.items()],
            ""
        ])
        
        high_priority = [item for item in self.review_items if item['severity'] == 'ERROR']
        if high_priority:
            report_lines.extend([
                "HIGH PRIORITY ITEMS (ERRORS):",
                *[f"  - {item['file_path']}::{item['object_name']} - {item['message']}" 
                  for item in high_priority],
                ""
            ])
        
        report_lines.extend([
            f"Detailed log available at: {self.log_file_path}",
            "=" * 80
        ])
        
        return "\n".join(report_lines)
    
    def _create_summary_by_type(self) -> Dict[str, int]:
        """Create summary grouped by issue type."""
        summary = {}
        for item in self.review_items:
            issue_type = item['issue_type']
            summary[issue_type] = summary.get(issue_type, 0) + 1
        return dict(sorted(summary.items(), key=lambda x: x[1], reverse=True))
    
    def _create_summary_by_severity(self) -> Dict[str, int]:
        """Create summary grouped by severity."""
        summary = {}
        for item in self.review_items:
            severity = item['severity']
            summary[severity] = summary.get(severity, 0) + 1
        return summary
    
    def _create_summary_by_file(self) -> Dict[str, int]:
        """Create summary grouped by file."""
        summary = {}
        for item in self.review_items:
            file_path = item['file_path']
            summary[file_path] = summary.get(file_path, 0) + 1
        return dict(sorted(summary.items(), key=lambda x: x[1], reverse=True))
    
    def _detect_object_type(self, object_name: str) -> str:
        """Detect object type from name patterns."""
        name_lower = object_name.lower()
        if any(keyword in name_lower for keyword in ['function', 'func']):
            return 'FUNCTION'
        elif any(keyword in name_lower for keyword in ['procedure', 'proc']):
            return 'PROCEDURE'
        elif any(keyword in name_lower for keyword in ['table', 'tbl']):
            return 'TABLE'
        else:
            return 'UNKNOWN'


# Predefined issue types and suggested actions
MANUAL_REVIEW_PATTERNS = {
    'UPDATE_FROM_syntax': {
        'severity': 'ERROR',
        'suggested_action': 'Convert UPDATE...FROM to Oracle MERGE statement or correlated subquery'
    },
    'LATERAL_FLATTEN': {
        'severity': 'WARNING', 
        'suggested_action': 'Replace LATERAL FLATTEN with Oracle JSON_TABLE or XMLTABLE functions'
    },
    'QUALIFY_clause': {
        'severity': 'WARNING',
        'suggested_action': 'Replace QUALIFY with nested query using ROW_NUMBER() in WHERE clause'
    },
    'Dynamic_SQL': {
        'severity': 'WARNING',
        'suggested_action': 'Review EXECUTE IMMEDIATE statements for Oracle syntax compatibility'
    },
    'Complex_data_types': {
        'severity': 'INFO',
        'suggested_action': 'Review complex data types (ARRAY, VARIANT, etc.) for Oracle equivalents'
    },
    'External_language': {
        'severity': 'ERROR',
        'suggested_action': 'Replace external language functions (JavaScript, Python) with PL/SQL or Java'
    }
} 