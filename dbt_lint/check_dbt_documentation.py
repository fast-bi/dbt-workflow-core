#!/usr/bin/env python3
import os
import json
import subprocess
import sys
import pandas as pd
from colorama import Fore, Style, init

# Initialize colorama for cross-platform colored output
init()

def log_and_print(message, level="INFO", color=None):
    """Print colored message for user and write structured log entry."""
    # Print colored message for user
    if color:
        print(f"{color}{message}{Style.RESET_ALL}")
    else:
        print(message)
    
    # Write structured log entry
    with open("dbt_documentation.log", "a") as f:
        f.write(f"{level}: {message}\n")

def find_model_paths():
    """Find all subdirectories at depth 1 inside the models folder."""
    try:
        result = subprocess.run(
            ["find", "./models", "-mindepth", "1", "-maxdepth", "1", "-type", "d"],
            capture_output=True, text=True, check=True
        )
        return sorted(result.stdout.strip().split('\n')) if result.stdout else []
    except subprocess.CalledProcessError as e:
        log_and_print(f"Finding model paths failed: {e}", "ERROR", Fore.RED)
        return []

def create_path_filters(model_paths):
    """Create path filter arguments for dbt-coverage command."""
    return ' '.join([f"--model-path-filter {path.replace('./', '')}" for path in model_paths if path])

def run_dbt_coverage(path_filters, threshold=0.9):
    """Run dbt-coverage and capture JSON output."""
    output_file = "documentation_coverage.json"
    
    try:
        log_and_print(f"Running dbt-coverage with filters: {path_filters}", "INFO", Fore.CYAN)
        cmd = f"dbt-coverage compute doc {path_filters} --cov-format markdown --cov-fail-under {threshold} --cov-report {output_file}"
        log_and_print(f"Running command: {cmd}", "INFO", Fore.CYAN)
        
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True,
                text=True,
                check=True
            )
            print(result.stdout)
            with open("dbt_documentation.log", "a") as f:
                f.write(result.stdout)
        except subprocess.CalledProcessError as e:
            print(e.stdout)
            with open("dbt_documentation.log", "a") as f:
                f.write(e.stdout)
            log_and_print(f"Documentation coverage is below the threshold of {threshold*100}%", "ERROR", Fore.RED)

        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                return json.load(f)
        else:
            log_and_print(f"Output file {output_file} not found", "ERROR", Fore.RED)
            return None
    except Exception as e:
        log_and_print(f"Running dbt-coverage failed: {e}", "ERROR", Fore.RED)
        return None

def analyze_coverage_data(coverage_data, threshold=0.9):
    """Analyze coverage data to generate a report."""
    if not coverage_data:
        log_and_print("No coverage data available to analyze", "ERROR", Fore.RED)
        return 1
    
    total_coverage = coverage_data.get('coverage', 0)
    covered = coverage_data.get('covered', 0)
    total = coverage_data.get('total', 0)
    
    log_and_print("\nDocumentation Coverage Summary", "STATUS", Fore.BLUE)
    log_and_print(f"Coverage: {covered}/{total} columns ({total_coverage*100:.1f}%)", "INFO", Fore.CYAN)
    
    models_needing_fixes = []
    
    for table in coverage_data.get('tables', []):
        name = table.get('name', '')
        coverage_pct = table.get('coverage', 0)
        
        if coverage_pct < 1.0:
            missing_cols = [
                col['name'] for col in table.get('columns', [])
                if col.get('coverage', 0) == 0
            ]
            
            if missing_cols:
                models_needing_fixes.append({
                    'model': name,
                    'coverage': coverage_pct * 100,
                    'missing_columns': missing_cols
                })
    
    models_needing_fixes.sort(key=lambda x: x['coverage'])
    
    if models_needing_fixes:
        log_and_print("\nModels Requiring Documentation Fixes", "STATUS", Fore.YELLOW)
        
        for item in models_needing_fixes:
            if item['coverage'] == 0:
                status = "CRITICAL"
                color = Fore.RED
            elif item['coverage'] < threshold * 100:
                status = "WARNING"
                color = Fore.YELLOW
            else:
                status = "MINOR"
                color = Fore.CYAN
                
            log_and_print(f"\n{status}: {item['model']} ({item['coverage']:.1f}%)", status, color)
            log_and_print("Missing documentation for columns:", "INFO")
            for col in item['missing_columns']:
                log_and_print(f"  - {col}", "INFO")
    
    if total_coverage < threshold:
        log_and_print(f"\nDocumentation coverage ({total_coverage*100:.1f}%) is below the threshold of {threshold*100}%", "ERROR", Fore.RED)
        return 1
    else:
        log_and_print(f"\nDocumentation coverage ({total_coverage*100:.1f}%) meets or exceeds the threshold of {threshold*100}%", "SUCCESS", Fore.GREEN)
        return 0

def main():
    # Create or clear the log file
    with open("dbt_documentation.log", "w") as f:
        f.write("")
    
    threshold = float(os.environ.get('DOC_COVERAGE_THRESHOLD', 0.9))
    
    model_paths = find_model_paths()
    if not model_paths:
        log_and_print("No model paths found", "ERROR", Fore.RED)
        return 1
    
    path_filters = create_path_filters(model_paths)
    coverage_data = run_dbt_coverage(path_filters, threshold)
    exit_code = analyze_coverage_data(coverage_data, threshold)
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()