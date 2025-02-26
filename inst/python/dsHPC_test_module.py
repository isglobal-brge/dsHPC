#!/usr/bin/env python
# Test module for dsHPC Python integration
"""
This is a simple Python module for testing the dsHPC Python integration functionality.
It provides basic functions that can be called from R to test the interface.
"""

import numpy as np
import json
import os
import sys
from datetime import datetime

def hello_world():
    """
    Simple function that returns a greeting message.
    
    Returns
    -------
    str
        A greeting message with Python version info.
    """
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    return f"Hello from Python {python_version}!"

def calculate_stats(numbers):
    """
    Calculate basic statistics for a list of numbers.
    
    Parameters
    ----------
    numbers : list or numpy.ndarray
        List of numbers to analyze.
    
    Returns
    -------
    dict
        Dictionary containing the statistics (mean, median, std, min, max).
    """
    # Convert to numpy array if it's not already
    arr = np.array(numbers, dtype=float)
    
    return {
        'mean': float(np.mean(arr)),
        'median': float(np.median(arr)),
        'std': float(np.std(arr)),
        'min': float(np.min(arr)),
        'max': float(np.max(arr)),
        'length': len(arr)
    }

def process_data_frame(df_dict):
    """
    Process a data frame that's been passed as a dictionary.
    
    Parameters
    ----------
    df_dict : dict
        Dictionary representation of a data frame with column names as keys.
    
    Returns
    -------
    dict
        Results of processing the data frame.
    """
    # Convert the dictionary to a structured representation
    columns = list(df_dict.keys())
    n_rows = len(df_dict[columns[0]]) if columns else 0
    
    # Simple summary of the data frame
    summary = {
        'columns': columns,
        'n_rows': n_rows,
        'column_types': {col: type(df_dict[col][0]).__name__ for col in columns if len(df_dict[col]) > 0},
        'column_summary': {}
    }
    
    # Calculate summary statistics for numeric columns
    for col in columns:
        values = df_dict[col]
        try:
            # Try to convert to numeric array
            arr = np.array(values, dtype=float)
            summary['column_summary'][col] = {
                'mean': float(np.mean(arr)),
                'median': float(np.median(arr)),
                'std': float(np.std(arr)) if n_rows > 1 else 0.0
            }
        except (ValueError, TypeError):
            # For non-numeric columns, just count unique values
            if values and all(isinstance(v, str) for v in values):
                unique_values = set(values)
                summary['column_summary'][col] = {
                    'unique_count': len(unique_values),
                    'first_value': values[0] if values else None
                }
    
    return summary

def get_system_info():
    """
    Get information about the Python environment and system.
    
    Returns
    -------
    dict
        Dictionary with system information.
    """
    import platform
    
    return {
        'python_version': sys.version,
        'platform': platform.platform(),
        'timestamp': datetime.now().isoformat(),
        'cpu_count': os.cpu_count(),
        'working_directory': os.getcwd(),
        'environment_vars': {k: v for k, v in os.environ.items() if not k.startswith('_')}
    }

def handle_complex_data(data):
    """
    Handle and process complex nested data structures.
    
    Parameters
    ----------
    data : dict
        A complex nested data structure.
    
    Returns
    -------
    dict
        Processed data with additional information.
    """
    # Create a deep copy to avoid modifying the input
    result = dict(data)
    
    # Add some processing information
    result['_processed'] = True
    result['_timestamp'] = datetime.now().isoformat()
    
    # Try to extract some statistics if possible
    try:
        if 'values' in data and isinstance(data['values'], list):
            values = np.array(data['values'], dtype=float)
            result['_stats'] = {
                'mean': float(np.mean(values)),
                'sum': float(np.sum(values)),
                'length': len(values)
            }
    except (ValueError, TypeError):
        # Not numeric data, can't compute stats
        pass
    
    # Try to serialize to JSON and back as a compatibility test
    try:
        json_str = json.dumps(result)
        result['_json_serializable'] = True
        result['_json_length'] = len(json_str)
    except (TypeError, ValueError):
        result['_json_serializable'] = False
    
    return result 