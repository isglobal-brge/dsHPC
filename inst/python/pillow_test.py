#!/usr/bin/env python3
"""
Test module for Python integration in dsHPC using PIL/Pillow.

This module provides basic image processing functions using the PIL/Pillow library.
It's designed for testing the R-Python integration in the dsHPC package.
"""

import os
import numpy as np
from PIL import Image, ImageOps, ImageFilter

def get_image_info(image_path):
    """
    Get basic information about an image file.
    
    Parameters
    ----------
    image_path : str
        Path to the image file
        
    Returns
    -------
    dict
        A dictionary containing basic image information
    """
    try:
        img = Image.open(image_path)
        info = {
            'width': img.width,
            'height': img.height,
            'format': img.format,
            'mode': img.mode,
            'file_size': os.path.getsize(image_path)
        }
        return info
    except Exception as e:
        raise ValueError(f"Error processing image at {image_path}: {str(e)}")

def apply_filter(image_path, filter_type="blur", output_path=None):
    """
    Apply a filter to an image.
    
    Parameters
    ----------
    image_path : str
        Path to the input image file
    filter_type : str, optional
        Type of filter to apply. Options: "blur", "sharpen", "contour", "grayscale"
    output_path : str, optional
        Path to save the processed image. If None, the image is not saved.
        
    Returns
    -------
    dict
        A dictionary containing the processed image data and metadata
    """
    try:
        img = Image.open(image_path)
        
        # Apply the selected filter
        if filter_type == "blur":
            processed = img.filter(ImageFilter.BLUR)
        elif filter_type == "sharpen":
            processed = img.filter(ImageFilter.SHARPEN)
        elif filter_type == "contour":
            processed = img.filter(ImageFilter.CONTOUR)
        elif filter_type == "grayscale":
            processed = ImageOps.grayscale(img)
        else:
            raise ValueError(f"Unknown filter type: {filter_type}")
        
        # Save the processed image if output_path is provided
        if output_path:
            processed.save(output_path)
        
        # Convert to numpy array for easier handling in R
        img_array = np.array(processed)
        
        result = {
            'width': processed.width,
            'height': processed.height,
            'mode': processed.mode,
            'filter_type': filter_type,
            'array_shape': img_array.shape,
            'array_mean': float(np.mean(img_array)),
            'array_std': float(np.std(img_array))
        }
        
        if output_path:
            result['output_path'] = output_path
            
        return result
    except Exception as e:
        raise ValueError(f"Error applying filter to image at {image_path}: {str(e)}")

def create_test_image(output_path, width=100, height=100, color=(255, 0, 0)):
    """
    Create a simple test image with a specified color.
    
    Parameters
    ----------
    output_path : str
        Path to save the created image
    width : int, optional
        Width of the image in pixels
    height : int, optional
        Height of the image in pixels
    color : tuple or list or int, optional
        RGB color tuple/list for the image or a single int for grayscale
        
    Returns
    -------
    dict
        A dictionary containing information about the created image
    """
    try:
        # Ensure color is in the correct format (tuple of 3 integers)
        if isinstance(color, (list, tuple)) and len(color) == 3:
            color = tuple(int(c) for c in color)
        elif isinstance(color, int):
            color = int(color)  # For grayscale
        else:
            raise ValueError(f"Invalid color format: {color}. Must be RGB tuple/list or single int.")
        
        # Create a new image with the specified color
        img = Image.new('RGB', (width, height), color)
        
        # Save the image
        img.save(output_path)
        
        result = {
            'width': width,
            'height': height,
            'color': color,
            'format': output_path.split('.')[-1].upper(),
            'path': output_path,
            'file_size': os.path.getsize(output_path)
        }
        
        return result
    except Exception as e:
        raise ValueError(f"Error creating test image at {output_path}: {str(e)}") 