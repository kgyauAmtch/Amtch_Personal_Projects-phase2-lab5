import boto3
import pandas as pd
from io import BytesIO, StringIO
import json
import os

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event, indent=2)}")
    
    try:
        s3 = boto3.client('s3')
        bucket = event['bucket']
        key = event['key']
        expected_headers_config = event['expected_headers']
        filename = os.path.basename(key)
        folder_path = os.path.dirname(key)
        
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        if not folder_path:
            folder_path = '/'
            
        print(f"Processing file: {filename} from folder: {folder_path}")
        
        # Find matching data type configuration
        expected_cols = None
        data_type_identifier = None
        sorted_prefixes = sorted(expected_headers_config.keys(), key=len, reverse=True)
        
        for path_prefix in sorted_prefixes:
            if key.startswith(path_prefix):
                expected_cols = expected_headers_config[path_prefix]
                data_type_identifier = path_prefix.strip('/').split('/')[-1]
                break
                
        print(f"Detected data type identifier: {data_type_identifier}")
        print(f"Expected columns for this type: {expected_cols}")
        
        if expected_cols is None:
            response = {
                "all_valid": True,
                "message": f"File {filename} in folder '{folder_path}' not mapped to any known data type for header validation, skipping."
            }
            print(f"Skipping validation. Response: {json.dumps(response, indent=2)}")
            return response
            
        print(f"Downloading file from s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        file_content = obj['Body'].read()
        file_extension = os.path.splitext(filename)[1].lower()
        
        # Read headers only based on file type
        if file_extension == '.csv':
            # Try UTF-8 first, fallback to latin-1 if needed
            try:
                content_str = file_content.decode('utf-8')
            except UnicodeDecodeError:
                print("UTF-8 decode failed, trying latin-1")
                content_str = file_content.decode('latin-1')
            
            df = pd.read_csv(StringIO(content_str), nrows=0)
            
        elif file_extension in ['.xlsx', '.xls']:
            df = pd.read_excel(BytesIO(file_content), nrows=0)
            
        else:
            raise ValueError(f"Unsupported file type extension: '{file_extension}' for file '{filename}'. Only .csv, .xlsx, and .xls are supported.")
        
        # Get actual headers and clean them (strip whitespace)
        actual_headers = [str(col).strip() for col in df.columns]
        expected_cols_clean = [str(col).strip() for col in expected_cols]
        
        # Perform validation
        is_valid = set(expected_cols_clean) == set(actual_headers)
        
        print(f"Actual headers: {actual_headers}")
        print(f"Expected headers (cleaned): {expected_cols_clean}")
        print(f"Validation result: {is_valid}")
        
        response = {
            "all_valid": is_valid,
            "validation_results": [{
                "filename": filename,
                "data_type": data_type_identifier,
                "valid": is_valid,
                "expected_headers": expected_cols_clean,
                "actual_headers": actual_headers,
                "missing_headers": list(set(expected_cols_clean) - set(actual_headers)),
                "extra_headers": list(set(actual_headers) - set(expected_cols_clean))
            }]
        }
        
        print(f"Final response: {json.dumps(response, indent=2)}")
        return response
        
    except Exception as e:
        print(f"Error occurred during validation for event: {json.dumps(event, indent=2)}")
        print(f"Error details: {str(e)}")
        
        error_response = {
            "all_valid": False,
            "validation_results": [{
                "filename": os.path.basename(event.get('key', 'unknown_file')),
                "data_type": "unknown",
                "valid": False,
                "error": str(e)
            }]
        }
        
        print(f"Error response: {json.dumps(error_response, indent=2)}")
        return error_response