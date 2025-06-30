import boto3
import pandas as pd
from io import BytesIO, StringIO
import json
import os # for path manipulation

def lambda_handler(event, context):
    # Debug: Print the incoming event
    print(f"Received event: {json.dumps(event, indent=2)}")

    try:
        s3 = boto3.client('s3')

        # Extract bucket, key (full S3 path), and expected headers configuration
        bucket = event['bucket']
        key = event['key'] # key will now include the folder path, e.g., "raw_landing_zone/orders/orders_apr_2025.xlsx"
        expected_headers_config = event['expected_headers'] # Renamed for clarity within the function

        filename = os.path.basename(key) # Extracts just the filename: "orders_apr_2025.xlsx"
        # Get the directory path, ensuring it ends with a '/' for consistent matching with prefixes
        folder_path = os.path.dirname(key)
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
        # Handle root level files if any (though typically not for this use case)
        if not folder_path:
            folder_path = '/' # Represent root if the key is just a filename

        print(f"Processing file: {filename} from folder: {folder_path}")

        expected_cols = None
        data_type_identifier = None # Identifier for the type of data (e.g., 'orders', 'order_items')
        
        # Sort keys to ensure more specific paths are checked first, e.g., "a/b/" before "a/"
        sorted_prefixes = sorted(expected_headers_config.keys(), key=len, reverse=True)

        for path_prefix in sorted_prefixes:
            if key.startswith(path_prefix):
                expected_cols = expected_headers_config[path_prefix]
                # Derive a meaningful identifier for the data type from the path prefix
                # Example: "raw_landing_zone/orders/" -> "orders"
                data_type_identifier = path_prefix.strip('/').split('/')[-1]
                break # Found a match, no need to check further

        print(f"Detected data type identifier: {data_type_identifier}")
        print(f"Expected columns for this type: {expected_cols}")

        # Skip validation if no matching folder prefix was found in the configuration
        if expected_cols is None:
            response = {
                "all_valid": True, # Set to True to allow unmapped files to pass validation if desired
                "message": f"File {filename} in folder '{folder_path}' not mapped to any known data type for header validation, skipping."
            }
            print(f"Skipping validation. Response: {json.dumps(response, indent=2)}")
            return response

        # Download file from S3
        print(f"Downloading file from s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)

        # Read file into DataFrame based on its extension
        file_extension = os.path.splitext(filename)[1].lower()

        if file_extension == '.csv':
            # Ensure proper encoding for CSV files
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        elif file_extension == '.xlsx':
            df = pd.read_excel(BytesIO(obj['Body'].read()))
        else:
            raise ValueError(f"Unsupported file type extension: '{file_extension}' for file '{filename}'. Only .csv and .xlsx are supported.")

        actual_headers = list(df.columns)
        is_valid = set(expected_cols) == set(actual_headers) # Checks for exact match of header names, order-independent

        print(f"Actual headers: {actual_headers}")
        print(f"Validation result: {is_valid}")

        # Prepare the response payload
        response = {
            "all_valid": is_valid,
            "validation_results": [{
                "filename": filename,
                "data_type": data_type_identifier, # Changed from 'file_type'
                "valid": is_valid,
                "expected_headers": expected_cols,
                "actual_headers": actual_headers,
                "missing_headers": list(set(expected_cols) - set(actual_headers)),
                "extra_headers": list(set(actual_headers) - set(expected_cols))
            }]
        }

        print(f"Final response: {json.dumps(response, indent=2)}")
        return response

    except Exception as e:
        print(f"Error occurred during validation for event: {json.dumps(event, indent=2)}")
        print(f"Error details: {str(e)}")

        # Construct an error response payload
        error_response = {
            "all_valid": False,
            "validation_results": [{
                "filename": os.path.basename(event.get('key', 'unknown_file')),
                "data_type": "unknown", # Changed from 'file_type'
                "valid": False,
                "error": str(e)
            }]
        }
        print(f"Error response: {json.dumps(error_response, indent=2)}")
        return error_response