from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import boto3
import os
import subprocess
import re
from pathlib import Path

def parse_pcap_filename(filename):
    """Parse PCAP filename to extract venue, dateint, and colocation"""
    base_name = filename.replace('.pcap.zst', '').replace('.pcap', '')
    
    # Pattern: EXCHANGE.VENUE.LETTER-DATEINT.COLO
    pattern = r'([A-Z]+)\.([A-Z]+)\.([A-Z])-(\d{8})\.([A-Z0-9]+)'
    match = re.match(pattern, base_name)
    
    if not match:
        raise ValueError(f"Could not parse filename: {filename}")
    
    exchange, venue, letter, dateint, colo = match.groups()
    
    return {
        'exchange': exchange,
        'venue': venue,
        'dateint': dateint,
        'colo': colo
    }

def download_pcap_from_s3():
    """Download PCAP file from S3 to local processing directory"""
    try:
        s3 = boto3.client('s3')
        
        # Configuration
        bucket_name = 'vir-airflow'
        s3_input_path = 'BATS.BZX.A-20220801.NY4.pcap.zst'
        local_path = '/tmp/' + s3_input_path
        
        # Download file
        s3.download_file(bucket_name, s3_input_path, local_path)
                
        # Parse filename to extract parameters
        filename = Path(s3_input_path).name
        parsed_params = parse_pcap_filename(filename)
        
        # Store file info in XCom
        return {
            'input_file': local_path,
            'bucket_name': bucket_name,
            'parsed_params': parsed_params
        }
        
    except Exception as e:
        raise Exception(f"Download failed: {e}")

def process_pcap_file(**context):
    """Run PcapToRef on the downloaded PCAP file with parsed parameters"""
    try:
        # Get file info from previous task
        file_info = context['task_instance'].xcom_pull(task_ids='download_pcap')
        parsed_params = file_info['parsed_params']

        # Define output directory
        output_dir = '/tmp/' + str(parsed_params['dateint']) + '/' + parsed_params['venue'] + '/'
        os.makedirs(output_dir, exist_ok=True)
        
        # Build command
        cmd = [
            '/home/airflow/PcapToRef',
            '-o', output_dir,
            '-v', parsed_params['venue'],
            '-d', str(parsed_params['dateint']),
            '-c', parsed_params['colo'],
            '-s', '0',
            '-e', '23'
        ]
                
        # Execute the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=None,
        )
        
        if result.returncode != 0:
            raise Exception(f"PcapToRef execution failed: {result.stderr}")
                
        # Return output info for next task
        return {
            'output_dir': output_dir,
            'venue': parsed_params['venue'],
            'dateint': parsed_params['dateint']
        }
        
    except Exception as e:
        raise Exception(f"Processing failed: {e}")

def upload_results_to_s3_sync(**context):
    """Upload entire output directory using AWS CLI sync"""
    try:
        # Get info from previous tasks
        file_info = context['task_instance'].xcom_pull(task_ids='download_pcap')
        process_info = context['task_instance'].xcom_pull(task_ids='process_pcap')
        
        bucket_name = file_info['bucket_name']
        output_dir = process_info['output_dir']
        input_file = file_info['input_file']

        # Create S3 destination path
        s3_dest = f"s3://{bucket_name}/output/{process_info['venue']}/{process_info['dateint']}/"
        
        # Use AWS CLI sync to upload entire directory
        sync_cmd = [
            'aws', 's3', 'sync', 
            output_dir, 
            s3_dest
        ]
        
        result = subprocess.run(sync_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"AWS S3 sync failed: {result.stderr}")
                
        # Count uploaded files
        file_count = sum(len(files) for _, _, files in os.walk(output_dir))
        
        # Clean up local directory
        import shutil
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        
        # Remove input PCAP file
        if os.path.exists(input_file):
            os.remove(input_file)
        
        return {
            'total_files_uploaded': file_count,
            's3_base_path': s3_dest,
            'sync_output': result.stdout
        }
        
    except Exception as e:
        raise Exception(f"Upload failed: {e}")

def cleanup_on_failure(context):
    """Clean up local files if processing fails"""
    try:
        # Clean up input file
        input_file = context['task_instance'].xcom_pull(task_ids='download_pcap', key='input_file')
        
        files_to_clean = []
        if input_file:
            files_to_clean.append(input_file)

        # Clean up any dateint/venue directories that might exist
        if os.path.exists('/tmp'):
            for item in os.listdir('/tmp'):
                item_path = os.path.join('/tmp', item)
                if os.path.isdir(item_path) and item.isdigit() and len(item) == 8:
                    files_to_clean.append(item_path)

        # Clean up all identified files/directories
        for file_path in files_to_clean:
            if file_path and os.path.exists(file_path):
                if os.path.isdir(file_path):
                    import shutil
                    shutil.rmtree(file_path)
                else:
                    os.remove(file_path)
    except Exception as e:
        print(f"Cleanup warning: {e}")

# DAG definition
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': cleanup_on_failure
}

dag = DAG(
    'pcap_to_ref_processing',
    default_args=default_args,
    description='Process PCAP files with PcapToRef and store results in S3',
    schedule=None,
    catchup=False,
    tags=['pcap', 'market-data', 's3']
)

# Task definitions
download_task = PythonOperator(
    task_id='download_pcap',
    python_callable=download_pcap_from_s3,
    dag=dag
)

check_resources_task = BashOperator(
    task_id='check_system_resources',
    bash_command='''
    echo "=== System Resources ==="
    echo "Disk space:"
    df -h /tmp
    echo "Memory:"
    free -h
    echo "CPU info:"
    nproc
    echo "PcapToRef binary check:"
    ls -la /home/airflow/PcapToRef || echo "PcapToRef binary not found!"
    ''',
    dag=dag
)

process_task = PythonOperator(
    task_id='process_pcap',
    python_callable=process_pcap_file,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_results',
    python_callable=upload_results_to_s3_sync,
    dag=dag
)

# Task dependencies
download_task >> check_resources_task  >> process_task >> upload_task