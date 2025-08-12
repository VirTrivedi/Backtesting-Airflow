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
        
        if not file_info:
            raise Exception("No file info received from download task")
        
        parsed_params = file_info['parsed_params']
        input_file = file_info['input_file']
                
        if not os.path.exists(input_file):
            raise Exception(f"No PCAP files found in /tmp directory. Expected: {input_file}")
        
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
            timeout=None
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
            
def process_histbook(**context):
    """Run HistBook on each PcapToRef output file individually"""
    try:
        # Get info from previous task
        pcap_info = context['task_instance'].xcom_pull(task_ids='process_pcap')
        input_dir = pcap_info['output_dir'].rstrip('/')
        
        # Create books subdirectory
        books_output_dir = os.path.join(input_dir, 'books/')
        os.makedirs(books_output_dir, exist_ok=True)
                
        # Find all book_events files
        book_event_files = []
        for root, dirs, files in os.walk(input_dir):
            if 'books' not in root:
                for file in files:
                    file_path = os.path.join(root, file)
                    if 'book_events' in file and file.endswith('.bin'):
                        book_event_files.append(file_path)
                
        if not book_event_files:
            raise Exception(f"No book_events files found in PcapToRef output directory: {input_dir}")
        
        # Process each file individually with HistBook
        successful_files = 0
        failed_files = 0
        
        for i, input_file in enumerate(book_event_files, 1):
            cmd = ['/home/airflow/HistBook', '-i', input_file, '-o', books_output_dir]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=None
            )
                        
            if result.returncode == 0:
                successful_files += 1
            else:
                failed_files += 1
                
        # Check what output files were created
        book_output_files = []
        for root, dirs, files in os.walk(books_output_dir):
            for file in files:
                book_output_files.append(os.path.join(root, file))
                
        # Don't fail the task if some files failed
        if successful_files == 0:
            raise Exception(f"HistBook failed to process any files. All {failed_files} files failed.")
        elif failed_files > 0:
            print(f"Warning: {failed_files} files failed to process, but {successful_files} succeeded.")
        
        return {
            'final_output_dir': input_dir,
            'venue': pcap_info['venue'],
            'dateint': pcap_info['dateint'],
        }
        
    except Exception as e:
        raise Exception(f"HistBook processing failed: {e}")
                
def upload_results_to_s3_sync(**context):
    """Upload output directory using AWS CLI sync"""
    try:
        # Get info from previous tasks
        file_info = context['task_instance'].xcom_pull(task_ids='download_pcap')
        histbook_info = context['task_instance'].xcom_pull(task_ids='process_histbook')
        
        bucket_name = file_info['bucket_name']
        output_dir = histbook_info['final_output_dir']

        # Create S3 destination path
        s3_dest = f"s3://{bucket_name}/{histbook_info['dateint']}/{histbook_info['venue']}/"
                
        # Use AWS CLI sync to upload directory structure
        sync_cmd = [
            'aws', 's3', 'sync',
            output_dir, 
            s3_dest
        ]
        
        result = subprocess.run(sync_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"AWS S3 sync failed: {result.stderr}")
        
        files_to_clean = []
        
        # Clean up input PCAP file
        if file_info and file_info.get('input_file'):
            input_file = file_info['input_file']
            if os.path.exists(input_file):
                files_to_clean.append(input_file)
        
        # Clean up processing directory
        if histbook_info and histbook_info.get('final_output_dir'):
            if os.path.exists(output_dir):
                files_to_clean.append(output_dir)
        
        # Clean up all files
        for file_path in files_to_clean:
            try:
                if os.path.isdir(file_path):
                    import shutil
                    shutil.rmtree(file_path)
                else:
                    os.remove(file_path)
            except Exception as cleanup_error:
                raise Exception(f"Failed to clean up {file_path}: {cleanup_error}")
            
        return {
            's3_base_path': s3_dest,
            'sync_output': result.stdout
        }
        
    except Exception as e:
        raise Exception(f"Upload failed: {e}")

def cleanup_on_failure(context):
    """Clean up local files if processing fails"""
    try:
        # Clean up input file
        file_info = context['task_instance'].xcom_pull(task_ids='download_pcap')
        input_file = file_info.get('input_file') if file_info else None
        
        files_to_clean = []
        if input_file:
            files_to_clean.append(input_file)

        # Clean up any dateint/venue directories that might exist
        if os.path.exists('/tmp'):
            for item in os.listdir('/tmp'):
                item_path = os.path.join('/tmp', item)
                if os.path.isdir(item_path) and item.isdigit() and len(item) == 8:
                    files_to_clean.append(item_path)
                elif item.endswith('.pcap.zst') or item.endswith('.pcap'):
                    files_to_clean.append(item_path)

        # Clean up all identified files/directories
        for file_path in files_to_clean:
            if file_path and os.path.exists(file_path):
                try:
                    if os.path.isdir(file_path):
                        import shutil
                        shutil.rmtree(file_path)
                    else:
                        os.remove(file_path)
                except Exception as cleanup_error:
                    raise Exception(f"Failed to clean up {file_path}: {cleanup_error}")
                    
    except Exception as e:
        raise Exception(f"Cleanup on failure failed: {e}")

# DAG definition
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': cleanup_on_failure,
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
    ls -la /home/airflow/HistBook || echo "HistBook binary not found!"
    ''',
    dag=dag
)

process_task = PythonOperator(
    task_id='process_pcap',
    python_callable=process_pcap_file,
    dag=dag
)

histbook_task = PythonOperator(
    task_id='process_histbook',
    python_callable=process_histbook,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_results',
    python_callable=upload_results_to_s3_sync,
    dag=dag
)

# Task dependencies
download_task >> check_resources_task  >> process_task >> histbook_task >> upload_task