from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import boto3
import os
import subprocess
import re
from pathlib import Path

def cleanup_on_failure(context):
    """Clean up local files if processing fails"""
    try:
        # Clean up input file
        file_info = context['task_instance'].xcom_pull(task_ids='download_pcap_from_s3')
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
    'backtesting_pipeline',
    default_args=default_args,
    description='Process PCAP files with PcapToRef, process book_events files with HistBook, and store results in S3',
    schedule=None,
    catchup=False,
    tags=['pcap', 'market-data', 's3']
)

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

@task(dag=dag)
def download_pcap_from_s3():
    """Find and download the most recent PCAP file from S3 to local processing directory"""
    try:
        s3 = boto3.client('s3')
        bucket_name = 'vir-airflow'

        # List all objects in the bucket
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/'
        )
        
        if 'Contents' not in response:
            raise Exception(f"No files found in bucket: {bucket_name}")
        
        print(f"=== ALL FILES IN BUCKET ===")
        all_files = response['Contents']
        print(f"Total files in bucket: {len(all_files)}")

        # Filter for PCAP files and find the most recent
        pcap_files = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.pcap.zst') or key.endswith('.pcap'):
                pcap_files.append({
                    'key': key,
                    'last_modified': obj['LastModified'],
                    'size': obj['Size']
                })
        
        if not pcap_files:
            raise Exception("No PCAP files (.pcap or .pcap.zst) found in the bucket")
        
        # Sort by last modified date and get the most recent file
        pcap_files.sort(key=lambda x: x['last_modified'], reverse=True)
        most_recent_file = pcap_files[0]
        s3_input_path = most_recent_file['key']
        
        # Download the selected file
        local_path = '/tmp/' + os.path.basename(s3_input_path)
        s3.download_file(bucket_name, s3_input_path, local_path)
        
        # Verify download
        if not os.path.exists(local_path):
            raise Exception(f"Download failed - file not found at: {local_path}")
                                
        # Parse filename to extract parameters
        filename = os.path.basename(s3_input_path)
        parsed_params = parse_pcap_filename(filename)

        # Store file info in XCom
        return {
            'input_file': local_path,
            'bucket_name': bucket_name,
            'parsed_params': parsed_params
        }
        
    except Exception as e:
        raise Exception(f"Download failed: {e}")
    
@task(dag=dag)
def process_pcap_file(**context):
    """Run PcapToRef on the downloaded PCAP file with parsed parameters"""
    try:        
        # Get file info from previous task
        file_info = context['task_instance'].xcom_pull(task_ids='download_pcap_from_s3')
        
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
        
        # Collect all output files created
        output_files = []
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                if file.endswith('.bin'):
                    output_files.append(os.path.join(root, file))

        # Return output info for next task
        return {
            'output_dir': output_dir,
            'venue': parsed_params['venue'],
            'dateint': parsed_params['dateint'],
            'output_files': output_files
        }
        
    except Exception as e:
        raise Exception(f"Processing failed: {e}")
            
@task(dag=dag)
def get_histbook_file_list(**context):
    """Get the list of files to process"""
    pcap_info = context['task_instance'].xcom_pull(task_ids='process_pcap_file')
    output_files = pcap_info.get('output_files', [])
    
    # Return list of file info dicts
    return [
        {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'file_index': i
        }
        for i, file_path in enumerate(output_files)
    ]

@task(dag=dag)
def process_single_book_events_file(file_info: dict, **context):
    """Process a single book_events file"""
    file_path = file_info['file_path']
    file_name = file_info['file_name']
        
    # Get additional context
    pcap_info = context['task_instance'].xcom_pull(task_ids='process_pcap_file')
    input_dir = pcap_info['output_dir']
    books_output_dir = os.path.join(input_dir, 'books/')
    os.makedirs(books_output_dir, exist_ok=True)
    
    # Verify input file exists
    if not os.path.exists(file_path):
        raise Exception(f"Input file does not exist: {file_path}")
    
    # Run HistBook
    cmd = ['/home/airflow/HistBook', '-i', file_path, '-o', books_output_dir]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=None
    )
    
    success = result.returncode == 0  
    return {
        'file_name': file_name,
        'file_path': file_path,
        'success': success,
        'return_code': result.returncode,
        'error': result.stderr if not success else None
    }

@task(dag=dag)
def collect_histbook_results(histbook_results: list, **context):
    """Collect and summarize all HistBook results"""    
    successful_files = sum(1 for result in histbook_results if result.get('success', False))
    failed_files = len(histbook_results) - successful_files
        
    # Get context info
    pcap_info = context['task_instance'].xcom_pull(task_ids='process_pcap_file')
    
    return {
        'final_output_dir': pcap_info['output_dir'],
        'successful_files': successful_files,
        'failed_files': failed_files,
        'total_files': len(histbook_results),
        'venue': pcap_info['venue'],
        'dateint': pcap_info['dateint'],
    }

@task(dag=dag)
def upload_results_to_s3_sync(**context):
    """Upload output directory using AWS CLI sync"""
    try:
        # Get info from previous tasks
        file_info = context['task_instance'].xcom_pull(task_ids='download_pcap_from_s3')
        histbook_info = context['task_instance'].xcom_pull(task_ids='collect_histbook_results')
        
        bucket_name = file_info['bucket_name']
        output_dir = histbook_info['final_output_dir']

        # Create S3 destination path
        s3_dest = f"s3://{bucket_name}/{histbook_info['dateint']}/{histbook_info['venue']}/"
                
        # Use AWS CLI sync to upload directory structure
        sync_cmd = ['aws', 's3', 'sync', output_dir, s3_dest]
        
        result = subprocess.run(sync_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"AWS S3 sync failed: {result.stderr}")
        
        files_to_clean = []
        
        # Clean up input PCAP file first
        if file_info and file_info.get('input_file'):
            input_file = file_info['input_file']
            if os.path.exists(input_file):
                files_to_clean.append(('file', input_file))
        
        # Clean up processing directory
        if os.path.exists(output_dir):
            dateint_dir = os.path.dirname(output_dir)
            files_to_clean.append(('directory', dateint_dir))
        
        # Clean up all files/directories
        for item_type, file_path in files_to_clean:
            try:
                if item_type == 'directory' and os.path.isdir(file_path):
                    import shutil
                    shutil.rmtree(file_path)
                elif item_type == 'file' and os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as cleanup_error:
                print(f"Warning: Failed to clean up {file_path}: {cleanup_error}")
            
        return {
            's3_base_path': s3_dest,
            'sync_output': result.stdout,
            'histbook_summary': {
                'total_files': histbook_info['total_files'],
                'successful_files': histbook_info['successful_files'],
                'failed_files': histbook_info['failed_files']
            }
        }
        
    except Exception as e:
        raise Exception(f"Upload failed: {e}")
    
# Task definitions
download_task = download_pcap_from_s3()

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

process_task = process_pcap_file()
file_list = get_histbook_file_list()
histbook_results = process_single_book_events_file.expand(file_info=file_list)
final_results = collect_histbook_results(histbook_results)
upload_task = upload_results_to_s3_sync()

# Task dependencies
download_task >> check_resources_task >> process_task >> file_list >> histbook_results >> final_results >> upload_task