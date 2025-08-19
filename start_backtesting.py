#!/usr/bin/env python3
"""
Script to start EC2 instance and trigger Airflow backtesting pipeline
"""
import boto3
import time
import requests
import sys
import os
from datetime import datetime, timezone

def get_config():
    """Load configuration from environment variables"""
    required_vars = {
        'EC2_INSTANCE_ID': 'EC2 instance ID',
        'EC2_REGION': 'AWS region',
        'AIRFLOW_BASE_URL': 'Airflow base URL',
        'AIRFLOW_USERNAME': 'Airflow username',
        'AIRFLOW_PASSWORD': 'Airflow password'
    }
    
    config = {}
    missing_vars = []
    
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value:
            missing_vars.append("{} ({})".format(var, description))
        else:
            config[var] = value
    
    if missing_vars:
        print("Missing required environment variables:")
        for var in missing_vars:
            print("  - {}".format(var))
        print("\nPlease set these environment variables and try again.")
        sys.exit(1)
    
    # Optional variables
    config['DAG_ID'] = os.getenv('DAG_ID', 'backtesting_pipeline')
    config['EC2_START_TIMEOUT_MINUTES'] = int(os.getenv('EC2_START_TIMEOUT_MINUTES', '10'))
    config['AIRFLOW_WAIT_TIMEOUT_MINUTES'] = int(os.getenv('AIRFLOW_WAIT_TIMEOUT_MINUTES', '6'))
    config['SERVICE_INIT_WAIT_SECONDS'] = int(os.getenv('SERVICE_INIT_WAIT_SECONDS', '60'))
    
    return config

def get_instance_public_ip(config):
    """Get the current public IP of the EC2 instance"""
    try:
        ec2 = boto3.client('ec2', region_name=config['EC2_REGION'])
        response = ec2.describe_instances(InstanceIds=[config['EC2_INSTANCE_ID']])
        
        instance = response['Reservations'][0]['Instances'][0]
        public_ip = instance.get('PublicIpAddress')
        
        if not public_ip:
            print("Instance doesn't have a public IP address")
            return None
            
        print("Instance public IP: {}".format(public_ip))
        return public_ip
        
    except Exception as e:
        print("Error getting instance public IP: {}".format(e))
        return None

def build_airflow_url(config, public_ip):
    """Build the Airflow URL using the current public IP"""
    airflow_port = "8080"
    if ":" in config['AIRFLOW_BASE_URL'] and config['AIRFLOW_BASE_URL'].count(":") >= 2:
        airflow_port = config['AIRFLOW_BASE_URL'].split(":")[-1]
    
    return "http://{}:{}".format(public_ip, airflow_port)

def start_ec2_instance(config):
    """Start the EC2 instance and wait for it to be running"""
    try:
        print("Starting EC2 instance...")
        ec2 = boto3.client('ec2', region_name=config['EC2_REGION'])
        
        # Check current instance state
        response = ec2.describe_instances(InstanceIds=[config['EC2_INSTANCE_ID']])
        current_state = response['Reservations'][0]['Instances'][0]['State']['Name']
        print("Current instance state: {}".format(current_state))
        
        if current_state == 'running':
            print("Instance is already running")
        elif current_state in ['stopping', 'pending', 'shutting-down']:
            print("Instance is in {} state, waiting...".format(current_state))
            time.sleep(30)
            return start_ec2_instance(config)
        else:
            ec2.start_instances(InstanceIds=[config['EC2_INSTANCE_ID']])
            print("Start command sent, waiting for instance to be running...")
            
            # Calculate waiter config based on timeout
            max_attempts = config['EC2_START_TIMEOUT_MINUTES'] * 4
            waiter = ec2.get_waiter('instance_running')
            waiter.wait(
                InstanceIds=[config['EC2_INSTANCE_ID']],
                WaiterConfig={'Delay': 15, 'MaxAttempts': max_attempts}
            )
            
            print("EC2 instance is now running")
        
        # Get the current public IP
        public_ip = get_instance_public_ip(config)
        if not public_ip:
            return None
            
        return public_ip
        
    except Exception as e:
        print("Error starting EC2 instance: {}".format(e))
        return None

def wait_for_airflow(config):
    """Wait for Airflow to be accessible"""
    print("Waiting for Airflow to be accessible...")
    max_attempts = config['AIRFLOW_WAIT_TIMEOUT_MINUTES'] * 4
    
    for attempt in range(max_attempts):
        try:
            response = requests.get("{}/api/v2/monitor/health".format(config['AIRFLOW_BASE_URL']), timeout=10)
            if response.status_code == 200:
                print("Airflow is accessible")
                return True
        except requests.exceptions.RequestException:
            pass
        
        time.sleep(15)
    
    print("Airflow did not become accessible within {} minutes".format(config['AIRFLOW_WAIT_TIMEOUT_MINUTES']))
    return False

def get_jwt_token(config):
    """Get JWT token for Airflow API authentication"""
    try:
        print("Getting JWT token for API authentication...")
        
        # Request JWT token
        token_url = "{}/auth/token".format(config['AIRFLOW_BASE_URL'])
        payload = {
            "username": config['AIRFLOW_USERNAME'],
            "password": config['AIRFLOW_PASSWORD']
        }
        
        response = requests.post(
            token_url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            token_data = response.json()
            access_token = token_data.get('access_token')
            if access_token:
                print("JWT token obtained successfully")
                return access_token
            else:
                print("No access token in response")
                return None
        else:
            print("Failed to get JWT token. Status: {}, Response: {}".format(response.status_code, response.text))
            return None
            
    except Exception as e:
        print("Error getting JWT token: {}".format(e))
        return None

def trigger_dag(config):
    """Trigger the Airflow DAG using JWT token authentication"""
    try:
        print("Triggering DAG: {}".format(config['DAG_ID']))
        
        # Step 1: Get JWT token
        jwt_token = get_jwt_token(config)
        if not jwt_token:
            print("Failed to get JWT token")
            return False, None
        
        # Step 2: Use JWT token to trigger DAG
        url = "{}/api/v2/dags/{}/dagRuns".format(config['AIRFLOW_BASE_URL'], config['DAG_ID'])
        run_id = "cron__{}".format(datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        
        # Use current time in UTC for logical date
        logical_date = datetime.now(timezone.utc).isoformat()
        
        payload = {
            "dag_run_id": run_id,
            "logical_date": logical_date,
            "conf": {
                "triggered_by": "cron_job",
                "trigger_time": logical_date
            }
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer {}'.format(jwt_token)
        }

        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            print("DAG triggered successfully! Run ID: {}".format(run_id))
            print("DAG URL: {}/dags/{}/grid".format(config['AIRFLOW_BASE_URL'], config['DAG_ID']))
            return True, run_id
        else:
            print("Failed to trigger DAG. Status: {}, Response: {}".format(response.status_code, response.text))
            return False, None
            
    except Exception as e:
        print("Error triggering DAG: {}".format(e))
        return False, None
    
def wait_for_dag_completion(config, run_id):
    """Wait for the DAG run to complete and return the final state"""
    print("Monitoring DAG run completion...")
    
    # Get JWT token for API calls
    jwt_token = get_jwt_token(config)
    if not jwt_token:
        print("Failed to get JWT token for monitoring")
        return False
    
    headers = {
        'Authorization': 'Bearer {}'.format(jwt_token)
    }
    
    max_wait_minutes = 120
    check_interval = 30
    max_attempts = (max_wait_minutes * 60) // check_interval
    
    for attempt in range(max_attempts):
        try:
            # Get DAG run status
            url = "{}/api/v2/dags/{}/dagRuns/{}".format(
                config['AIRFLOW_BASE_URL'], 
                config['DAG_ID'], 
                run_id
            )
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                dag_run_data = response.json()
                state = dag_run_data.get('state')
                
                print("DAG run status: {} (attempt {}/{})".format(state, attempt + 1, max_attempts))
                
                if state in ['success', 'failed']:
                    print("DAG run completed with state: {}".format(state))
                    return state == 'success'
                elif state == 'running':
                    print("DAG is still running, waiting {} seconds...".format(check_interval))
                    time.sleep(check_interval)
                else:
                    print("DAG run in unexpected state: {}".format(state))
                    time.sleep(check_interval)
            else:
                print("Failed to get DAG run status. Status: {}, Response: {}".format(
                    response.status_code, response.text))
                time.sleep(check_interval)
                
        except Exception as e:
            print("Error checking DAG run status: {}".format(e))
            time.sleep(check_interval)
    
    print("Timeout waiting for DAG completion after {} minutes".format(max_wait_minutes))
    return False

def stop_ec2_instance(config):
    """Stop the EC2 instance"""
    try:
        print("Stopping EC2 instance...")
        ec2 = boto3.client('ec2', region_name=config['EC2_REGION'])
        
        # Stop the instance
        ec2.stop_instances(InstanceIds=[config['EC2_INSTANCE_ID']])
        print("Stop command sent for instance: {}".format(config['EC2_INSTANCE_ID']))
        
        # Wait for instance to be stopped
        print("Waiting for instance to stop...")
        waiter = ec2.get_waiter('instance_stopped')
        waiter.wait(
            InstanceIds=[config['EC2_INSTANCE_ID']],
            WaiterConfig={'Delay': 15, 'MaxAttempts': 20}
        )
        
        print("EC2 instance has been stopped successfully")
        return True
        
    except Exception as e:
        print("Error stopping EC2 instance: {}".format(e))
        return False

def main():
    """Main execution function"""
    print("=== Starting Backtesting Pipeline Automation ===")
    
    # Load configuration
    config = get_config()
    print("Configuration loaded")
    
    try:
        # Step 1: Start EC2 instance and get public IP
        public_ip = start_ec2_instance(config)
        if not public_ip:
            print("Failed to start EC2 instance or get public IP, exiting")
            sys.exit(1)
        
        # Step 2: Build dynamic Airflow URL
        airflow_url = build_airflow_url(config, public_ip)
        config['AIRFLOW_BASE_URL'] = airflow_url
        
        # Step 3: Wait a bit for services to initialize
        print("Waiting {} seconds for services to initialize...".format(config['SERVICE_INIT_WAIT_SECONDS']))
        time.sleep(config['SERVICE_INIT_WAIT_SECONDS'])
        
        # Step 4: Wait for Airflow to be accessible
        if not wait_for_airflow(config):
            print("Airflow is not accessible, stopping instance and exiting")
            stop_ec2_instance(config)
            sys.exit(1)
        
        # Step 5: Trigger the DAG
        success, run_id = trigger_dag(config)
        if not success:
            print("Failed to trigger DAG, stopping instance and exiting")
            stop_ec2_instance(config)
            sys.exit(1)
        
        print("=== Backtesting Pipeline Started Successfully ===")
        print("Monitor progress at: {}/dags/{}/grid".format(config['AIRFLOW_BASE_URL'], config['DAG_ID']))
        
        # Step 6: Wait for DAG completion
        dag_success = wait_for_dag_completion(config, run_id)
        
        if dag_success:
            print("=== DAG Run Completed Successfully ===")
        else:
            print("=== DAG Run Failed or Timed Out ===")
        
        # Step 7: Stop EC2 instance
        print("=== Stopping EC2 Instance ===")
        if stop_ec2_instance(config):
            print("=== Pipeline Automation Completed ===")
        else:
            print("=== Pipeline Completed but EC2 stop failed ===")
            print("Please manually stop instance: {}".format(config['EC2_INSTANCE_ID']))
    
    except KeyboardInterrupt:
        print("\n=== Script interrupted by user ===")
        print("Stopping EC2 instance before exit...")
        stop_ec2_instance(config)
        sys.exit(1)
    except Exception as e:
        print("=== Unexpected error occurred ===")
        print("Error: {}".format(e))
        print("Stopping EC2 instance before exit...")
        stop_ec2_instance(config)
        sys.exit(1)

if __name__ == "__main__":
    main()