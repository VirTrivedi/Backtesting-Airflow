#!/bin/bash
# Load environment variables and run the Python script with proper environment

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Load environment variables from .env file
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(cat "$SCRIPT_DIR/.env" | grep -v '^#' | xargs)
    echo "Environment variables loaded from .env"
else
    echo ".env file not found in $SCRIPT_DIR"
    exit 1
fi

# Set up Python environment
export PATH="/home/vir/.local/bin:$PATH"
export PYTHONPATH="/home/vir/.local/lib/python3.7/site-packages:$PYTHONPATH"

# Suppress urllib3 warnings if they appear
export PYTHONWARNINGS="ignore::urllib3.exceptions.InsecureRequestWarning"

# Run the Python script with explicit python3
/usr/bin/python3 "$SCRIPT_DIR/start_backtesting.py"