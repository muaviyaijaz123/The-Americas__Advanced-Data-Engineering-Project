
#!/bin/bash

# Getting directory of the current script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Navigating to project directory
PROJECT_DIR="$SCRIPT_DIR"
if [[ "$(basename "$SCRIPT_DIR")" != "project" ]]; then
    PROJECT_DIR="$SCRIPT_DIR/project"
fi

# Install dependencies from requirements.txt
if [[ -f "$PROJECT_DIR/requirements.txt" ]]; then
    echo "Installing dependencies from requirements.txt...\n"
    pip3 install -r "$PROJECT_DIR/requirements.txt"
else
    echo "requirements.txt not found in $PROJECT_DIR. Skipping dependency installation."
fi

# Running the Python script
python3 "$PROJECT_DIR/test_pipeline.py"


#Requirements to run the pipeline

# 1. Please sign into your Kaggle Accont first
# 2. Go to Settings option from top right
# 3. Create 'New API token' which will crete 'kaggle.json' file
# 4. Place this kaggle.json file into project folder and then run the pipeline

#5. I have made a requirements.txt file now separately and incldued in this script to install those dependencies
# but in case it does not work make sure to have these dependecies on your system

# import shutil
# import platform
# import subprocess
# import zipfile
# import pandas as pd
# import sqlite3