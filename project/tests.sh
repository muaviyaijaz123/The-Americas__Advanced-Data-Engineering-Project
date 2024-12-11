
#!/bin/bash

# Getting directory of the current script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Navigating to project directory
PROJECT_DIR="$SCRIPT_DIR"
if [[ "$(basename "$SCRIPT_DIR")" != "project" ]]; then
    PROJECT_DIR="$SCRIPT_DIR/project"
fi

# Running the Python script
python3 "$PROJECT_DIR/test_pipeline.py"

# Requirements to run the tests pipeline 

# 1. Sign into your Kaggle Account 
# 2. Go to Settings option from top right
# 3. Create 'New API token' which will crete 'kaggle.json' file
# 4. Place this kaggle.json file into project folder and then run the pipeline

# I am installing requirements.txt file now in yml file to install dependencies