
#!/bin/bash

# Getting directory of the current script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Navigating to project directory
PROJECT_DIR="$SCRIPT_DIR"
if [[ "$(basename "$SCRIPT_DIR")" != "project" ]]; then
    PROJECT_DIR="$SCRIPT_DIR/project"
fi

# Running the Python script
python3 "$PROJECT_DIR/pipeline.py"  


# Requirements to run the pipeline

# 1. Sign into your Kaggle Account 
# 2. Go to Settings option from top right
# 3. Create 'New API token' which will crete 'kaggle.json' file
# 4. Place this kaggle.json file into project folder and then run the pipeline

# I have also placed the requirements.txt file now in project folder
# so that you know what dependencies to install