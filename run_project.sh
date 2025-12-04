#!/bin/bash

# Master Run Script
# Starts UI, API, and Data components in separate terminals.

echo "🚀 Starting Project..."

# Get the absolute path of the project root
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Function to open a new terminal and run a script
run_in_new_terminal() {
    local script_path="$1"
    local title="$2"
    
    if [ -f "$script_path" ]; then
        echo "   Starting $title..."
        chmod +x "$script_path"
        # Use osascript to tell Terminal to do this
        osascript -e "tell application \"Terminal\" to do script \"$script_path\""
    else
        echo "❌ Could not find $script_path"
    fi
}

# Start UI
run_in_new_terminal "$PROJECT_ROOT/UI/run.sh" "UI Component"

# Start API
run_in_new_terminal "$PROJECT_ROOT/API/run.sh" "API Component"

# Start Data UI
run_in_new_terminal "$PROJECT_ROOT/Data/run.sh" "Data Component"

echo "✅ All components started in separate terminals."
