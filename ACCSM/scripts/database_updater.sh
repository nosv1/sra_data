#!/bin/bash

# scheduled via cron by using
#  */5 * * * * /home/pi/sra_data/ACCSM/scripts/database_updater.sh
# view the cron jobs with crontab -l

LOGFILE="/home/pi/sra_data/ACCSM/scripts/logs/$(date +'%Y%m%d_%H%M%S').log"
VENV_PATH="/home/pi/sra_data/env_pi3/bin/activate"
DOWNLOAD_SCRIPT="/home/pi/sra_data/ACCSM/results_downloader.py"
PARSER_SCRIPT="/home/pi/sra_data/race_result_parser_neo4j.py"
DOWNLOAD_DIR="/home/pi/sra_data/ACCSM/downloads/"
GIT_DIR="/home/pi/sra_data/"

log() {
    echo "$(date): $1"
}

{
    log "Starting database updater script"

    # Activate virtual environment
    if source "$VENV_PATH"; then
        log "Virtual environment activated"
    else
        log "Failed to activate virtual environment"
        exit 1
    fi

    # Run the accsm download script
    if python3 "$DOWNLOAD_SCRIPT" --accsm; then
        log "ACCSM Download script executed successfully"
    else
        log "ACCSM Download script execution failed"
        deactivate
        exit 1
    fi

    # Run the sra download script
    if python3 "$DOWNLOAD_SCRIPT" --sra --after-date 2025-03-02; then
        log "SRA Download script executed successfully"
    else
        log "SRA Download script execution failed"
        deactivate
        exit 1
    fi

    # # Stage the downloads directory
    # if git -C "$GIT_DIR" add "$DOWNLOAD_DIR"; then
    #     log "Staged the downloads directory"
    # else
    #     log "Failed to stage the downloads directory"
    #     deactivate
    #     exit 1
    # fi

    # # Commit the changes
    # if git -C "$GIT_DIR" commit -m "Update ACCSM downloads"; then
    #     log "Committed the changes"
    # else
    #     log "Failed to commit the changes"
    #     deactivate
    #     exit 1
    # fi

    # # Push the changes
    # if git -C "$GIT_DIR" push; then
    #     log "Pushed the changes"
    # else
    #     log "Failed to push the changes"
    #     deactivate
    #     exit 1
    # fi

    # Run the parser script
    if python3 "$PARSER_SCRIPT"; then
        log "Parser script executed successfully"
    else
        log "Parser script execution failed"
        deactivate
        exit 1
    fi

    # Deactivate virtual environment
    deactivate
    log "Virtual environment deactivated"

    log "Script finished"

    # Check the exit status of the script
    if [ $? -eq 0 ]; then
        # Get the most recent log file
        recent_log=$(ls -t /home/pi/sra_data/ACCSM/scripts/logs/*.log | head -n 1)
        # Remove all but the most recent log file
        find /home/pi/sra_data/ACCSM/scripts/logs/ -type f -name '*.log' ! -path "$recent_log" -exec rm {} \;
    else
        # Keep the log file if there was an error
        log "Script failed, keeping log file"
        # Rename the file to show there was an error
        mv "$LOGFILE" "${LOGFILE}.error"
    fi
} >> "$LOGFILE" 2>&1