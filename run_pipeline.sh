#!/usr/bin/env bash
set -e
DATE=$(date +"%Y%m%d_%H%M")
LOG_DIR="logs/$DATE"
OUT_DIR="outputs/$DATE"

mkdir -p "$LOG_DIR" "$OUT_DIR" data_raw/scans_slither data_raw/scans_mythril data_raw/scans_echidna

log() { echo -e "\n[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_DIR/pipeline.log"; }
trap 'log "❌ Error at line $LINENO. Check $LOG_DIR for details."' ERR

log "🚀 Starting DeFi Pipeline ($DATE)..."

steps=(
  "Fetch verified contracts" "fetch_contracts_expanded.py"
  "Run Slither" "run_slither_batch.py"
  "Run Mythril" "run_mythril_batch.py"
  "Fetch Exploit Datasets" "fetch_exploit_datasets.py"
  "Merge Exploit Groundtruth" "merge_exploit_groundtruth.py"
  "Compute Severity Scores" "compute_severity_scores.py"
  "Run Dynamic Analysis" "run_dynamic_batch.py"
)

for ((i=0; i<${#steps[@]}; i+=2)); do
  step_name="${steps[i]}"
  step_script="${steps[i+1]}"
  log "▶️ Step $((i/2+1)): $step_name..."
  if [[ -f "$step_script" ]]; then
    python "$step_script" 2>&1 | tee -a "$LOG_DIR/${step_script%.py}.log"
  else
    log "⚠️ Missing: $step_script (skipped)"
  fi
done

log "✅ Pipeline completed successfully."