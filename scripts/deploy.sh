#!/usr/bin/env bash
# Deploy the DAB bundle with variable overrides from .env
# Usage: ./scripts/deploy.sh [target] [profile]
#   target: dev (default)
#   profile: fevm-classic (default)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$REPO_DIR/.env"
TARGET="${1:-dev}"
PROFILE="${2:-fevm-classic}"

VAR_FLAGS=()

if [ -f "$ENV_FILE" ]; then
  while IFS='=' read -r key value; do
    # Skip empty lines and comments
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    VAR_FLAGS+=("--var" "$key=$value")
  done < "$ENV_FILE"
fi

echo "Deploying bundle '$TARGET' with profile '$PROFILE'..."
if [ ${#VAR_FLAGS[@]} -gt 0 ]; then
  echo "Variable overrides: ${VAR_FLAGS[*]}"
fi

cd "$REPO_DIR"
databricks bundle validate -t "$TARGET" --profile "$PROFILE" "${VAR_FLAGS[@]}"
databricks bundle deploy -t "$TARGET" --profile "$PROFILE" "${VAR_FLAGS[@]}"
echo "Deploy complete."
