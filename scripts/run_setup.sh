#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

python3 -m pip install -r requirements.txt
python3 scripts/bootstrap.py
spark-submit src/model_creation.py
spark-submit src/color_creation.py
spark-submit src/cars_generator.py

echo "Setup complete. Start the four long-running jobs using the README commands."

