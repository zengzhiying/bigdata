#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

pyinstaller \
  --clean \
  --noconfirm \
  es-pit-reader.spec

echo "Binary created at: $SCRIPT_DIR/dist/es-pit-reader"
