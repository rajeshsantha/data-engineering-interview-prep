#!/usr/bin/env zsh
# Usage: ./mkfile.sh <path/to/file.ext>
# Creates the directory structure and the file if the path ends with an extension.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <path/to/file.ext>" >&2
  exit 1
fi

TARGET="$1"
DIR="$(dirname "$TARGET")"
FILENAME="$(basename "$TARGET")"

# Check that the filename has an extension (contains a dot that is not the first char)
if [[ "$FILENAME" != *.* || "$FILENAME" == .* ]]; then
  echo "Error: '$FILENAME' does not appear to have a file extension." >&2
  exit 1
fi

# Create directory structure if needed
if [[ "$DIR" != "." ]]; then
  mkdir -p "$DIR"
  echo "Directory created (or already exists): $DIR"
fi

# Create the file if it doesn't already exist
if [[ -e "$TARGET" ]]; then
  echo "File already exists: $TARGET"
else
  touch "$TARGET"
  echo "File created: $TARGET"
fi
