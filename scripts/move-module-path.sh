#!/usr/bin/env bash
# Usage: scripts/move-module-path.sh OLD_MAJOR NEW_MAJOR (e.g. v2 v3)
set -euo pipefail
cd "$(dirname "$0")/.."
old="github.com/iskorotkov/avro/$1"
new="github.com/iskorotkov/avro/$2"
go mod edit -module "$new"
grep -rIl --exclude-dir=.git --include='*.go' --include='*.md' --include='*.tmpl' --include='*.yml' --include='*.yaml' --include='Makefile' -F "$old" . \
  | xargs -r sed -i "s#$old#$new#g"
