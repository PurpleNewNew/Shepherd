#!/usr/bin/env bash
set -euo pipefail

# Simple guardrail to catch legacy magic numbers that should rely on config/defaults.
# Extend PATTERNS as new constants migrate.
PATTERNS=(
  "30720"
)

fail=0
for pattern in "${PATTERNS[@]}"; do
  tmpfile=$(mktemp)
  if rg --glob '!config/defaults/**/*.go' --glob '!tools/check_constants.sh' "${pattern}" >"${tmpfile}"; then
    echo "Found forbidden pattern '${pattern}':"
    cat "${tmpfile}"
    fail=1
  fi
  rm -f "${tmpfile}" 2>/dev/null || true
done

if [[ $fail -ne 0 ]]; then
  echo "\nPlease replace the magic number with an entry in config/defaults." >&2
  exit 1
fi

echo "Constant check passed."
