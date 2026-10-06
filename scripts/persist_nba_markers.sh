#!/usr/bin/env bash
set -euo pipefail

# Persist Telegram idempotency markers immediately after a polling tick.
# Caller should only invoke this in production schedule mode.

if [ ! -d posted ]; then
  echo "No posted/ directory; nothing to persist."
  exit 0
fi

if [ -z "$(git status --porcelain posted)" ]; then
  echo "No marker changes."
  exit 0
fi

git config user.name "github-actions"
git config user.email "actions@github.com"
git add -A posted
git commit -m "NBA live markers $(date -u +'%Y-%m-%d %H:%M:%S')"

for attempt in 1 2 3; do
  if git pull --rebase origin main && git push origin HEAD:main; then
    echo "Markers pushed immediately after poll."
    exit 0
  fi
  echo "Marker push attempt ${attempt} failed; retrying."
  git rebase --abort 2>/dev/null || true
  sleep $((attempt * 2))
done

echo "Failed to persist posted/ markers after 3 attempts."
exit 1
