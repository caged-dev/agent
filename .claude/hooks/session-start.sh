#!/bin/bash
set -euo pipefail

echo '{"async": false}'

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Sync shared Caged skills/agents from caged-dev/workspace so a standalone
# checkout of this repo (e.g. Claude Code on the web opened directly here,
# without the nested apps/ workspace layout) gets the same guardrails as a
# session started from the workspace root.
WS_DIR="$(mktemp -d)"
if git clone --depth 1 --branch dev https://github.com/caged-dev/workspace "$WS_DIR" >/dev/null 2>&1 && [ -d "$WS_DIR/.claude" ]; then
  rm -rf "$CLAUDE_PROJECT_DIR/.claude/skills" "$CLAUDE_PROJECT_DIR/.claude/agents"
  mkdir -p "$CLAUDE_PROJECT_DIR/.claude"
  cp -r "$WS_DIR/.claude/skills" "$CLAUDE_PROJECT_DIR/.claude/skills"
  cp -r "$WS_DIR/.claude/agents" "$CLAUDE_PROJECT_DIR/.claude/agents"
  # Shared skills/agents are written with paths relative to this repo's
  # location inside the monorepo layout (oss/agent/) — strip that prefix so
  # they read correctly from this repo's own root.
  find "$CLAUDE_PROJECT_DIR/.claude/skills" "$CLAUDE_PROJECT_DIR/.claude/agents" -name "*.md" \
    -exec sed -i "s#oss/agent/##g" {} +
else
  echo "warning: could not sync caged-dev/workspace config, continuing without it" >&2
fi
rm -rf "$WS_DIR"

go mod download
