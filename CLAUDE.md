# Caged Agent — Claude Code Instructions

This is the standalone in-sandbox agent binary for **Caged** (caged.dev), an AI Agent Sandbox Platform by Bytangle Ltd. It corresponds to `oss/agent/` when checked out inside the `caged-dev/workspace` workspace, but this repo also works standalone (e.g. Claude Code on the web opened directly here). It is a standalone Go module with its own `go.mod` — it is NOT part of the `caged-api` monorepo's module.

**Source of truth for shared conventions is `caged-dev/workspace`.** This repo's `.claude/skills/` and `.claude/agents/` are re-synced from there at the start of every Claude Code web session (see `.claude/hooks/session-start.sh`) — edit skills/agents in `workspace`, not here; local edits here are overwritten on the next session start.

## What Caged Does
Caged lets developers run AI coding agents (Claude Code, Cursor, Aider, custom MCP agents) inside isolated Firecracker microVMs on bare metal. Every session is isolated, observed, costed, scored, and replayable. This binary runs inside the sandbox itself, recording file changes/commands/LLM calls and enforcing budget/isolation limits from the inside.

## Critical Rules (ALL code changes)

### NEVER
- Return raw error messages to API clients — use structured error responses
- Log sensitive fields: password, apiKey, accessToken, secretKey
- Call Docker/Firecracker directly — always go through the `Runtime` interface
- Skip input validation on API handlers
- Use `any` type in Go — always define concrete types
- Commit secrets, .env files, or credentials
- Ship without tests for the changed code
- Ship without updating relevant docs
- Spawn unbounded goroutines — always use worker pools or semaphores
- Make external calls without timeouts — always use context.WithTimeout
- Write unbounded queries — always use LIMIT and proper indexes
- Accept unbounded work — always apply backpressure (buffered channels, rate limits)

### ALWAYS
- Use structured logging (slog in Go, structured JSON)
- Validate at system boundaries (API handlers, webhook receivers)
- Use context.Context for cancellation and deadlines
- Write tests alongside implementation (same PR)
- Use interfaces for all cross-package dependencies (consumer defines, provider implements)
- Use conventional commits: feat:, fix:, docs:, refactor:, test:, chore:
- Add OpenTelemetry spans for cross-service calls
- Use database transactions for multi-row operations
- Set explicit bounds on connection pools, goroutines, and queues

### Go Conventions
- Error handling: `if err != nil { return fmt.Errorf("doing X: %w", err) }`
- Interfaces in consumer packages, implementations in provider packages
- Table-driven tests with `t.Run()`
- No init() functions — explicit initialization only
- Context as first parameter always
- Unexported by default — export only what's needed

## Interface Contract Enforcement (MANDATORY — AUTOMATIC)
This repo speaks the MCP protocol to `mcp-server` and reports events/costs to `caged-api`. If you change a message type, event schema, or protocol handler signature, verify it still matches what those repos expect before shipping. Run `go build ./...` and `go vet ./...` here after any such change.

## File Structure
```
cmd/            # Entry point(s)
internal/       # Implementation packages
```

## Skills & Subagents
- `.claude/skills/<name>/SKILL.md` — auto-discovered by the Skill tool; most relevant here: `go-backend`, `mcp-protocol`, `security-guardrails`, `interface-contracts`, `testing`, `code-review`, `sdk-development`.
- `.claude/agents/<name>.md` — dispatch via the Agent tool: `backend-engineer`, `security-engineer`, `qa-engineer`, `sdk-engineer`.
- Full workspace-level context (product specs, roadmap, branding): `caged-dev/workspace` repo.
