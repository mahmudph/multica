package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// commandcodeTerminateGrace bounds how long a cancelled command-code process
// is given to exit after SIGTERM before its whole process group is
// SIGKILLed. Mirrors opencodeTerminateGrace's role for the same reason: the
// stdout read end must stay open until the tree is actually gone, or a
// wedged descendant can spin writing into a closed pipe (#4533).
const commandcodeTerminateGrace = 5 * time.Second

// commandcodeBlockedArgs are flags hardcoded by the daemon (or owned by a
// dedicated ExecOptions field) that user-configured custom_args must not
// override. Includes every flag this backend itself passes, plus the
// alternate resume/session flags that would otherwise let a custom_args
// entry escape task isolation (resuming or persisting an unrelated
// conversation).
var commandcodeBlockedArgs = map[string]blockedArgMode{
	"-p":                             blockedStandalone, // headless mode; hardcoded
	"--print":                        blockedStandalone, // long form of -p
	"--output-format":                blockedWithValue,  // json protocol for daemon communication
	"--yolo":                         blockedStandalone, // daemon manages non-interactive permission bypass
	"--dangerously-skip-permissions": blockedStandalone, // alias for --yolo
	"--skip-onboarding":              blockedStandalone, // hardcoded so first-run taste onboarding never blocks a headless run
	"--no-auto-update":               blockedStandalone, // hardcoded to keep the CLI version stable mid-task
	"-m":                             blockedWithValue,  // owned by ExecOptions.Model
	"--model":                        blockedWithValue,
	"--effort":                       blockedWithValue, // owned by ExecOptions.ThinkingLevel
	"--max-turns":                    blockedWithValue, // owned by ExecOptions.MaxTurns
	"-r":                             blockedWithValue, // owned by ExecOptions.ResumeSessionID
	"--resume":                       blockedWithValue,
	"-c":                             blockedStandalone, // would resume the most recent session in the workdir, breaking task isolation
	"--continue":                     blockedStandalone,
	"--session":                      blockedWithValue,  // alternate resume-by-transcript-path flag; same isolation risk as --resume
	"--no-session":                   blockedStandalone, // would keep the transcript in-memory only, breaking resume
	"--fork-session":                 blockedStandalone, // would fork instead of continuing the pinned session in place
}

// commandcodeBackend implements Backend by spawning
// `command-code -p --output-format json --yolo` and reading the resulting
// NDJSON event stream from stdout. Structurally modeled on opencodeBackend:
// a single one-shot headless run per Execute call, prompt delivered on
// stdin (never argv — avoids the Windows CreateProcess argv-length cap a
// large prompt can hit, see opencodeBackend's comment), streamed JSON
// parsed line by line.
//
// The wire format was captured from a live `cmd -p ... --output-format
// json --yolo` run (Command Code v1.26.0) rather than reverse-engineered
// from documentation alone — the public docs at commandcode.ai/docs/headless
// show only two example frames. Event/field names not exercised by that
// capture are commented as unconfirmed where it matters.
type commandcodeBackend struct {
	cfg Config
}

func (b *commandcodeBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "command-code"
	}
	resolved, err := exec.LookPath(execPath)
	if err != nil {
		return nil, fmt.Errorf("command-code executable not found at %q: %w", execPath, err)
	}
	execPath = resolved

	timeout := opts.Timeout
	runCtx, cancel := runContext(ctx, timeout)

	args := []string{"-p", "--output-format", "json", "--yolo", "--skip-onboarding", "--no-auto-update"}
	if opts.Model != "" {
		args = append(args, "-m", opts.Model)
	}
	if opts.ThinkingLevel != "" {
		args = append(args, "--effort", opts.ThinkingLevel)
	}
	if opts.MaxTurns > 0 {
		args = append(args, "--max-turns", fmt.Sprintf("%d", opts.MaxTurns))
	}
	if opts.ResumeSessionID != "" {
		args = append(args, "--resume", opts.ResumeSessionID)
	}
	// SystemPrompt is never forwarded: command-code has no inline
	// system-prompt flag and instead discovers AGENTS.md by walking up from
	// the workdir, same as Codex/OpenCode/Qwen — the daemon already writes
	// the per-task runtime brief there (MUL-5392).
	//
	// MCP config is likewise not forwarded: command-code's CLI reference
	// exposes only `cmd mcp` server-management subcommands, no per-invocation
	// config flag comparable to Claude's --mcp-config or OpenCode's
	// OPENCODE_CONFIG_CONTENT. ExecOptions.McpConfig is intentionally unread
	// here; providerSupportsMcpConfig on the frontend must not list
	// "commandcode" until that changes.
	args = append(args, filterCustomArgs(opts.ExtraArgs, commandcodeBlockedArgs, b.cfg.Logger)...)
	args = append(args, filterCustomArgs(opts.CustomArgs, commandcodeBlockedArgs, b.cfg.Logger)...)

	cmd := b.cfg.commandAt(execPath).exec(runCtx, args...)
	hideAgentWindow(cmd)
	// Take over context cancellation the same way opencodeBackend does: drive
	// a graceful, group-wide SIGTERM→SIGKILL from the cancellation goroutine
	// below instead of newRuntimeCmd's default of SIGKILLing the whole group
	// the instant runCtx is done.
	cmd.Cancel = func() error { return nil }
	b.cfg.logAgentCommandWithPrompt(cmd, newAgentCommandLogArgs(args), len(prompt))
	cmd.WaitDelay = 10 * time.Second
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	cmd.Env = buildEnv(b.cfg.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("command-code stdout pipe: %w", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("command-code stdin pipe: %w", err)
	}
	var closeStdinOnce sync.Once
	closeStdin := func() { closeStdinOnce.Do(func() { _ = stdin.Close() }) }
	stderr := newStderrTail(newLogWriter(b.cfg.Logger, "[command-code:stderr] "), 0)
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		closeStdin()
		cancel()
		return nil, fmt.Errorf("start command-code: %w", ExplainExecError(err))
	}

	b.cfg.Logger.Info("command-code started", "pid", cmd.Process.Pid, "cwd", opts.Cwd, "model", opts.Model)

	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)

	// procDone closes once cmd.Wait() returns, letting the cancellation
	// handler skip a process that already exited.
	procDone := make(chan struct{})

	// Write the prompt from its own goroutine so it cannot deadlock against
	// the stdout reader below — see opencodeBackend's identical comment.
	// Command Code reads piped stdin to EOF when -p is given without a query
	// argument (confirmed against the CLI's headless-mode docs).
	writeErrCh := make(chan error, 1)
	go func() {
		_, err := io.WriteString(stdin, prompt)
		closeStdin()
		writeErrCh <- err
	}()

	// On cancellation / timeout, terminate command-code (and its tool
	// subprocesses) BEFORE unblocking the scanner — see opencodeBackend's
	// identical comment for why closing stdout first is unsafe (#4533).
	go func() {
		select {
		case <-procDone:
			return
		case <-runCtx.Done():
		}
		closeStdin()
		if cmd.Process != nil {
			signalProcessGroup(cmd, syscall.SIGTERM)
			select {
			case <-procDone:
			case <-time.After(commandcodeTerminateGrace):
				signalProcessGroup(cmd, syscall.SIGKILL)
			}
		}
		_ = stdout.Close()
	}()

	go func() {
		defer cancel()
		defer close(msgCh)
		defer close(resCh)

		startTime := time.Now()
		scanResult := b.processEvents(stdout, msgCh)

		exitErr := cmd.Wait()
		close(procDone)
		duration := time.Since(startTime)

		writeErr := <-writeErrCh

		if runCtx.Err() == context.DeadlineExceeded {
			scanResult.status = "timeout"
			scanResult.errMsg = fmt.Sprintf("command-code timed out after %s", timeout)
		} else if runCtx.Err() == context.Canceled {
			scanResult.status = "aborted"
			scanResult.errMsg = "execution cancelled"
		} else if exitErr != nil && scanResult.status == "completed" {
			scanResult.status = "failed"
			scanResult.errMsg = withAgentStderr(fmt.Sprintf("command-code exited with error: %v", exitErr), "command-code", stderr.Tail())
		} else if exitErr != nil && scanResult.noTerminalSignal {
			scanResult.errMsg = withAgentStderr(fmt.Sprintf("%s; command-code exited with error: %v", scanResult.errMsg, exitErr), "command-code", stderr.Tail())
		} else if writeErr != nil && !scanResult.sawTerminalSignal {
			// A failed prompt write is only benign once the run is PROVEN to
			// have finished — see opencodeBackend's identical comment.
			if scanResult.errMsg == "" {
				scanResult.errMsg = fmt.Sprintf("command-code prompt write failed: %v", writeErr)
			} else {
				scanResult.errMsg = fmt.Sprintf("%s; command-code prompt write failed: %v", scanResult.errMsg, writeErr)
			}
			scanResult.status = "failed"
		}

		b.cfg.Logger.Info("command-code finished", "pid", cmd.Process.Pid, "status", scanResult.status, "duration", duration.Round(time.Millisecond).String())

		var usage map[string]TokenUsage
		if len(scanResult.usage) > 0 {
			usage = scanResult.usage
		}

		resCh <- Result{
			Status:     scanResult.status,
			Output:     scanResult.output,
			Error:      scanResult.errMsg,
			DurationMs: duration.Milliseconds(),
			SessionID:  scanResult.sessionID,
			Usage:      usage,
		}
	}()

	return &Session{Messages: msgCh, Result: resCh}, nil
}

// ── Event handlers ──

// commandcodeEventResult holds the accumulated state from processing the
// event stream.
type commandcodeEventResult struct {
	status    string
	errMsg    string
	output    string
	sessionID string
	usage     map[string]TokenUsage // accumulated per-model, from model_request_end events
	// noTerminalSignal fires when the stream ended without a `result` frame —
	// the positive terminal signal this protocol reports. Mirrors
	// opencodeBackend.eventResult.noTerminalSignal.
	noTerminalSignal bool
	// sawTerminalSignal is positive evidence a `result` frame was parsed,
	// independent of noTerminalSignal (see opencodeBackend's identical field
	// for why these are not each other's negation).
	sawTerminalSignal bool
}

// processEvents reads JSON lines from r, dispatches events to ch, and
// returns the accumulated result. Extracted from Execute for testability.
func (b *commandcodeBackend) processEvents(r io.Reader, ch chan<- Message) commandcodeEventResult {
	var output strings.Builder
	var sessionID string
	usage := make(map[string]TokenUsage)
	finalStatus := "completed"
	var finalError string
	sawResult := false

	scanner := newAgentStreamScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var envelope commandcodeEnvelope
		if err := json.Unmarshal([]byte(line), &envelope); err != nil {
			continue
		}

		switch envelope.Type {
		case "event":
			if envelope.Event == nil {
				continue
			}
			b.handleEvent(*envelope.Event, ch, &output, usage)
			if envelope.Event.Type == "run_start" && envelope.Event.SessionID != "" {
				sessionID = envelope.Event.SessionID
			}
		case "result":
			sawResult = true
			if envelope.SessionID != "" {
				sessionID = envelope.SessionID
			}
			// finalText is the CLI's own canonical answer for the run;
			// prefer it over the text reconstructed from streamed deltas
			// (which this replaces).
			output.Reset()
			output.WriteString(envelope.FinalText)
			switch envelope.Subtype {
			case "success":
				finalStatus = "completed"
			case "max_turns":
				finalStatus = "failed"
				finalError = "command-code hit --max-turns before finishing the run"
			default:
				// "error" and any future subtype fail closed rather than
				// silently reporting success.
				finalStatus = "failed"
				errText := commandcodeErrorText(envelope.Error)
				if errText == "" {
					errText = fmt.Sprintf("command-code run failed (subtype %q)", envelope.Subtype)
				}
				finalError = errText
				trySend(ch, Message{Type: MessageError, Content: finalError})
			}
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		b.cfg.Logger.Warn("command-code stdout scanner error", "error", scanErr)
		if finalStatus == "completed" {
			finalStatus = "failed"
			finalError = fmt.Sprintf("stdout read error: %v", scanErr)
		}
	}

	noTerminalSignal := false
	if finalStatus == "completed" && !sawResult {
		// The stream ended without the terminal `result` frame this
		// protocol always emits on a real completion — most commonly a
		// pre-flight failure (invalid model, auth) that exits before
		// entering the event loop at all. Fail closed rather than report a
		// false-green completion; the caller appends the process exit
		// detail / stderr tail.
		finalStatus = "failed"
		finalError = "command-code stream ended without a terminal result frame"
		noTerminalSignal = true
	}

	return commandcodeEventResult{
		status:            finalStatus,
		errMsg:            finalError,
		output:            output.String(),
		sessionID:         sessionID,
		usage:             usage,
		noTerminalSignal:  noTerminalSignal,
		sawTerminalSignal: sawResult,
	}
}

// handleEvent dispatches a single AgentEvent to ch and/or accumulates usage.
//
// Every event type NOT handled below is intentionally not surfaced:
// message_start, model_request_start, and model_trace carry no user-facing
// content; thinking_start/thinking_end and message_update/message_end are
// snapshots/echoes of content already streamed through thinking_delta,
// text_delta, tool_queued, and tool_completed; turn_end and run_end
// duplicate data available elsewhere (turn_end's usage would double-count
// against model_request_end, run_end's result mirrors the top-level
// `result` frame handled in processEvents). Unrecognised future event types
// fall through here too — Command Code's own docs ask consumers to treat
// unknown event.type values as forward-compatible.
func (b *commandcodeBackend) handleEvent(event commandcodeEvent, ch chan<- Message, output *strings.Builder, usage map[string]TokenUsage) {
	switch event.Type {
	case "run_start":
		trySend(ch, Message{Type: MessageStatus, Status: "running", SessionID: event.SessionID})
	case "turn_start":
		trySend(ch, Message{Type: MessageStatus, Status: "running"})
	case "thinking_delta":
		if event.Delta != "" {
			trySend(ch, Message{Type: MessageThinking, Content: event.Delta})
		}
	case "text_delta":
		if event.Delta != "" {
			output.WriteString(event.Delta)
			trySend(ch, Message{Type: MessageText, Content: event.Delta})
		}
	case "tool_queued":
		trySend(ch, Message{Type: MessageToolUse, Tool: event.ToolName, CallID: event.ToolCallID, Input: event.Input})
	case "tool_completed":
		trySend(ch, Message{Type: MessageToolResult, Tool: event.ToolName, CallID: event.ToolCallID, Output: commandcodeToolResultText(event.Result)})
	case "api_retry":
		msg := fmt.Sprintf("retrying model request (attempt %d): %s", event.Attempt, event.Error)
		b.cfg.Logger.Warn("command-code api_retry", "attempt", event.Attempt, "error", event.Error, "delayMs", event.DelayMs)
		trySend(ch, Message{Type: MessageLog, Content: msg, Level: "warn"})
	case "model_request_end":
		if event.Usage != nil && event.Model != "" {
			u := usage[event.Model]
			u.InputTokens += event.Usage.InputTokens
			u.OutputTokens += event.Usage.OutputTokens
			u.CacheReadTokens += event.Usage.CacheReadTokens
			u.CacheWriteTokens += event.Usage.CacheWriteTokens
			usage[event.Model] = u
		}
	}
}

// commandcodeToolResultText flattens a tool_completed event's `result`
// array (content-block shaped, matching every text part observed so far)
// into a single string. Non-text parts are JSON-encoded verbatim rather
// than dropped, since no non-text part shape has been observed to model
// more precisely.
func commandcodeToolResultText(parts []commandcodeToolResultPart) string {
	if len(parts) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 {
			sb.WriteString("\n")
		}
		if p.Type == "text" || p.Type == "" {
			sb.WriteString(p.Text)
		} else {
			data, _ := json.Marshal(p)
			sb.Write(data)
		}
	}
	return sb.String()
}

// commandcodeErrorText extracts a human-readable message from a result
// frame's `error` field. The field's shape on a real error subtype has not
// been observed (see the package-level backend comment), so this accepts a
// bare string, a `{message}` / `{error}` object, or falls back to the raw
// JSON rather than silently dropping the diagnostic.
func commandcodeErrorText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var obj struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		if obj.Message != "" {
			return obj.Message
		}
		if obj.Error != "" {
			return obj.Error
		}
	}
	return string(raw)
}

// ── JSON types for `command-code -p --output-format json` stdout events ──
//
// Captured verbatim from a live Command Code v1.26.0 run (see the
// commandcodeBackend doc comment) — not transcribed from documentation,
// which publishes only two example frames.

// commandcodeEnvelope is the outer shape of every NDJSON line:
// {"type":"event","event":{...}} for every line but the last, and
// {"type":"result",...} for exactly one, final line.
type commandcodeEnvelope struct {
	Type  string            `json:"type"`
	Event *commandcodeEvent `json:"event,omitempty"`
	// Fields below are only populated when Type == "result".
	Subtype    string            `json:"subtype,omitempty"`
	SessionID  string            `json:"sessionId,omitempty"`
	StopReason string            `json:"stopReason,omitempty"`
	Usage      *commandcodeUsage `json:"usage,omitempty"`
	DurationMs int64             `json:"durationMs,omitempty"`
	FinalText  string            `json:"finalText,omitempty"`
	// Error's shape on a real failure is unconfirmed; kept raw and decoded
	// defensively by commandcodeErrorText.
	Error json.RawMessage `json:"error,omitempty"`
}

// commandcodeEvent is the inner event object. Fields are a superset across
// every event.type observed: run_start, turn_start, message_start,
// model_request_start, model_trace, api_retry, thinking_start,
// thinking_delta, thinking_end, text_delta, message_update,
// model_request_end, message_end, tool_queued, tool_running, tool_completed,
// turn_end, run_end.
type commandcodeEvent struct {
	Type string `json:"type"`

	SessionID string `json:"sessionId,omitempty"` // run_start

	Model string            `json:"model,omitempty"` // model_request_start, model_request_end
	Usage *commandcodeUsage `json:"usage,omitempty"` // model_request_end, turn_end (per-request, not cumulative)

	Delta string `json:"delta,omitempty"` // thinking_delta, text_delta
	Text  string `json:"text,omitempty"`  // thinking_end (full accumulated thinking; unused, see handleEvent)

	ToolCallID string                      `json:"toolCallId,omitempty"` // tool_queued, tool_running, tool_completed
	ToolName   string                      `json:"toolName,omitempty"`
	Input      map[string]any              `json:"input,omitempty"`  // tool_queued
	Result     []commandcodeToolResultPart `json:"result,omitempty"` // tool_completed

	Attempt int    `json:"attempt,omitempty"` // api_retry
	Error   string `json:"error,omitempty"`   // api_retry (plain string here, unlike the result frame's field)
	DelayMs int64  `json:"delayMs,omitempty"` // api_retry
}

// commandcodeUsage is the token-usage shape shared by model_request_end,
// turn_end, and the top-level result frame.
type commandcodeUsage struct {
	InputTokens      int64 `json:"inputTokens"`
	OutputTokens     int64 `json:"outputTokens"`
	CacheReadTokens  int64 `json:"cacheReadTokens"`
	CacheWriteTokens int64 `json:"cacheWriteTokens"`
}

// commandcodeToolResultPart is one content block of a tool_completed
// event's `result` array — every observed instance has been {"type":"text"}.
type commandcodeToolResultPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
