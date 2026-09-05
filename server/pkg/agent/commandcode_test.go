package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestNewCommandcodeBackend(t *testing.T) {
	b, err := New("commandcode", Config{ExecutablePath: "/nonexistent/command-code"})
	if err != nil {
		t.Fatalf("New(commandcode): %v", err)
	}
	if _, ok := b.(*commandcodeBackend); !ok {
		t.Fatalf("New(commandcode) returned %T", b)
	}
}

// The NDJSON bodies below are trimmed/reordered excerpts of a real capture
// from `command-code -p ... --output-format json --yolo` (v1.26.0) — not
// hand-guessed shapes. See the commandcodeBackend doc comment.

func TestCommandcodeBackendExecuteStreamsProtocol(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture")
	}
	bin := writeCommandcodeFixture(t, `
printf '%s\n' '{"type":"event","event":{"type":"run_start","sessionId":"bea99f10-7299-4d7f-8f49-10da28915f31"}}'
printf '%s\n' '{"type":"event","event":{"type":"turn_start","turnNumber":1}}'
printf '%s\n' '{"type":"event","event":{"type":"thinking_delta","delta":"The"}}'
printf '%s\n' '{"type":"event","event":{"type":"thinking_delta","delta":" plan"}}'
printf '%s\n' '{"type":"event","event":{"type":"text_delta","delta":"I"}}'
printf '%s\n' '{"type":"event","event":{"type":"text_delta","delta":"'"'"'ll help"}}'
printf '%s\n' '{"type":"event","event":{"type":"tool_queued","toolCallId":"call-1","toolName":"write_file","input":{"file_path":"hello.txt","content":"hello world"}}}'
printf '%s\n' '{"type":"event","event":{"type":"tool_running","toolCallId":"call-1","toolName":"write_file","description":null}}'
printf '%s\n' '{"type":"event","event":{"type":"tool_completed","toolCallId":"call-1","toolName":"write_file","result":[{"type":"text","text":"File created successfully at: hello.txt"}],"deferred":false}}'
printf '%s\n' '{"type":"event","event":{"type":"model_request_end","model":"poolside/laguna-s-2.1-free","usage":{"inputTokens":14628,"outputTokens":144,"cacheReadTokens":2624,"cacheWriteTokens":0},"stopReason":"tool_calls"}}'
printf '%s\n' '{"type":"event","event":{"type":"turn_end","turnNumber":1,"hadToolCalls":true}}'
printf '%s\n' '{"type":"event","event":{"type":"turn_start","turnNumber":2}}'
printf '%s\n' '{"type":"event","event":{"type":"model_request_end","model":"poolside/laguna-s-2.1-free","usage":{"inputTokens":29794,"outputTokens":106,"cacheReadTokens":29664,"cacheWriteTokens":0},"stopReason":"end_turn"}}'
printf '%s\n' '{"type":"result","subtype":"success","sessionId":"bea99f10-7299-4d7f-8f49-10da28915f31","stopReason":"end_turn","usage":{"inputTokens":44422,"outputTokens":250,"cacheReadTokens":32288,"cacheWriteTokens":0},"durationMs":24456,"finalText":"Done! Created hello.txt."}'
`)
	b, err := New("commandcode", Config{ExecutablePath: bin, TaskID: "task-1", Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := b.Execute(context.Background(), "create hello.txt", ExecOptions{Cwd: t.TempDir(), Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var messages []Message
	for message := range session.Messages {
		messages = append(messages, message)
	}
	result := <-session.Result

	if result.Status != "completed" || result.SessionID != "bea99f10-7299-4d7f-8f49-10da28915f31" {
		t.Fatalf("bad result: %#v", result)
	}
	if result.Output != "Done! Created hello.txt." {
		t.Fatalf("bad output: %q", result.Output)
	}
	// Two model_request_end events for the same model must aggregate, not
	// overwrite — this is the whole point of keying Usage by model.
	usage := result.Usage["poolside/laguna-s-2.1-free"]
	if usage.InputTokens != 14628+29794 || usage.OutputTokens != 144+106 || usage.CacheReadTokens != 2624+29664 {
		t.Fatalf("bad aggregated usage: %#v", usage)
	}

	var gotThinking, gotText, gotToolUse, gotToolResult, gotStatus int
	for _, m := range messages {
		switch m.Type {
		case MessageThinking:
			gotThinking++
		case MessageText:
			gotText++
		case MessageToolUse:
			gotToolUse++
			if m.Tool != "write_file" || m.CallID != "call-1" || m.Input["file_path"] != "hello.txt" {
				t.Fatalf("bad tool-use message: %#v", m)
			}
		case MessageToolResult:
			gotToolResult++
			if m.Tool != "write_file" || m.CallID != "call-1" || !strings.Contains(m.Output, "File created successfully") {
				t.Fatalf("bad tool-result message: %#v", m)
			}
		case MessageStatus:
			gotStatus++
			if m.SessionID != "" && m.SessionID != "bea99f10-7299-4d7f-8f49-10da28915f31" {
				t.Fatalf("bad status message session id: %#v", m)
			}
		}
	}
	if gotThinking != 2 || gotText != 2 || gotToolUse != 1 || gotToolResult != 1 || gotStatus == 0 {
		t.Fatalf("unexpected message mix: thinking=%d text=%d toolUse=%d toolResult=%d status=%d (%#v)",
			gotThinking, gotText, gotToolUse, gotToolResult, gotStatus, messages)
	}
	// run_start must pin the session id early, before the terminal result
	// frame arrives — see Message.SessionID's doc comment on early
	// resume-pointer pinning.
	if messages[0].Type != MessageStatus || messages[0].SessionID != "bea99f10-7299-4d7f-8f49-10da28915f31" {
		t.Fatalf("expected first message to carry the session id from run_start, got %#v", messages[0])
	}
}

func TestCommandcodeBackendMaxTurnsSubtype(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture")
	}
	bin := writeCommandcodeFixture(t, `
printf '%s\n' '{"type":"event","event":{"type":"run_start","sessionId":"s-max-turns"}}'
printf '%s\n' '{"type":"result","subtype":"max_turns","sessionId":"s-max-turns","stopReason":"max_turns","usage":{"inputTokens":1,"outputTokens":1,"cacheReadTokens":0,"cacheWriteTokens":0},"durationMs":1,"finalText":""}'
`)
	b, err := New("commandcode", Config{ExecutablePath: bin, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := b.Execute(context.Background(), "loop forever", ExecOptions{Cwd: t.TempDir(), Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for range session.Messages {
	}
	result := <-session.Result
	if result.Status != "failed" || !strings.Contains(result.Error, "max-turns") {
		t.Fatalf("bad max_turns result: %#v", result)
	}
}

func TestCommandcodeBackendErrorSubtype(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture")
	}
	bin := writeCommandcodeFixture(t, `
printf '%s\n' '{"type":"event","event":{"type":"run_start","sessionId":"s-error"}}'
printf '%s\n' '{"type":"result","subtype":"error","sessionId":"s-error","usage":{"inputTokens":0,"outputTokens":0,"cacheReadTokens":0,"cacheWriteTokens":0},"durationMs":1,"finalText":"","error":"insufficient credits"}'
`)
	b, err := New("commandcode", Config{ExecutablePath: bin, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := b.Execute(context.Background(), "hello", ExecOptions{Cwd: t.TempDir(), Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var sawErrorMessage bool
	for m := range session.Messages {
		if m.Type == MessageError && m.Content == "insufficient credits" {
			sawErrorMessage = true
		}
	}
	result := <-session.Result
	if result.Status != "failed" || result.Error != "insufficient credits" {
		t.Fatalf("bad error result: %#v", result)
	}
	if !sawErrorMessage {
		t.Fatal("expected a MessageError with the result frame's error text")
	}
}

// TestCommandcodeBackendNoTerminalResult covers the pre-flight-failure shape
// observed live: an invalid --model exits 1 with a plain-text stderr line
// and ZERO stdout — no JSON at all, let alone a result frame. This must fail
// closed rather than report a false-green "completed".
func TestCommandcodeBackendNoTerminalResult(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture")
	}
	bin := writeCommandcodeFixture(t, `
echo 'Error: unknown model "totally-not-a-real-model-xyz".' >&2
exit 1
`)
	b, err := New("commandcode", Config{ExecutablePath: bin, Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := b.Execute(context.Background(), "hello", ExecOptions{Cwd: t.TempDir(), Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for range session.Messages {
	}
	result := <-session.Result
	if result.Status != "failed" {
		t.Fatalf("expected failed status for a stream with no terminal result frame, got %#v", result)
	}
	if !strings.Contains(result.Error, "unknown model") {
		t.Fatalf("expected the stderr tail to surface the real cause, got error: %q", result.Error)
	}
}

func TestCommandcodeToolResultText(t *testing.T) {
	got := commandcodeToolResultText([]commandcodeToolResultPart{
		{Type: "text", Text: "line one"},
		{Type: "text", Text: "line two"},
	})
	if got != "line one\nline two" {
		t.Fatalf("bad joined text: %q", got)
	}
	if commandcodeToolResultText(nil) != "" {
		t.Fatal("expected empty string for no result parts")
	}
}

func TestCommandcodeErrorText(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`"plain string error"`, "plain string error"},
		{`{"message":"structured error"}`, "structured error"},
		{`{"error":"nested error field"}`, "nested error field"},
		{``, ""},
	}
	for _, c := range cases {
		if got := commandcodeErrorText(json.RawMessage(c.raw)); got != c.want {
			t.Errorf("commandcodeErrorText(%q) = %q, want %q", c.raw, got, c.want)
		}
	}
}

// TestCommandcodeBlockedArgsCoverage guards against a hardcoded flag losing
// its blocklist entry: every flag this backend hardcodes in Execute's args
// slice must also appear in commandcodeBlockedArgs, or a user's custom_args
// could inject a conflicting duplicate.
func TestCommandcodeBlockedArgsCoverage(t *testing.T) {
	hardcoded := []string{"-p", "--output-format", "--yolo", "--skip-onboarding", "--no-auto-update"}
	for _, flag := range hardcoded {
		if _, ok := commandcodeBlockedArgs[flag]; !ok {
			t.Errorf("hardcoded flag %q is missing from commandcodeBlockedArgs", flag)
		}
	}
}

func writeCommandcodeFixture(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "command-code")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}
