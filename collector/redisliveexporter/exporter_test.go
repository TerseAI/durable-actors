package redisliveexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestTracesAreSplitByStampedProject(t *testing.T) {
	traces := ptrace.NewTraces()
	for _, project := range []string{"alpha", "beta", "alpha", ""} {
		spans := traces.ResourceSpans().AppendEmpty()
		if project != "" {
			spans.Resource().Attributes().PutStr(projectAttribute, project)
		}
		spans.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName(project + "-span")
	}
	payloads, err := tracesByProject(traces)
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 2 {
		t.Fatalf("expected two projects, got %d", len(payloads))
	}
	request := ptraceotlp.NewExportRequest()
	if err := request.UnmarshalProto(payloads["alpha"]); err != nil {
		t.Fatal(err)
	}
	if got := request.Traces().SpanCount(); got != 2 {
		t.Fatalf("expected both alpha spans, got %d", got)
	}
}

func TestLogsAreSplitByStampedProject(t *testing.T) {
	logs := plog.NewLogs()
	for _, project := range []string{"alpha", "beta"} {
		records := logs.ResourceLogs().AppendEmpty()
		records.Resource().Attributes().PutStr(projectAttribute, project)
		records.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(project + " line")
	}
	payloads, err := logsByProject(logs)
	if err != nil {
		t.Fatal(err)
	}
	request := plogotlp.NewExportRequest()
	if err := request.UnmarshalProto(payloads["beta"]); err != nil {
		t.Fatal(err)
	}
	body := request.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
	if body != "beta line" || request.Logs().LogRecordCount() != 1 {
		t.Fatalf("unexpected beta payload %q", body)
	}
}

func TestChannelMatchesTheControlPlane(t *testing.T) {
	if got := Channel("default", "logs"); got != "durable-actors:v1:live:default:logs" {
		t.Fatalf("unexpected channel %q", got)
	}
}

func TestConfigRequiresARedisURL(t *testing.T) {
	if (&Config{}).Validate() == nil {
		t.Fatal("empty endpoint accepted")
	}
	if (&Config{Endpoint: "http://redis"}).Validate() == nil {
		t.Fatal("non-Redis endpoint accepted")
	}
	if err := (&Config{Endpoint: "rediss://:secret@redis:6380/0"}).Validate(); err != nil {
		t.Fatal(err)
	}
}
