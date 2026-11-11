package redisliveexporter

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func TestTelemetryIsSplitByStampedProject(t *testing.T) {
	traces := ptrace.NewTraces()
	logs := plog.NewLogs()
	for _, project := range []string{"alpha", "beta", "alpha", ""} {
		spans := traces.ResourceSpans().AppendEmpty()
		records := logs.ResourceLogs().AppendEmpty()
		if project != "" {
			spans.Resource().Attributes().PutStr(projectAttribute, project)
			records.Resource().Attributes().PutStr(projectAttribute, project)
		}
		spans.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName(project)
		records.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(project)
	}

	spanPayloads, err := tracesByProject(traces)
	if err != nil {
		t.Fatal(err)
	}
	spanRequest := ptraceotlp.NewExportRequest()
	if err := spanRequest.UnmarshalProto(spanPayloads["alpha"]); err != nil {
		t.Fatal(err)
	}
	if len(spanPayloads) != 2 || spanRequest.Traces().SpanCount() != 2 {
		t.Fatalf("expected alpha and beta with both alpha spans, got %d projects", len(spanPayloads))
	}

	logPayloads, err := logsByProject(logs, false)
	if err != nil {
		t.Fatal(err)
	}
	logRequest := plogotlp.NewExportRequest()
	if err := logRequest.UnmarshalProto(logPayloads["beta"]); err != nil {
		t.Fatal(err)
	}
	if len(logPayloads) != 2 || logRequest.Logs().LogRecordCount() != 1 {
		t.Fatalf("expected alpha and beta with one beta line, got %d projects", len(logPayloads))
	}
}

func TestGatewayRecordsAreSplitByTheirOwnProjectOnlyWhenTrusted(t *testing.T) {
	logs := plog.NewLogs()
	gateway := logs.ResourceLogs().AppendEmpty()
	gateway.Resource().Attributes().PutStr("durable_actors.gateway_id", "gateway-1")
	records := gateway.ScopeLogs().AppendEmpty().LogRecords()
	for _, project := range []string{"alpha", "beta", "alpha", ""} {
		record := records.AppendEmpty()
		if project != "" {
			record.Attributes().PutStr(projectAttribute, project)
		}
	}

	untrusted, err := logsByProject(logs, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(untrusted) != 0 {
		t.Fatalf("records without a stamped resource project must be dropped, got %d projects", len(untrusted))
	}
	trusted, err := logsByProject(logs, true)
	if err != nil {
		t.Fatal(err)
	}
	request := plogotlp.NewExportRequest()
	if err := request.UnmarshalProto(trusted["alpha"]); err != nil {
		t.Fatal(err)
	}
	gatewayID, _ := request.Logs().ResourceLogs().At(0).Resource().Attributes().Get("durable_actors.gateway_id")
	if len(trusted) != 2 || request.Logs().LogRecordCount() != 2 || gatewayID.AsString() != "gateway-1" {
		t.Fatalf("expected alpha and beta, alpha with both records and the gateway resource, got %d projects", len(trusted))
	}
}
