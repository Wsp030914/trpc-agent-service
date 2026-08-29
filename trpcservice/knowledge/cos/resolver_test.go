package cos

import (
	"bytes"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestNewDocumentGeneratesScopedImmutableObjectKey(t *testing.T) {
	scope := tenant.Scope{TenantID: "tenant/a", AppID: "app:b"}
	document, err := NewDocument(
		scope,
		"handbook",
		"employee-guide",
		1,
		[]byte("policy"),
		"text/plain",
		"g1",
	)
	if err != nil {
		t.Fatalf("new document: %v", err)
	}
	if document.ObjectKey == "" || bytes.Contains([]byte(document.ObjectKey), []byte("tenant/a")) {
		t.Fatalf("object key = %q", document.ObjectKey)
	}
	if len(document.ContentSHA256) != 32 {
		t.Fatalf("content hash length = %d", len(document.ContentSHA256))
	}
	again, err := NewDocument(scope, "handbook", "employee-guide", 1, []byte("policy"), "text/plain", "g1")
	if err != nil {
		t.Fatalf("new repeated document: %v", err)
	}
	if document.ObjectKey != again.ObjectKey {
		t.Fatalf("object keys differ: %q != %q", document.ObjectKey, again.ObjectKey)
	}
}
