package migration

import (
	"encoding/json"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestEqualEventsNormalizesPersistedRepresentations(t *testing.T) {
	t.Parallel()
	now := time.Now()
	withoutMonotonic := time.Unix(0, now.UnixNano()).In(time.FixedZone("offset", 8*60*60))
	left := []event.Event{{
		ID:         "event-1",
		Version:    2,
		Author:     "assistant",
		Timestamp:  now,
		StateDelta: nil,
	}}
	right := []event.Event{{
		ID:         "event-1",
		Version:    2,
		Author:     "assistant",
		Timestamp:  withoutMonotonic,
		StateDelta: map[string][]byte{},
	}}
	if !equalEvents(left, right) {
		t.Fatal("equivalent persisted events do not match")
	}
}

func TestEqualEventsRejectsSemanticChanges(t *testing.T) {
	t.Parallel()
	timestamp := time.Now().UTC()
	base := []event.Event{
		{ID: "event-1", Version: 1, Author: "user", Timestamp: timestamp},
		{ID: "event-2", Version: 1, Author: "assistant", Timestamp: timestamp.Add(time.Second)},
	}
	for name, mutate := range map[string]func([]event.Event){
		"content": func(events []event.Event) { events[0].Author = "other" },
		"version": func(events []event.Event) { events[0].Version++ },
		"order":   func(events []event.Event) { events[0], events[1] = events[1], events[0] },
	} {
		t.Run(name, func(t *testing.T) {
			right := append([]event.Event(nil), base...)
			mutate(right)
			if equalEvents(base, right) {
				t.Fatal("semantic event change was accepted")
			}
		})
	}
}

func TestEqualEventsIgnoresResponseReceiveTimestamp(t *testing.T) {
	t.Parallel()
	now := time.Now()
	left := event.Event{
		ID:        "event-1",
		Version:   1,
		Timestamp: now,
		Response:  &model.Response{ID: "response-1", Timestamp: now.Add(-time.Minute)},
	}
	right := left
	right.Timestamp = time.Unix(0, now.UnixNano()).UTC()
	right.Response = &model.Response{ID: "response-1", Timestamp: now.Add(time.Hour)}
	if !equalEvents([]event.Event{left}, []event.Event{right}) {
		t.Fatal("provider response timestamp representation changed semantic event")
	}
}

func TestEqualTracksUsesCanonicalPayloadAndSequence(t *testing.T) {
	t.Parallel()
	now := time.Now()
	left := map[session.Track]*session.TrackEvents{
		"trace": {Track: "trace", Events: []session.TrackEvent{{
			Track:     "trace",
			Payload:   json.RawMessage(`{"a":1,"b":[2]}`),
			Timestamp: now,
		}}},
	}
	right := map[session.Track]*session.TrackEvents{
		"trace": {Track: "trace", Events: []session.TrackEvent{{
			Track:     "trace",
			Payload:   json.RawMessage(` { "b": [2], "a": 1 } `),
			Timestamp: time.Unix(0, now.UnixNano()).UTC(),
		}}},
	}
	if !equalTracks(left, right) {
		t.Fatal("equivalent track payloads do not match")
	}
	right["trace"].Events[0].Payload = json.RawMessage(`{"a":2,"b":[2]}`)
	if equalTracks(left, right) {
		t.Fatal("changed track payload was accepted")
	}
}

func TestEqualSummaryNormalizesTopicOrder(t *testing.T) {
	t.Parallel()
	now := time.Now()
	left := &session.Summary{Summary: "summary", Topics: []string{"agent", "user"}, UpdatedAt: now}
	right := &session.Summary{
		Summary:   "summary",
		Topics:    []string{"user", "agent"},
		UpdatedAt: time.Unix(0, now.UnixNano()).UTC(),
	}
	if !equalSummary(left, right) {
		t.Fatal("equivalent summary topic sets do not match")
	}
}

func TestSemanticCollectionsTreatNilAndEmptyAsEquivalent(t *testing.T) {
	t.Parallel()
	if !equalTracks(nil, map[session.Track]*session.TrackEvents{}) {
		t.Fatal("nil and empty tracks do not match")
	}
	if !equalSummaries(nil, map[string]*session.Summary{}) {
		t.Fatal("nil and empty summaries do not match")
	}
}

func TestEqualStateRequiresMatchingKeys(t *testing.T) {
	t.Parallel()
	if equalState(session.StateMap{"left": nil}, session.StateMap{"right": nil}) {
		t.Fatal("state maps with different keys match")
	}
}
