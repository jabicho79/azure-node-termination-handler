package imds

import (
	"testing"
	"time"
)

func TestNotBeforeTime(t *testing.T) {
	e := Event{NotBefore: "Mon, 19 Sep 2016 18:29:47 GMT"}
	got, ok := e.NotBeforeTime()
	want := time.Date(2016, 9, 19, 18, 29, 47, 0, time.UTC)
	if !ok || !got.Equal(want) {
		t.Errorf("got %v, %v; want %v, true", got, ok, want)
	}
	if _, ok := (Event{}).NotBeforeTime(); ok {
		t.Error("empty NotBefore should be false")
	}
	if _, ok := (Event{NotBefore: "garbage"}).NotBeforeTime(); ok {
		t.Error("garbage NotBefore should be false")
	}
}

func TestAffectsCaseInsensitive(t *testing.T) {
	e := Event{Resources: []string{"Pool_3"}}
	if !e.Affects("pool_3") {
		t.Error("expected Affects(pool_3)")
	}
	if e.Affects("pool_4") {
		t.Error("unexpected Affects(pool_4)")
	}
}
