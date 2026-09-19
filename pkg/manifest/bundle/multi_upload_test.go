package bundle

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type exactEvent struct {
	kind       string
	generation store.Generation
	put        recordedPut
}

type multiExactStore struct {
	mu sync.Mutex

	accepted map[store.Generation]store.WriteAdmission
	admitErr map[store.Generation]error
	events   []exactEvent
	pool     int
}

func (s *multiExactStore) PoolSize() int { return s.pool }

func (s *multiExactStore) AdmitWriteFor(_ context.Context, generation store.Generation) (store.WriteAdmission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, exactEvent{kind: "admit", generation: generation})
	if err := s.admitErr[generation]; err != nil {
		return store.WriteAdmission{}, err
	}
	return s.accepted[generation], nil
}

func (s *multiExactStore) Put(_ context.Context, admission store.WriteAdmission, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	put := recordedPut{admission: admission, partition: partition, key: key, data: append([]byte(nil), data...)}
	s.events = append(s.events, exactEvent{kind: "put", generation: admission.Generation, put: put})
	return true, nil
}

func (s *multiExactStore) snapshot() []exactEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]exactEvent(nil), s.events...)
}

func TestUploadExactManifestsUsesEverySourceAdmissionAndRootLast(t *testing.T) {
	a := newTestFixture(t, "A")
	b := newTestFixture(t, "B")
	c := newTestFixture(t, "C")
	defer a.reader.Close()
	defer b.reader.Close()
	defer c.reader.Close()
	target := &multiExactStore{
		accepted: map[store.Generation]store.WriteAdmission{
			a.admission.Generation: a.admission,
			b.admission.Generation: b.admission,
			c.admission.Generation: c.admission,
		},
		pool: 3,
	}
	root := ExactManifest{Key: c.root, Reader: c.reader}
	dependencies := []ExactManifest{{Key: a.root, Reader: a.reader}, {Key: b.root, Reader: b.reader}}
	if err := UploadExactManifests(context.Background(), root, dependencies, c.customer, c.decryptor, target, VerifyOptions{}); err != nil {
		t.Fatalf("UploadExactManifests: %v", err)
	}
	events := target.snapshot()
	if len(events) < 4 {
		t.Fatalf("events = %v", events)
	}
	for index, generation := range []store.Generation{"A", "B", "C"} {
		if events[index].kind != "admit" || events[index].generation != generation {
			t.Fatalf("event[%d] = %#v, want admission %q", index, events[index], generation)
		}
	}
	seenManifest := false
	for index, event := range events[3:] {
		if event.kind != "put" {
			t.Fatalf("event[%d] = %#v after preflight", index+3, event)
		}
		if event.put.partition == store.PartitionManifest {
			seenManifest = true
		} else if seenManifest {
			t.Fatalf("Chunk Put occurred after Manifest at event %d", index+3)
		}
	}
	last := events[len(events)-1]
	if last.put.partition != store.PartitionManifest || last.put.key != c.root || last.put.admission != c.admission {
		t.Fatalf("last event = %#v, want current root", last)
	}
	manifestPuts := map[store.ContentKey]store.WriteAdmission{}
	for _, event := range events {
		if event.kind == "put" && event.put.partition == store.PartitionManifest {
			manifestPuts[event.put.key] = event.put.admission
		}
	}
	for _, fixture := range []testFixture{a, b, c} {
		if got := manifestPuts[fixture.root]; got != fixture.admission {
			t.Fatalf("Manifest %x admission = %#v, want %#v", fixture.root, got, fixture.admission)
		}
	}
}

func TestUploadExactManifestsPreflightsAllAdmissionsBeforePut(t *testing.T) {
	a := newTestFixture(t, "A")
	b := newTestFixture(t, "B")
	defer a.reader.Close()
	defer b.reader.Close()
	removed := errors.New("generation removed")
	target := &multiExactStore{
		accepted: map[store.Generation]store.WriteAdmission{"A": a.admission},
		admitErr: map[store.Generation]error{"B": removed},
	}
	err := UploadExactManifests(context.Background(), ExactManifest{Key: b.root, Reader: b.reader}, []ExactManifest{{Key: a.root, Reader: a.reader}}, b.customer, b.decryptor, target, VerifyOptions{})
	if !errors.Is(err, removed) {
		t.Fatalf("UploadExactManifests error = %v", err)
	}
	for _, event := range target.snapshot() {
		if event.kind == "put" {
			t.Fatal("Put occurred before all source admissions passed preflight")
		}
	}
}

func TestUploadExactManifestsStrictlyVerifiesEverySourceBeforeTargetContact(t *testing.T) {
	dependency := newTestFixture(t, "DEPENDENCY")
	root := newTestFixture(t, "ROOT")
	defer dependency.reader.Close()
	defer root.reader.Close()

	// Reordering CD records does not affect the hot tail-index Reader, but the
	// explicit verifier must reject it before exact upload contacts the Store.
	mutated := reorderFirstTwoCentralRecords(t, root.data)
	badRoot, err := NewReader(bytes.NewReader(mutated), int64(len(mutated)))
	if err != nil {
		t.Fatalf("hot Reader unexpectedly inspected Central Directory order: %v", err)
	}
	defer badRoot.Close()
	target := &multiExactStore{accepted: map[store.Generation]store.WriteAdmission{
		dependency.admission.Generation: dependency.admission,
		root.admission.Generation:       root.admission,
	}}
	err = UploadExactManifests(
		context.Background(),
		ExactManifest{Key: root.root, Reader: badRoot},
		[]ExactManifest{{Key: dependency.root, Reader: dependency.reader}},
		root.customer,
		root.decryptor,
		target,
		VerifyOptions{},
	)
	if err == nil || !strings.Contains(err.Error(), "strict source container verification") {
		t.Fatalf("UploadExactManifests error = %v", err)
	}
	if events := target.snapshot(); len(events) != 0 {
		t.Fatalf("target contacted before every source container passed strict verification: %v", events)
	}
}

func TestUploadExactManifestsChecksAdmissionButAllowsExtraSaltKeys(t *testing.T) {
	source := newTestFixture(t, "SOURCE")
	root := newTestFixture(t, "ROOT")
	defer source.reader.Close()
	defer root.reader.Close()

	t.Run("target salt mismatch", func(t *testing.T) {
		wrong := source.admission
		wrong.Salt[0] ^= 0xff
		target := &multiExactStore{accepted: map[store.Generation]store.WriteAdmission{
			source.admission.Generation: wrong,
			root.admission.Generation:   root.admission,
		}}
		err := UploadExactManifests(context.Background(), ExactManifest{Key: root.root, Reader: root.reader}, []ExactManifest{{Key: source.root, Reader: source.reader}}, root.customer, root.decryptor, target, VerifyOptions{})
		if err == nil || !strings.Contains(err.Error(), "does not exactly match") {
			t.Fatalf("UploadExactManifests error = %v", err)
		}
		for _, event := range target.snapshot() {
			if event.kind == "put" {
				t.Fatal("Put occurred after admission mismatch")
			}
		}
	})

	t.Run("authenticated keys need not derive from base admission salt", func(t *testing.T) {
		wrongSalt, err := store.SaltForGeneration("WRONG")
		if err != nil {
			t.Fatal(err)
		}
		wrongAdmission := store.WriteAdmission{Generation: "WRONG", Salt: wrongSalt}
		entries := []rawEntry{{name: admissionName(wrongAdmission)}}
		entries = append(entries, rawEntry{name: manifestPrefix + keyString(source.root), data: objectBytes(t, source.reader, store.PartitionManifest, source.root)})
		for _, key := range manifestChunkKeys(t, source.reader, source.root) {
			entries = append(entries, rawEntry{name: chunkPrefix + keyString(key), data: objectBytes(t, source.reader, store.PartitionChunk, key)})
		}
		data := rawZIP(t, entries, "")
		wrongReader, err := NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		defer wrongReader.Close()
		target := &multiExactStore{accepted: map[store.Generation]store.WriteAdmission{
			wrongAdmission.Generation: wrongAdmission,
			root.admission.Generation: root.admission,
		}, pool: 1}
		err = UploadExactManifests(context.Background(), ExactManifest{Key: root.root, Reader: root.reader}, []ExactManifest{{Key: source.root, Reader: wrongReader}}, root.customer, root.decryptor, target, VerifyOptions{Workers: 1})
		if err != nil {
			t.Fatalf("UploadExactManifests: %v", err)
		}
		publishedRoot := false
		for _, event := range target.snapshot() {
			if event.kind == "put" && event.put.partition == store.PartitionManifest && event.put.key == root.root {
				publishedRoot = true
			}
		}
		if !publishedRoot {
			t.Fatal("root Manifest was not published after successful authenticated upload")
		}
	})
}

func TestVerifyExactManifestsIgnoresUnselectedObjectsButRequiresClosure(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	if err := VerifyExactManifests(context.Background(), ExactManifest{Key: fixture.root, Reader: fixture.reader}, nil, fixture.customer, fixture.decryptor, VerifyOptions{}); err != nil {
		t.Fatalf("VerifyExactManifests rejected unselected sibling Manifest: %v", err)
	}
	incomplete := subsetBundle(t, fixture, []store.ContentKey{fixture.root}, nil, false)
	err := VerifyExactManifests(context.Background(), ExactManifest{Key: fixture.root, Reader: incomplete}, nil, fixture.customer, fixture.decryptor, VerifyOptions{})
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("VerifyExactManifests error = %v, want ErrIncomplete", err)
	}
	target := &multiExactStore{accepted: map[store.Generation]store.WriteAdmission{fixture.admission.Generation: fixture.admission}}
	err = UploadExactManifests(context.Background(), ExactManifest{Key: fixture.root, Reader: incomplete}, nil, fixture.customer, fixture.decryptor, target, VerifyOptions{})
	if !errors.Is(err, ErrIncomplete) {
		t.Fatalf("UploadExactManifests error = %v, want ErrIncomplete", err)
	}
	for _, event := range target.snapshot() {
		if event.kind == "put" {
			t.Fatal("exact upload performed a Put before validating the selected closure")
		}
	}
}

func TestUploadExactManifestsRejectsConflictingSourceAssignment(t *testing.T) {
	fixture := newTestFixture(t, "G1")
	defer fixture.reader.Close()
	copyReader := subsetBundle(t, fixture, []store.ContentKey{fixture.root}, nil, true)
	target := &multiExactStore{accepted: map[store.Generation]store.WriteAdmission{fixture.admission.Generation: fixture.admission}}
	err := UploadExactManifests(context.Background(), ExactManifest{Key: fixture.root, Reader: fixture.reader}, []ExactManifest{{Key: fixture.root, Reader: copyReader}}, fixture.customer, fixture.decryptor, target, VerifyOptions{})
	if err == nil || !strings.Contains(err.Error(), "multiple Bundle sources") {
		t.Fatalf("UploadExactManifests error = %v", err)
	}
	if len(target.snapshot()) != 0 {
		t.Fatal("target was contacted for an invalid source plan")
	}
}

var _ ExactStore = (*multiExactStore)(nil)

func TestVerifyAndUploadExactManifestsWithActualExtraSalt(t *testing.T) {
	dependency := newTestFixtureWithExtraSalt(t, "G1", []byte("dependency-domain"))
	root := newTestFixtureWithExtraSalt(t, "G2", []byte("root-domain"))
	defer dependency.reader.Close()
	defer root.reader.Close()
	selectedRoot := ExactManifest{Key: root.root, Reader: root.reader}
	selectedDeps := []ExactManifest{{Key: dependency.root, Reader: dependency.reader}}
	if err := VerifyExactManifests(context.Background(), selectedRoot, selectedDeps, root.customer, root.decryptor, VerifyOptions{}); err != nil {
		t.Fatalf("VerifyExactManifests with actual extra salt: %v", err)
	}
	target := &multiExactStore{accepted: map[store.Generation]store.WriteAdmission{
		dependency.admission.Generation: dependency.admission,
		root.admission.Generation:       root.admission,
	}, pool: 2}
	if err := UploadExactManifests(context.Background(), selectedRoot, selectedDeps, root.customer, root.decryptor, target, VerifyOptions{Workers: 2}); err != nil {
		t.Fatalf("UploadExactManifests with actual extra salt: %v", err)
	}
	events := target.snapshot()
	if len(events) < 3 {
		t.Fatalf("missing upload events: %v", events)
	}
	for _, event := range events {
		if event.kind != "put" {
			continue
		}
		reader := root.reader
		if event.put.admission == dependency.admission {
			reader = dependency.reader
		}
		expected := objectBytes(t, reader, event.put.partition, event.put.key)
		if !bytes.Equal(expected, event.put.data) {
			t.Fatal("exact upload rewrote encoded object")
		}
	}
	last := events[len(events)-1]
	if last.kind != "put" || last.put.partition != store.PartitionManifest || last.put.key != root.root {
		t.Fatalf("current root not published last: %#v", last)
	}
}
