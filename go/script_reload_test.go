package bullmq

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// A load that fails must not be remembered as final: the old sync.Once cached the
// error forever, so a Redis blip during the very first flow add disabled the
// producer for the life of the process.
func TestScriptCacheRetriesAfterFailedLoad(t *testing.T) {
	var calls int
	boom := errors.New("redis is down")
	sc := &scriptCache{load: func(context.Context) error {
		calls++
		if calls == 1 {
			return boom
		}
		return nil
	}}

	if _, err := sc.ensure(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("first ensure err = %v, want %v", err, boom)
	}
	gen, err := sc.ensure(context.Background())
	if err != nil {
		t.Fatalf("second ensure: %v, want the load to be retried", err)
	}
	if gen == 0 {
		t.Error("generation is still 0 after a successful load")
	}
	if calls != 2 {
		t.Errorf("load called %d times, want 2 (retry after failure)", calls)
	}
}

// Once loaded, the scripts are not re-sent on every call: that is what makes the
// preload cheap enough to sit in front of every flow add.
func TestScriptCacheLoadsOnce(t *testing.T) {
	var calls int
	sc := &scriptCache{load: func(context.Context) error { calls++; return nil }}

	first, _ := sc.ensure(context.Background())
	second, _ := sc.ensure(context.Background())
	if calls != 1 {
		t.Errorf("load called %d times, want 1", calls)
	}
	if first != second {
		t.Errorf("generation moved without a reload: %d -> %d", first, second)
	}
}

// After NOSCRIPT every in-flight caller asks for a reload at once. They must
// collapse into a single SCRIPT LOAD sweep, not one sweep per goroutine.
func TestScriptCacheReloadIsSingleFlight(t *testing.T) {
	var mu sync.Mutex
	var calls int
	sc := &scriptCache{load: func(context.Context) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	}}

	gen, err := sc.ensure(context.Background())
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sc.reload(context.Background(), gen); err != nil {
				t.Errorf("reload: %v", err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 { // the initial load plus exactly one reload
		t.Errorf("load called %d times, want 2 (one initial + one shared reload)", calls)
	}
	if sc.generation() == gen {
		t.Error("generation did not move after a reload")
	}
}

// A reload requested against a generation that is already gone is someone else's
// work, already done — it must not trigger another sweep.
func TestScriptCacheReloadIgnoresStaleGeneration(t *testing.T) {
	var calls int
	sc := &scriptCache{load: func(context.Context) error { calls++; return nil }}

	gen, _ := sc.ensure(context.Background())
	if err := sc.reload(context.Background(), gen); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if err := sc.reload(context.Background(), gen); err != nil { // stale
		t.Fatalf("stale reload: %v", err)
	}
	if calls != 2 {
		t.Errorf("load called %d times, want 2 (stale reload must be a no-op)", calls)
	}
}

// A failed reload drops the cached state: the next call must load again rather
// than trust a cache Redis may no longer have.
func TestScriptCacheFailedReloadInvalidates(t *testing.T) {
	var calls int
	boom := errors.New("connection reset")
	sc := &scriptCache{load: func(context.Context) error {
		calls++
		if calls == 2 {
			return boom
		}
		return nil
	}}

	gen, _ := sc.ensure(context.Background())
	if err := sc.reload(context.Background(), gen); !errors.Is(err, boom) {
		t.Fatalf("reload err = %v, want %v", err, boom)
	}
	if _, err := sc.ensure(context.Background()); err != nil {
		t.Fatalf("ensure after a failed reload: %v", err)
	}
	if calls != 3 {
		t.Errorf("load called %d times, want 3 (initial, failed reload, fresh load)", calls)
	}
}
