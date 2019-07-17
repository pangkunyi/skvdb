package skvdb

import "testing"

func Benchmark(b *testing.B) {
	db := New(".", 10)
	for i := 0; i < b.N; i++ {
		_, err := db.getCounter(func(counter uint64) error {
			return nil
		})
		if err != nil {
			b.Fatal("failed to get counter", err)
		}
	}
}
func TestResetCounter(t *testing.T) {
	db := New(".", 10)
	if err := db.resetCounter(); err != nil {
		t.Fatal("failed to reset counter", err)
	}
}
func TestGetCounter(t *testing.T) {
	db := New(".", 10)
	for i := 0; i < 1000; i++ {
		counter, err := db.getCounter(func(counter uint64) error {
			return nil
		})
		if err != nil {
			t.Fatal("failed to get counter", err)
		}

		t.Log("counter:", counter)
	}
}

func TestPrefetchCounter(t *testing.T) {
	db := New(".", 10)
	if err := db.prefetchCounter(); err != nil {
		t.Fatal("failed to prefetch counter", err)
	}
	if db.counterIdx != 0 {
		t.Fatalf("unexpected counterIdx[%d]", db.counterIdx)
	}
	t.Log("counter:", db.counter)
}
