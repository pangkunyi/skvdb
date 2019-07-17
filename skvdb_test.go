package skvdb

import (
	"fmt"
	"testing"
	"time"
)

func TestSave(t *testing.T) {
	db := New("./", 10)
	ts := time.Now().UnixNano()
	t.Logf("time:%d", ts)
	fmt.Println(ts)
	fmt.Println(ts)
	fmt.Println(ts)
	key := &Key{1, ts, 3}
	if err := db.Save(key, []byte("testing")); err != nil {
		t.Fatal("failed to save", err)
	}
}

func TestQuery(t *testing.T) {
	db := New("./", 10)
	ts := time.Now().UnixNano()
	t.Logf("time:%d", ts)
	fmt.Println(ts)
	fmt.Println(ts)
	key := &Key{1, ts, 3}
	payload, err := db.Query(key)
	if err != nil {
		t.Fail()
	}
	if string(payload) != "testing" {
		t.Fail()
	}
}
