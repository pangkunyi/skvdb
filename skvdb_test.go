package skvdb

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

func BenchmarkQuery(b *testing.B) {
	db := New("./", 10)
	payload := "Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos "
	key, err := db.Save([]byte(payload))
	if err != nil {
		b.Fatal("failed to save record", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Query(key.HexString())
		if err != nil {
			b.Fatal("failed to query", err)
		}
	}
	_payload, err := db.Query(key.HexString())
	if err != nil {
		b.Fatal("failed to query", err)
	}
	if string(_payload) != payload {
		b.Fatal("invalid query")
	}

}

func BenchmarkSave(b *testing.B) {
	db := New("./", 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Save([]byte("Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos Beautiful Girls Videos "))
	}
}

func TestSave(t *testing.T) {
	db := New("./", 10)
	if key, err := db.Save([]byte("testing")); err != nil {
		t.Fatal("failed to save", err)
	} else {
		t.Log(key)
		t.Log(key.HexString())
	}
}

func TestReadNextRecord(t *testing.T) {
	db := New("./", 10)
	fd, err := os.OpenFile("./2019-07-18/7.dat", os.O_RDONLY, 0660)
	record, err := db.readNextRecord(1, fd, 10000000)
	if err != nil {
		t.Fatal("failed to query", err)
	}
	t.Log(record)
}

func TestQuery(t *testing.T) {
	db := New("./", 10)
	payload, err := db.Query("a0a883005d3050ee00000000000013a2")
	if err != nil {
		t.Fatal("failed to query", err)
	}
	if string(payload) != "testing" {
		t.Fatalf("failed to query, payload[%s] not correct, cause by:%v", string(payload), err)
	}
}

func TestKeyHexString(t *testing.T) {
	ts := time.Now().Unix()
	rand.Seed(ts)
	key := &Key{rand.Uint32() >> 8, uint64(ts), 3}
	t.Logf("key:%s", key.String())
	t.Logf("key hex:%s", key.HexString())
}

func TestNewKey(t *testing.T) {
	keyStr := "34845f005d2fe5320000000000000003"
	key, err := NewKey(keyStr)
	if err != nil {
		t.Fatal("invalid key", err)
	}
	if key.Rand != 3441759 || key.Timestamp != 1563419954 || key.Counter != 3 {
		t.Fatalf("invalid key:%s", key)
	}
}
