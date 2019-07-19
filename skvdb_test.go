package skvdb

import (
	"io"
	"math/rand"
	"os"
	"strings"
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

func TestQuery(t *testing.T) {
	db := New("./", 10)
	key := "3a4c53005d3165150000000000001d18"
	payload, err := db.Query(key)
	if err != nil {
		t.Fatal("failed to query", err)
	}
	if len(string(payload)) < 0 {
		t.Fatalf("failed to query, payload[%s] not correct, cause by:%v", string(payload), err)
	}
}
func TestReadNextRecord(t *testing.T) {
	db := New("./", 10)
	fd, err := os.OpenFile("./2019-07-19/7.dat", os.O_RDONLY, 0660)
	if err != nil {
		t.Fatal("failed to open file")
	}
	fi, err := fd.Stat()
	if err != nil {
		t.Fatal("failed to stat file")
	}
	record, err := db.readNextRecord(int64(6094521), fd, fi.Size())
	if err != nil {
		t.Fatal("failed to read next record")
	}
	t.Log(record.key)
	record, err = db.readNextRecord(int64(6094586), fd, fi.Size())
	if err != nil {
		t.Fatal("failed to read next record")
	}
	t.Log(record.key)
	record, err = db.readNextRecord(int64(6104223), fd, fi.Size())
	if err != nil {
		t.Fatal("failed to read next record")
	}
	t.Log(record.key)
}

func TestReadSequence(t *testing.T) {
	db := New("./", 10)
	fd, err := os.OpenFile("./2019-07-19/7.dat", os.O_RDONLY, 0660)
	if err != nil {
		t.Fatal("failed to open file")
	}
	offset, err := fd.Seek(int64(6094521), 0)
	if err != nil {
		t.Fatal("failed to read offset", err)
	}
	t.Logf("offset:%d", offset)
	skvr := make([]byte, 4)
	for {
		offset, err := fd.Seek(0, 1)
		if err != nil {
			t.Fatal("failed to read offset", err)
		}
		t.Logf("offset start:%d", offset)
		n, err := fd.Read(skvr)
		if err != nil {
			if err != io.EOF {
				t.Fatal("failed to read skvr")
			}
		}
		if n < 1 {
			t.Logf("EOF, n:%d", n)
			break
		}
		record, err := db.tryReadRecord(fd)
		if err != nil {
			t.Fatal("failed to read record")
		}
		if record.key.Counter == 7666 {
			t.Log("hello world")
		}
		t.Log(record.key)
		offset, err = fd.Seek(0, 1)
		if err != nil {
			t.Fatal("failed to read offset", err)
		}
		t.Logf("offset end:%d", offset)
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

func TestSaveAndQuery(t *testing.T) {
	db := New(".", 10)
	size := 100000
	m := make(map[string]string)
	for i := 0; i < size; i++ {
		val := genVal()
		key, err := db.Save([]byte(val))
		if err != nil {
			t.Fatal("failed to save", err)
		}
		m[key.HexString()] = val
	}

	for k, v := range m {
		payload, err := db.Query(k)
		if err != nil {
			t.Fatalf("failed to query, key: %s, cause by:%s", k, err)
		}
		if v != string(payload) {
			t.Fatalf("failed to query, key: %s, cause by:%s", k, err)
		}
	}
}

func genVal() string {
	str := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789~!@#$%^&*()_+-=;:\"'|\\<,>.?/`")
	size := rand.Intn(50000)
	var sb strings.Builder
	for i := 0; i < size; i++ {
		sb.WriteByte(str[rand.Intn(len(str))])
	}
	return sb.String()
}

func TestSeek(t *testing.T) {
	fd, err := os.OpenFile("./2019-07-19/0.dat", os.O_RDONLY, 0660)
	if err != nil {
		t.Fatal("failed to open file")
	}
	n, err := fd.Seek(10, 0)
	if err != nil {
		t.Fatal("failed to seek", err)
	}
	n, err = fd.Seek(-1, 1)
	if n != 9 {
		t.Fatalf("failed to seek, pos:%d", n)
	}
}
