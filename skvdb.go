package skvdb

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/adler32"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

const (
	lenOfSkvr       = 4
	bufSize         = 4096
	maxLenOfPayload = uint32(10485760)
)

var (
	skvdbMagic              = []byte("SKVDB") //5 bytes
	skvrMagic               = []byte("SKVR")  //skv record magic, 4 bytes
	skvdbVersion      int32 = 1               //4 bytes
	errorEmptyKey           = errors.New("Error: Empty Key")
	errorNoNextRecord       = fmt.Errorf("no next record, cause EOF")
)

type fileHeader struct {
	magic   string
	version int32
}

type record struct {
	checksum     uint32 //4 bytes
	lenOfPayload uint32
	key          *Key   //20 bytes
	val          []byte //lenOfPayload-20 bytes
}

func (r *record) byteSize() uint32 {
	return 12 + r.lenOfPayload
}

//SkvDB skvdb
type SkvDB struct {
	dataDir     string
	partitions  uint64
	fdMap       map[string]*os.File
	preFd       *os.File
	preFilename string
	//counter
	counter     uint64
	counterIdx  uint64
	counterLock *sync.Mutex
}

//Key key, 20 bytes
type Key struct {
	Rand      uint32 //4 byte
	Timestamp uint64 //8 bytes
	Counter   uint64 //8 bytes
}

//Equals check two keys if equals
func (k *Key) Equals(k1 *Key) bool {
	if k1 == nil {
		return false
	}
	return k.Rand == k1.Rand && k.Timestamp == k1.Timestamp && k.Counter == k1.Counter
}

//Equals check two keys if equals
func (k *Key) compare(k1 *Key) int {
	if k1 == nil {
		return 1
	}
	if k.Counter > k1.Counter {
		return 1
	} else if k.Counter < k1.Counter {
		return -1
	}
	return 0
}

func (k *Key) String() string {
	return fmt.Sprintf("Rand:%d, Timestamp:%d, Counter:%d", k.Rand, k.Timestamp, k.Counter)
}

//HexString key hex string
func (k *Key) HexString() string {
	buf := new(bytes.Buffer)
	rt := uint64(k.Rand)<<40 | k.Timestamp
	binary.Write(buf, binary.BigEndian, rt)
	binary.Write(buf, binary.BigEndian, k.Counter)
	return hex.EncodeToString(buf.Bytes())
}

//NewKey create a key from a key string
func NewKey(key string) (*Key, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("invalid key[%s]'s len[%d]", key, len(key))
	}
	payload, err := hex.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key[%s]", key)
	}
	buf := bytes.NewBuffer(payload)
	var rt uint64
	var counter uint64
	if err := binary.Read(buf, binary.BigEndian, &rt); err != nil {
		return nil, fmt.Errorf("invalid key[%s]", key)
	}
	if err := binary.Read(buf, binary.BigEndian, &counter); err != nil {
		return nil, fmt.Errorf("invalid key[%s]", key)
	}
	return &Key{Rand: uint32(rt >> 40), Timestamp: rt << 24 >> 24, Counter: counter}, nil
}

func (k *Key) hash() uint64 {
	result := uint64(1)
	result = uint64(31)*result + uint64(k.Rand)
	result = uint64(31)*result + k.Timestamp
	result = uint64(31)*result + k.Counter
	return result
}

//New create one skvdb instance
func New(dataDir string, partitions uint64) *SkvDB {
	return &SkvDB{dataDir: dataDir, partitions: partitions, fdMap: make(map[string]*os.File), counterLock: new(sync.Mutex)}
}

func (skv *SkvDB) key2Filename(key *Key) string {
	t := time.Unix(int64(key.Timestamp), 0)
	index := (uint64(31)*(uint64(31)+uint64(key.Rand))+key.Timestamp) % skv.partitions
	if index < 0 {
		index = -index
	}
	return fmt.Sprintf("%s/%s/%d.dat", skv.dataDir, t.Format("2006-01-02"), index)
}

func (skv *SkvDB) readNextRecord(offset int64, fd *os.File, endOffset int64) (*record, error) {
	buf := make([]byte, bufSize)
	if _, err := fd.Seek(offset, 0); err != nil {
		return nil, err
	}
	recordStartOffset := int64(0)
	for {
		n, err := fd.Read(buf)
		if err == io.EOF {
			return nil, errorNoNextRecord
		}
		if err != nil {
			return nil, err
		}
		if n < 1 {
			return nil, errorNoNextRecord
		}

		found := false
		idx := 0
		for idx < n {
			if buf[idx] != skvrMagic[0] {
				idx++
				recordStartOffset++
				continue
			}
			if buf[idx+1] != skvrMagic[1] {
				idx++
				recordStartOffset++
				continue
			}
			if buf[idx+2] != skvrMagic[2] {
				idx = idx + 2
				recordStartOffset = recordStartOffset + 2
				continue
			}
			if buf[idx+3] != skvrMagic[3] {
				idx = idx + 3
				recordStartOffset = recordStartOffset + 3
				continue
			}
			found = true
			break
		}
		if found {
			newOffset := offset + recordStartOffset + lenOfSkvr
			if _, err := fd.Seek(newOffset, 0); err != nil {
				return nil, err
			}
			record, err := skv.tryReadRecord(fd)
			if err != nil {
				if _, err := fd.Seek(newOffset, 0); err != nil {
					return nil, err
				}
				continue
			}
			return record, nil
		}
	}
}

func (skv *SkvDB) tryReadRecord(fd *os.File) (*record, error) {
	var key Key
	var record record
	if err := binary.Read(fd, binary.BigEndian, &record.checksum); err != nil {
		return nil, err
	}
	if err := binary.Read(fd, binary.BigEndian, &record.lenOfPayload); err != nil {
		return nil, err
	}
	if record.lenOfPayload > maxLenOfPayload {
		return nil, fmt.Errorf("too large length of payload, len:%d", record.lenOfPayload)
	}
	payload := make([]byte, record.lenOfPayload)
	size, err := fd.Read(payload)
	if err != nil {
		return nil, err
	}
	if size != int(record.lenOfPayload) {
		return nil, errorNoNextRecord
	}
	if record.checksum != adler32.Checksum(payload) {
		return nil, fmt.Errorf("checksum error")
	}
	payloadBuffer := bytes.NewBuffer(payload)
	if err := binary.Read(payloadBuffer, binary.BigEndian, &key.Rand); err != nil {
		return nil, err
	}
	if err := binary.Read(payloadBuffer, binary.BigEndian, &key.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(payloadBuffer, binary.BigEndian, &key.Counter); err != nil {
		return nil, err
	}
	record.key = &key
	record.val = payload[20:]
	return &record, nil
}

//Query query value by key string
func (skv *SkvDB) Query(key string) ([]byte, error) {
	_key, err := NewKey(key)
	if err != nil {
		return nil, err
	}
	return skv.QueryByKey(_key)
}

//QueryByKey query value by key
func (skv *SkvDB) QueryByKey(key *Key) ([]byte, error) {
	if key == nil {
		return nil, errorEmptyKey
	}
	filename := skv.key2Filename(key)
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("key[%s] file not found", key)
		}
		return nil, fmt.Errorf("can not open key file, cause by %v", err)
	}
	defer fd.Close()
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	start := int64(0)
	end := fi.Size()
	stack := list.New()
	stack.PushBack([]int64{start, end})
	for stack.Len() > 0 {
		startEnd := stack.Remove(stack.Back()).([]int64)
		start = startEnd[0]
		end = startEnd[1]
		if start <= end {
			mid := (end + start) / 2
			record, err := skv.readNextRecord(mid, fd, end)
			if err != nil {
				if err != errorNoNextRecord {
					return nil, err
				}
			}
			if err == errorNoNextRecord {
				stack.PushBack([]int64{start, mid - 1})
				continue
			}
			switch key.compare(record.key) {
			case 0:
				return record.val, nil
			case 1:
				stack.PushBack([]int64{mid + int64(record.byteSize()), end})
				break
			case -1:
				stack.PushBack([]int64{start, mid - 1})
				break
			}
		}
	}
	return nil, errorNoNextRecord
}

//Save save record in key value
func (skv *SkvDB) Save(val []byte) (Key, error) {
	key := Key{Rand: rand.Uint32() >> 8, Timestamp: uint64(time.Now().Unix())}

	filename := skv.key2Filename(&key)
	ok := false
	var fd *os.File
	if fd, ok = skv.fdMap[filename]; !ok {
		err := os.MkdirAll(path.Dir(filename), 0600)
		if err != nil {
			return key, err
		}
		fd, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return key, err
		}
		skv.fdMap[filename] = fd
	}
	if fd != nil {
		if skv.preFd != fd {
			if skv.preFd != nil {
				defer func(_fd *os.File, _filename string) {
					delete(skv.fdMap, _filename)
					_fd.Close()
				}(skv.preFd, skv.preFilename)
			}
			skv.preFd = fd
			skv.preFilename = filename
		}
		counter, err := skv.getCounter(func(counter uint64) error {
			key.Counter = counter
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, key.Rand)
			binary.Write(buf, binary.BigEndian, key.Timestamp)
			binary.Write(buf, binary.BigEndian, key.Counter)
			buf.Write(val)

			payload := buf.Bytes()
			lenOfPayload := uint32(len(payload))
			if lenOfPayload > maxLenOfPayload {
				return fmt.Errorf("reach max length of payload, len:%d", lenOfPayload)
			}
			checksum := adler32.Checksum(payload)
			rBuf := new(bytes.Buffer)
			rBuf.Write(skvrMagic)
			binary.Write(rBuf, binary.BigEndian, checksum)
			binary.Write(rBuf, binary.BigEndian, uint32(len(payload)))
			rBuf.Write(payload)

			if _, err := fd.Write(rBuf.Bytes()); err != nil {
				return err
			}
			if err := fd.Sync(); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return key, err
		}
		key.Counter = counter
	}
	return key, nil
}
