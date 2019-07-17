package skvdb

import (
	"encoding/binary"
	"os"
)

const (
	//CounterPrefetchSize prefetch numbers from counter for performance
	CounterPrefetchSize = uint64(1)
)

func (skv *SkvDB) getCounter(callback func(uint64) error) (uint64, error) {
	skv.counterLock.Lock()
	defer skv.counterLock.Unlock()
	if skv.counter < 1 {
		if err := skv.prefetchCounter(); err != nil {
			return 0, err
		}
	}
	skv.counterIdx++
	idx := skv.counterIdx
	counter := skv.counter
	newCounter := counter + idx
	if idx > CounterPrefetchSize {
		if err := skv.prefetchCounter(); err != nil {
			return 0, err
		}
	}
	if err := callback(newCounter); err != nil {
		return 0, err
	}
	return newCounter, nil
}

func (skv *SkvDB) resetCounter() error {
	fd, err := os.OpenFile(skv.dataDir+"/counter", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer fd.Close()
	err = binary.Write(fd, binary.BigEndian, int64(1))
	if err != nil {
		return err
	}
	return nil
}

func (skv *SkvDB) prefetchCounter() error {
	fd, err := os.OpenFile(skv.dataDir+"/counter", os.O_RDWR, 0660)
	if err != nil {
		return err
	}
	defer fd.Close()
	err = binary.Read(fd, binary.BigEndian, &skv.counter)
	if err != nil {
		return err
	}
	if _, err := fd.Seek(0, 0); err != nil {
		return err
	}
	err = binary.Write(fd, binary.BigEndian, skv.counter+CounterPrefetchSize+1)
	if err != nil {
		return err
	}
	skv.counterIdx = 0
	return nil
}
