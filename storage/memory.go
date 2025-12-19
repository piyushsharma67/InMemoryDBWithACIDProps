package storage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
)

type MemoryStorage struct {
	Storage map[any]any
	MU      sync.RWMutex
	WAL     *WAL
	nextTxnID uint64
}

func NewMemoryStorage() *MemoryStorage {
	wal, err := NewWal("wal.log")
	if err != nil {
		panic(err)
	}

	ms := &MemoryStorage{
		Storage: make(map[any]any),
		WAL:     wal,
	}

	ms.Recover()
	return ms
}

func (m *MemoryStorage) Get(key any) (any, error) {
	m.MU.Lock()
	defer m.MU.Unlock()

	val, ok := m.Storage[key]

	if !ok {
		return nil, errors.New("NOT_FOUND")
	}

	return val, nil

}

func (m *MemoryStorage) Set(key string, value any) error {
	if key == "" {
		return errors.New("INVALID_KEY")
	}

	if err := m.WAL.LogSet(key, value); err != nil {
		return err
	}

	m.MU.Lock()
	defer m.MU.Unlock()

	m.Storage[key] = value
	return nil
}

func (m *MemoryStorage) Recover() error {
	file, err := os.Open("wal.log")
	if err != nil {
		return nil
	}
	defer file.Close()

	type txnBuf map[string]any
	txns := make(map[uint64]txnBuf)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		var (
			cmd string
			id  uint64
			key string
			val string
		)

		if _, err := fmt.Sscanf(line, "%s %d", &cmd, &id); err != nil {
			continue
		}

		switch cmd {
		case "BEGIN":
			txns[id] = make(txnBuf)

		case "SET":
			fmt.Sscanf(line, "%s %d %s %s", &cmd, &id, &key, &val)
			txns[id][key] = val

		case "COMMIT":
			for k, v := range txns[id] {
				m.Storage[k] = v
			}
			delete(txns, id)
		}
	}

	return scanner.Err()
}

