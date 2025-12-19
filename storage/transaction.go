package storage

import (
	"errors"
	"sync/atomic"
)

type Txn struct {
	Id     uint64
	writes map[string]any
	db     *MemoryStorage
	active bool
}

func (m *MemoryStorage) Begin() *Txn {
	id := atomic.AddUint64(&m.nextTxnID, 1)

	return &Txn{
		Id:     id,
		writes: make(map[string]any),
		db:     m,
		active: true,
	}
}

func (tx *Txn) Get(key string) (any, error) {
	if !tx.active {
		return nil, errors.New("txn not active")
	}

	// 1️⃣ Read own writes
	if val, ok := tx.writes[key]; ok {
		return val, nil
	}

	// 2️⃣ Read committed state
	tx.db.MU.RLock()
	defer tx.db.MU.RUnlock()

	val, ok := tx.db.Storage[key]
	if !ok {
		return nil, errors.New("NOT_FOUND")
	}
	return val, nil
}

func (tx *Txn) Set(key string, value any) error {
	if !tx.active {
		return errors.New("txn not active")
	}
	if key == "" {
		return errors.New("INVALID_KEY")
	}

	tx.writes[key] = value
	return nil
}

func (tx *Txn) Commit() error {
	if !tx.active {
		return errors.New("txn already finished")
	}

	// 🔒 serialize commits
	tx.db.MU.Lock()
	defer tx.db.MU.Unlock()

	// 1️⃣ WAL (with txnID)
	if err := tx.db.WAL.LogTxn(tx); err != nil {
		return err
	}

	// 2️⃣ Apply writes
	for k, v := range tx.writes {
		tx.db.Storage[k] = v
	}

	tx.active = false
	return nil
}

func (tx *Txn) Rollback() error {
	if !tx.active {
		return errors.New("txn already finished")
	}

	// Just discard local writes
	tx.writes = nil
	tx.active = false
	return nil
}
