package server

import (
	"sync"

	"github.com/piyushsharma67/my_db/storage"
)

type TxnManager struct {
	mu    sync.Mutex
	txns  map[uint64]*storage.Txn
	store *storage.MemoryStorage
}

func NewTxnManager(store *storage.MemoryStorage) *TxnManager {
	return &TxnManager{
		txns:  make(map[uint64]*storage.Txn),
		store: store,
	}
}

func (tm *TxnManager) Begin() *storage.Txn {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx := tm.store.Begin()
	tm.txns[tx.Id] = tx
	return tx
}

func (tm *TxnManager) Get(id uint64) (*storage.Txn, bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, ok := tm.txns[id]
	return tx, ok
}

func (tm *TxnManager) Remove(id uint64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.txns, id)
}
