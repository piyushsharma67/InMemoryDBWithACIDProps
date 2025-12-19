package storage

import (
	"fmt"
	"os"
	"sync"
)

type WAL struct {
	File *os.File
	mu   sync.Mutex
}

func NewWal(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		File: file,
	}, nil
}

func (w *WAL) LogSet(key string, value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.File.WriteString(
		"SET " + key + " " + fmt.Sprintf("%v", value) + "\n",
	)
	if err != nil {
		return err
	}

	// Force write to disk
	return w.File.Sync()
}

func (w *WAL) LogTxn(tx *Txn) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.File.WriteString(fmt.Sprintf("BEGIN %d\n", tx.Id))

	for k, v := range tx.writes {
		w.File.WriteString(
			fmt.Sprintf("SET %d %s %v\n", tx.Id, k, v),
		)
	}

	w.File.WriteString(fmt.Sprintf("COMMIT %d\n", tx.Id))
	return w.File.Sync()
}
