package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/piyushsharma67/my_db/server"
	"github.com/piyushsharma67/my_db/storage"
)

func main() {
	store := storage.NewMemoryStorage()
	txnMgr := server.NewTxnManager(store)

	http.HandleFunc("/txn/begin", func(w http.ResponseWriter, r *http.Request) {
		tx := txnMgr.Begin()
		json.NewEncoder(w).Encode(map[string]any{
			"txn_id": tx.Id,
		})
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", 400)
			return
		}

		// read committed state directly
		val, err := store.Get(key)
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}

		json.NewEncoder(w).Encode(map[string]any{
			"value": val,
		})
	})

	http.HandleFunc("/txn/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 {
			http.Error(w, "invalid path", 400)
			return
		}

		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "invalid txn id", 400)
			return
		}

		tx, ok := txnMgr.Get(id)
		if !ok {
			http.Error(w, "txn not found", 404)
			return
		}

		action := parts[2]

		switch action {

		case "set":
			var req struct {
				Key   string `json:"key"`
				Value any    `json:"value"`
			}
			json.NewDecoder(r.Body).Decode(&req)

			if err := tx.Set(req.Key, req.Value); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			w.WriteHeader(http.StatusOK)

		case "get":
			key := r.URL.Query().Get("key")
			val, err := tx.Get(key)
			if err != nil {
				http.Error(w, err.Error(), 404)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"value": val,
			})

		case "commit":
			if err := tx.Commit(); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			txnMgr.Remove(id)
			w.WriteHeader(http.StatusOK)

		case "rollback":
			if err := tx.Rollback(); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			txnMgr.Remove(id)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unknown action", 400)
		}
	})
	fmt.Println("Starting server on port 8080")
	http.ListenAndServe(":8080", nil)
}
