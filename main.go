package main

import (
	"db/cli"
	"db/database"
	"encoding/json"
	"fmt"
	"net/http"
)

var db, err = database.NewDatabase("./", "uid__")

type Request struct {
	Type       string `json:"type"`
	Collection string `json:"collection,omitempty"`
	Key        string `json:"key,omitempty"`
	Value      string `json:"value,omitempty"`
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is supported", http.StatusMethodNotAllowed)
		return
	}

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	switch req.Type {
	case "create_collection":
		col := db.CreateCollection(req.Collection, 3)
		if col != nil {
			json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Collection created"})
		} else {
			http.Error(w, "Collection creation failed", http.StatusInternalServerError)
		}

	// case "delete_collection":
	// 	err := db.DeleteCollection(req.Collection)
	// 	if err != nil {
	// 		http.Error(w, "Failed to delete collection", http.StatusInternalServerError)
	// 		return
	// 	}
	// 	json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Collection deleted"})

	case "get":
		col, err := db.GetCollection(req.Collection)
		if err != nil {
			http.Error(w, "Collection not found", http.StatusNotFound)
			return
		}
		val, found := col.FindKey(req.Key)
		if !found {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		if strVal, ok := val.(string); ok {
			json.NewEncoder(w).Encode(map[string]string{"value": strVal})
		} else {
			http.Error(w, "Value is not a string", http.StatusInternalServerError)
		}

	case "set":
		col, err := db.GetCollection(req.Collection)
		if err != nil {
			http.Error(w, "Collection not found", http.StatusNotFound)
			return
		}
		col.InsertKV(req.Key, req.Value)
		json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Value set"})

	case "delete":
		col, err := db.GetCollection(req.Collection)
		if err != nil {
			http.Error(w, "Collection not found", http.StatusNotFound)
			return
		}
		col.InsertKV(req.Key, "") // todo: use DeleteKey()
		json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Key deleted"})

	// case "retrieve_all_keys":
	// 	col, err := db.GetCollection(req.Collection)
	// 	if err != nil {
	// 		http.Error(w, "Collection not found", http.StatusNotFound)
	// 		return
	// 	}
	// 	keys := col.GetAllKeys()
	// 	json.NewEncoder(w).Encode(map[string]any{"keys": keys})

	default:
		http.Error(w, "Unsupported request type", http.StatusBadRequest)
	}
	// cli.Execute()
}

func main() {
	// check()
	if err != nil {
		fmt.Println("DB creation failed", err)
	}

	cli.Execute()
	// http.HandleFunc("/query", queryHandler)
	fmt.Println("NutellaDB server is running on port 8080")
	// log.Fatal(http.ListenAndServe(":8080", nil))
}
