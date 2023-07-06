// Copyright 2020 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/heroiclabs/nakama-common/runtime"
	"github.com/lib/pq"
)

var (
	errInternalError  = runtime.NewError("internal server error", 13) // INTERNAL
	errMarshal        = runtime.NewError("cannot marshal type", 13)   // INTERNAL
	errNoInputAllowed = runtime.NewError("no input allowed", 3)       // INVALID_ARGUMENT
	errNoUserIdFound  = runtime.NewError("no user ID in context", 3)  // INVALID_ARGUMENT
	errUnmarshal      = runtime.NewError("cannot unmarshal type", 13) // INTERNAL
)

const (
	rpcIdReadData = "read_data"
)

func InitModule(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.NakamaModule, initializer runtime.Initializer) error {
	initStart := time.Now()

	if err := initializer.RegisterRpc(rpcIdReadData, rpcReadData); err != nil {
		return err
	}

	if err := registerSessionEvents(db, nk, initializer); err != nil {
		return err
	}

	logger.Info("Plugin loaded in '%d' msec.", time.Now().Sub(initStart).Milliseconds())
	return nil
}

type Data struct {
	Type    string `json:"type"`
	Version string `json:"version"`
	Hash    string `json:"hash"`
	Content string `json:"content"`
}

func rpcReadData(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.NakamaModule, payload string) (string, error) {
	var err error
	// logger.Info("received request")
	// Parse the payload into a GuildData struct
	var readData Data
	err = json.Unmarshal([]byte(payload), &readData)
	if err != nil {
		return "", err
	}

	// Set defaults if they are not present in the payload
	if readData.Type == "" {
		readData.Type = "core"
	}
	if readData.Version == "" {
		readData.Version = "1.0.0"
	}

	// Create the file path based on the type and version
	filePath := filepath.Join(readData.Type, readData.Version+".json")

	// Read the file from disk
	fileContent, err := ioutil.ReadFile("/nakama/data/" + filePath)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(fileContent)
	calculatedHash := hex.EncodeToString(hash[:])

	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS hash_data (
		id SERIAL PRIMARY KEY,
		type TEXT,
		version TEXT,
		hash TEXT,
		content TEXT
	)`,
	)
	if err != nil {
		logger.Error("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO hash_data (type, version, hash, content) VALUES ($1, $2, $3, $4)", readData.Type, readData.Version, calculatedHash, string(fileContent))
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code.Name() == "unique_violation" {
			return "", errors.New("Guild already exists")
		}
		return "", err
	}

	response := Data{
		Type:    readData.Type,
		Version: readData.Version,
		Hash:    calculatedHash,
	}

	if readData.Hash != calculatedHash {
		response.Content = ""
	} else {
		response.Content = string(fileContent)
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return "", err
	}

	return string(responseJSON), nil
}
