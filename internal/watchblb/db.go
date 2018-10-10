// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package watchblb

import (
	"database/sql"
	"fmt"
	"time"

	// Import sqlite3 driver so that we can create db backed by sqlite.
	_ "github.com/mattn/go-sqlite3"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/client/blb"
)

var errNoBlobs = fmt.Errorf("no previously written blobs")

// SqliteDB is a persistent DB backed by sqlite for storing blob ids and
// creation times.
type SqliteDB struct {
	// The sqlite database.
	db *sql.DB

	// Prepared statements for operating on 'blobids' table.
	putStmt, getByIDStmt, delByCreationStmt, getDeletedStmt, delByIDStmt, randStmt *sql.Stmt
}

// NewSqliteDB creates a SqliteDB backed by the file located at 'path'.
func NewSqliteDB(path string) *SqliteDB {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatalf("failed to open the db backed by %s: %s", path, err)
	}

	// Create the table to store active blobs, if it doesn't already exist.
	//
	// Due to a bug in early version of sqlite, a non-integer primary key
	// can be null. So we need to set it to be not null explicitly here.
	// (see https://www.sqlite.org/lang_createtable.html#rowid).
	createStmt := "CREATE TABLE IF NOT EXISTS blobids (id TEXT NOT NULL PRIMARY KEY, creation INTEGER NOT NULL, deleted INTEGER)"
	if _, err := db.Exec(createStmt); err != nil {
		db.Close()
		log.Fatalf("failed to create blobids table: %s", err)
	}

	// Prepared statements on 'blobids' table.

	// Insert a blob and its creation time into the table.
	putStmt, err := db.Prepare("INSERT INTO blobids (id, creation, deleted) VALUES (?, ?, 0)")
	if err != nil {
		db.Close()
		log.Fatalf("failed to prepare put statement: %s", err)
	}

	// Retrieve the creation time of a given blob.
	getByIDStmt, err := db.Prepare("SELECT creation FROM blobids WHERE id=?")
	if err != nil {
		db.Close()
		log.Fatalf("failed to prepare getById statement: %s", err)
	}

	// Flag expired blobs for deletion.
	delByCreationStmt, err := db.Prepare("UPDATE blobids SET deleted=1 WHERE creation<?")
	if err != nil {
		db.Close()
		log.Fatalf("failed to prepare delByCreation statement: %s", err)
	}

	// Retrieve blobs that have been flagged for deletion.
	getDeletedStmt, err := db.Prepare("SELECT id FROM blobids WHERE deleted=1")
	if err != nil {
		db.Close()
		log.Fatalf("failed to prepare getDeleted statement: %s", err)
	}

	// Delete a flagged blob with the given id.
	delByIDStmt, err := db.Prepare("DELETE FROM blobids WHERE id=? AND deleted=1")
	if err != nil {
		db.Close()
		log.Fatalf("failed to prepare delByID statement: %s", err)
	}

	// Retrieve a random blob that hasn't been flagged for deletion.
	randStmt, err := db.Prepare("SELECT id FROM blobids WHERE deleted=0 ORDER BY RANDOM() LIMIT 1")
	if err != nil {
		db.Close()
		log.Fatalf("failed to prepare rand statement: %s", err)
	}

	return &SqliteDB{
		db:                db,
		putStmt:           putStmt,
		getByIDStmt:       getByIDStmt,
		delByCreationStmt: delByCreationStmt,
		getDeletedStmt:    getDeletedStmt,
		delByIDStmt:       delByIDStmt,
		randStmt:          randStmt,
	}
}

// Put stores a blob id with its creation time.
func (s *SqliteDB) Put(id blb.BlobID, creation time.Time) (err error) {
	if _, err = s.putStmt.Exec(id.String(), creation.UnixNano()); err != nil {
		log.Errorf("failed to insert (id=%s, creation=%s): %s", id, creation, err)
	}
	return err
}

// Get retrieves the creation time for the given blob.
func (s *SqliteDB) Get(id blb.BlobID) (creation time.Time, err error) {
	var t int64
	if err = s.getByIDStmt.QueryRow(id.String()).Scan(&t); err != nil {
		log.Errorf("failed to get record for id=%s: %s", id, err)
		return
	}
	return time.Unix(0, t), nil
}

// DeleteIfExpires flag expired blobs for deletion. A blob expires if creation +
// lifetime < time.Now().
//
// The workflow is:
//   (1) Flag expired blobs for deletion.
//   (2) Return such blobs to watchblb.
//   (3) After watchblb removes the blobs from the blb cluster, it acknowledges
//   the success by calling 'ConfirmDeletion', which deletes the flagged blobs
//   from the db.
//   (4) If watchblb fails before step (4), it needs to replay the deletion
//   operations upon restart.
// By doing this, we guarantee that the information stored in the db is
// consistent with that in the blb cluster, and thus there is no blob leakage
// during the process of removing expired blobs.
//
// Note however, there is no guarantee on no blob leakage during blob creation
// -- if watchblb crashes after creating the blob in the blb cluster but before
// logging it in the db, the blob is leaked.
func (s *SqliteDB) DeleteIfExpires(lifetime time.Duration) ([]blb.BlobID, error) {
	// Compute the creation time upper bound. Any blob with a creation time
	// smaller than this bound will be deleted.
	upper := time.Now().Add(-lifetime)

	// Flag expired blobs for deletion.
	if _, err := s.delByCreationStmt.Exec(upper.UnixNano()); err != nil {
		log.Errorf("failed to update record for creation<%s: %s", upper, err)
		return nil, err
	}

	// Return such blobs.
	return s.GetDeleted()
}

// GetDeleted retrieves blobs that are flagged for deletion.
func (s *SqliteDB) GetDeleted() ([]blb.BlobID, error) {
	rows, err := s.getDeletedStmt.Query()
	if err != nil {
		log.Errorf("failed to select record flagged for deletion: %s", err)
	}
	var idStr string
	var blobs []blb.BlobID
	for rows.Next() {
		if err := rows.Scan(&idStr); err != nil {
			log.Fatalf("failed to get blob id: %s", err)
		}
		if id, err := blb.ParseBlobID(idStr); err != nil {
			log.Fatalf("failed to parse blob id: %s", err)
		} else {
			blobs = append(blobs, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Errorf("error in iterating through rows: %s", err)
		return nil, err
	}

	return blobs, nil
}

// Rand retrieves a random blob that has not expired.
func (s *SqliteDB) Rand() (id blb.BlobID, err error) {
	var idStr string
	if err = s.randStmt.QueryRow().Scan(&idStr); err != nil {
		log.Errorf("failed to get a random record: %s", err)
	}
	if err == sql.ErrNoRows {
		err = errNoBlobs
	}
	if err != nil {
		return
	}

	id, err = blb.ParseBlobID(idStr)
	return
}

// ConfirmDeletion removes expired blobs in 'blobs'. All errors are logged and
// the last error is returned.
func (s *SqliteDB) ConfirmDeletion(blobs []blb.BlobID) (err error) {
	for _, id := range blobs {
		if _, derr := s.delByIDStmt.Exec(id.String()); derr != nil {
			log.Errorf("failed to remove blob %s: %s", id, err)
			err = derr
		}
	}
	return
}

// Close closes the db. All errors will be logged and the last error is
// returned.
func (s *SqliteDB) Close() (err error) {
	if cerr := s.putStmt.Close(); cerr != nil {
		err = cerr
		log.Errorf("failed to close put statement: %s", err)
	}
	if cerr := s.getByIDStmt.Close(); cerr != nil {
		err = cerr
		log.Errorf("failed to close getByID statement: %s", err)
	}
	if cerr := s.delByCreationStmt.Close(); cerr != nil {
		err = cerr
		log.Errorf("failed to close delByCreation statement: %s", err)
	}
	if cerr := s.getDeletedStmt.Close(); cerr != nil {
		err = cerr
		log.Errorf("failed to close getDeleted statement: %s", err)
	}

	if cerr := s.delByIDStmt.Close(); cerr != nil {
		err = cerr
		log.Errorf("failed to close delByID statement: %s", err)
	}
	if cerr := s.randStmt.Close(); cerr != nil {
		err = cerr
		log.Errorf("failed to close rand statement: %s", err)
	}
	if cerr := s.db.Close(); cerr != nil {
		err = cerr
		log.Errorf("failed to close db: %s", err)
	}
	return err
}
