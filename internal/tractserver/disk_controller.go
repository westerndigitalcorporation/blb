// Copyright (c) 2017 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
)

type diskController struct {
	s *Store
}

// NewDiskController creates a new disk controller, listening on a unix socket based on
// the address in the config, adding and removing disks from the given store.
func NewDiskController(s *Store) *diskController {
	cfg := s.Config()
	base := cfg.DiskControllerBase
	c := &diskController{s}

	if err := os.MkdirAll(base, 0700); err != nil {
		log.Fatalf("Couldn't create directory %q for disk controller: %s", base, err)
	}

	path := filepath.Join(base, cfg.Addr)
	os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		log.Fatalf("Could not listen on unix socket %q: %s", path, err)
	}

	m := http.NewServeMux()
	m.HandleFunc("/disk", c.disk)
	go http.Serve(l, m)

	return c
}

func (c *diskController) disk(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		c.addDisk(w, r)
	case "DELETE":
		c.delDisk(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, "Bad method (POST or DELETE allowed)")
	}
}

func (c *diskController) addDisk(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	root := q.Get("root")
	if root == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Missing root")
		return
	}

	if c.s.DiskExists(root) {
		w.WriteHeader(http.StatusOK) // this shouldn't be treated as an error
		fmt.Fprintf(w, "Disk at %q already exists", root)
		return
	}

	// Doesn't exist. Let's add it. Do a basic sanity check. NewManager will do more.
	if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Root %q is not a directory", root)
		return
	}

	disk, err := NewManager(root, c.s.Config())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error creating manager for %q: %s", root, err)
		return
	}

	err = c.s.AddDisk(disk)
	if err == ErrDiskExists {
		// This could happen if two requests tried to add this disk in parallel
		// and the other one won.
		disk.Stop()
		w.WriteHeader(http.StatusOK) // this shouldn't be treated as an error
		fmt.Fprintf(w, "Disk at %q already exists", root)
		return
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error adding %q: %s", root, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Added %q", root)
}

func (c *diskController) delDisk(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	root := q.Get("root")
	err := c.s.RemoveDisk(root)
	if err == ErrDiskNotFound {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "No disk with that root exists")
		return
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error removing %q: %s", root, err)
		return
	}
	fmt.Fprintf(w, "Removed %q", root)
}
