// Copyright (c) 2017 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package backupblb

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/westerndigitalcorporation/blb/internal/server"
)

const (
	stateFile      = "state.json"
	filenameHeader = "X-Blb-Snapshot-Filename"
	urlPattern     = "http://%s/raft/last_snapshot"
)

type scheduleEntry struct {
	backupsAgo uint64
	keepEvery  uint64
}

// A prune schedule defines how many backups to keep based on how long ago (in
// number of backups) a backup was from. It's defined in intervals: for each
// entry, if the current backup was <= backupsAgo, then it will be kept if its
// sequence mod keepEvery is zero. By default, the last entry is used.
//
// Intervals should be listed in ascending order. Each keepEvery value must be a
// multiple of the previous one, because of how mod works, otherwise we'll drop
// more than intended.
var pruneSchedule = []scheduleEntry{
	{10, 1},  // keep all of last ten
	{400, 4}, // keep 1/4 of the next 400 (works out to ~1 per day for 3 months)
	{0, 32},  // keep 1/32 after that (~1 per week)
}

var (
	metricLastCheck = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "backupblb",
		Name:      "last_check_time",
		Help:      "time of last successful check in unix time",
	}, []string{"backup_service"})
	metricLastBackup = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "backupblb",
		Name:      "last_backup_time",
		Help:      "time of last successful backup in unix time",
	}, []string{"backup_service"})
)

type BackupManager struct {
	cfg   Config
	opm   *server.OpMetric
	state *State
	svcRe *regexp.Regexp
	cli   http.Client
}

func NewBackup(cfg Config) *BackupManager {
	return &BackupManager{
		cfg:   cfg,
		opm:   server.NewOpMetric("backup", "op"),
		svcRe: regexp.MustCompile(cfg.ServiceRE),
		cli:   http.Client{Timeout: cfg.HTTPTimeout},
	}
}

func (b *BackupManager) Start() {
	b.initState()
	for range time.Tick(b.cfg.CheckInterval) {
		b.checkDiscovery()
		b.fetchBackups()
		b.saveState()
	}
}

func (b *BackupManager) statePath() string {
	return filepath.Join(b.cfg.BaseDir, stateFile)
}

func (b *BackupManager) initState() {
	var err error
	b.state, err = loadState(b.statePath())
	if err != nil {
		b.state = &State{
			Groups: make(map[string]*GroupState),
		}
	}
	for service, st := range b.state.Groups {
		st.setMetrics(service)
	}
}

func (b *BackupManager) saveState() {
	err := saveState(b.state, b.statePath())
	if err != nil {
		log.Errorf("Error saving state: %s", err)
	}
}

func (b *BackupManager) checkDiscovery() {
	/* TODO: We don't have a generic Iterate anymore.
	// Find all services that we care about and make sure they're in our state.
	err := discovery.Iterate(b.cfg.Cluster, func(r discovery.Record) bool {
		s := r.Name.Service
		if r.Name.User != discoveryUser || !b.svcRe.MatchString(s) {
			return true
		}
		if _, ok := b.state.Groups[s]; !ok {
			b.state.Groups[s] = new(GroupState)
			log.Infof("registering new service %q", s)
		}
		addrs := r.Addrs(discovery.Binary)
		if len(addrs) == 0 {
			log.Errorf("no addresses for %s", s)
			return true
		}
		if !slices.EqualStrings(b.state.Groups[s].Addrs, addrs) {
			b.state.Groups[s].Addrs = addrs
			log.Infof("found addresses for %s: %v", s, addrs)
		}
		return true
	})
	if err != nil {
		log.Errorf("discovery iterate error: %s", err)
	}
	*/
}

func (b *BackupManager) fetchBackups() {
	var wg sync.WaitGroup
	for service, groupState := range b.state.Groups {
		wg.Add(1)
		go b.fetchBackup(service, groupState, &wg)
	}
	wg.Wait()
}

func (b *BackupManager) fetchBackup(service string, state *GroupState, wg *sync.WaitGroup) {
	defer state.setMetrics(service)
	defer wg.Done()
	defer b.opm.Start("fetch").End()

	addrs := state.Addrs
	if len(addrs) == 0 {
		log.Errorf("no addresses for service %s", service)
		return
	}

	// Pick an address to fetch from. Try to reuse the same address so we only
	// download new snapshots. Note that all replicas snapshot independently;
	// there's no need to go to the leader, and in fact it would be nice to not
	// use the leader, to spread out load.
	perm := rand.Perm(len(addrs))
	for i := range perm {
		if addrs[perm[i]] == state.LastAddr {
			perm[0], perm[i] = perm[i], perm[0]
		}
	}

	for _, i := range perm {
		// Do the fetch.
		addr := addrs[i]
		req, err := http.NewRequest("", fmt.Sprintf(urlPattern, addr), nil)
		if state.LastEtag != "" {
			req.Header.Set("If-None-Match", state.LastEtag)
		}
		resp, err := b.cli.Do(req)
		if err != nil {
			log.Errorf("http error from %s %s: %s", service, addr, err)
			continue
		}
		now := time.Now().Unix()
		if !func() bool {
			defer func() {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}()
			if resp.StatusCode == http.StatusNotModified {
				log.Infof("snapshot not modified on %s %s", service, addr)
				state.LastCheck = now
				return false
			}
			if resp.StatusCode != http.StatusOK {
				log.Errorf("unexpected http status from %s %s: %s", service, addr, resp.Status)
				return true
			}
			filename := resp.Header.Get(filenameHeader)
			if filename == "" {
				log.Infof("snapshot reply missing filename on %s %s", service, addr)
				return true
			}

			// add a sequence number to the filename (for pruning)
			filename = fmt.Sprintf("%08x|%s", state.LastSeq+1, filename)

			log.Infof("got new snapshot for %s %s", service, addr)
			dir := filepath.Join(b.cfg.BaseDir, service)
			if err = os.MkdirAll(dir, 0755); err != nil {
				log.Errorf("couldn't mkdir %s", dir)
				return false
			}
			fn := filepath.Join(dir, filename)
			f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Errorf("couldn't create file %s", fn)
				return false
			}
			n, err := io.Copy(f, resp.Body)
			f.Close()
			if err != nil {
				log.Errorf("couldn't copy response to file %s: %s", fn, err)
				os.Remove(fn)
			}
			log.Infof("wrote snapshot to %s (%d bytes)", fn, n)
			state.LastAddr = addr
			state.LastEtag = resp.Header.Get("Etag")
			state.LastCheck = now
			state.LastBackup = now
			state.LastSeq++
			go b.pruneBackups(dir, state.LastSeq)
			return false
		}() {
			break
		}
	}
}

func (b *BackupManager) pruneBackups(dir string, lastSeq uint64) {
	time.Sleep(time.Minute)

	d, err := os.Open(dir)
	if err != nil {
		log.Errorf("error opening dir %s: %s", dir, err)
		return
	}
	names, err := d.Readdirnames(0)
	if err != nil {
		log.Errorf("error listing dir %s: %s", dir, err)
		return
	}

	toDel := findSnapshotsToDelete(names, lastSeq)

	for _, name := range toDel {
		path := filepath.Join(dir, name)
		log.Infof("pruning %s", path)
		if err := os.Remove(path); err != nil {
			log.Errorf("error removing %q: %s", path, err)
		}
	}
}

func findSnapshotsToDelete(names []string, lastSeq uint64) (out []string) {
	for _, name := range names {
		seq := getSeq(name)
		if seq == 0 {
			continue
		}
		sched := getSchedule(lastSeq - seq)
		if seq%sched.keepEvery != 0 {
			out = append(out, name)
		}
	}
	return
}

func getSchedule(ago uint64) scheduleEntry {
	for _, sched := range pruneSchedule {
		if ago <= sched.backupsAgo {
			return sched
		}
	}
	return pruneSchedule[len(pruneSchedule)-1]
}

func getSeq(name string) (seq uint64) {
	if n, err := fmt.Sscanf(name, "%x|", &seq); err != nil || n != 1 {
		log.Errorf("couldn't parse sequence from %q: %s", name, err)
		return 0
	}
	return seq
}

func (s *GroupState) setMetrics(service string) {
	metricLastCheck.WithLabelValues(service).Set(float64(s.LastCheck))
	metricLastBackup.WithLabelValues(service).Set(float64(s.LastBackup))
}
