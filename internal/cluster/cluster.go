// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/curator"
	"github.com/westerndigitalcorporation/blb/internal/tractserver"
	"github.com/westerndigitalcorporation/blb/pkg/failures"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

var join = filepath.Join

// genLocalAddr returns a new local address with a port that nobody else should be using.
func genLocalAddr() string {
	return fmt.Sprintf("localhost:%d", test.GetFreePort())
}

// Proc defines the interface that different processes should implement.
type Proc interface {
	// Process has to implement String method.
	fmt.Stringer
	// Start starts the process.
	Start() error
	// Stop stops the process.
	Stop() error
	// Name returns the name of the process.
	Name() string
	// Running returns true if the process is running.
	Running() bool
	// Pid returns the PID of a running process.
	Pid() int
	// GetFailureConfig returns its current failure configuration.
	GetFailureConfig() (map[string]*json.RawMessage, error)
	// SetFailureConfig sets the process's failure configuration to "config", in
	// entirety. The existing configuration parameters that are missing in 'config'
	// will be reset to (null)nil.
	SetFailureConfig(config map[string]interface{}) error
	// UpdateFailureConfig updates the failure configuration of the process to
	// 'updates'. Only the configuration specified in 'updates' will be updated,
	// the other configuration parameters will be intact.
	UpdateFailureConfig(updates map[string]interface{}) error
	// PostInitialMembership posts an initial raft membership to a process.
	PostInitialMembership(members []string) error
}

// ReplicaProc defines the interface that should be implemented by services that
// is replicated using Raft.
type ReplicaProc interface {
	// ReplicaProc is also a Proc
	Proc
	// RaftAddr returns the raft address of the service.
	RaftAddr() string
}

// procBase implements some common functionalities for different processes.
type procBase struct {
	name        string
	bin         string
	serviceAddr string
	loggers     []Logger
	cmd         *exec.Cmd
}

// newProcBase creates a new procBase.
func newProcBase(name, bin, serviceAddr string, loggers []Logger) *procBase {
	log.Infof("%s has addr %s", name, serviceAddr)
	return &procBase{
		name:        name,
		bin:         bin,
		serviceAddr: serviceAddr,
		loggers:     loggers,
	}
}

// Name returns the name of the process.
func (p *procBase) Name() string {
	return p.name
}

// ServiceAddr returns the service address of the process.
func (p *procBase) ServiceAddr() string {
	return p.serviceAddr
}

// start starts the process.
func (p *procBase) start(args ...string) error {
	if p.Running() {
		return fmt.Errorf("Process %s is running", p.Name())
	}

	// Always log to stderr, we'll capture and process logs.
	args = append(args, "-logtostderr", "-v=1")

	p.cmd = exec.Command(p.bin, args...)
	p.cmd.Stdout = NewLogDemuxer(p.name, p.loggers)
	p.cmd.Stderr = p.cmd.Stdout
	return p.cmd.Start()
}

// Stop stops the process.
func (p *procBase) Stop() error {
	if p.cmd == nil {
		return fmt.Errorf("Process %s has not started", p.name)
	}
	log.Infof("stopping process %s at %s", p.name, p.serviceAddr)
	if err := p.cmd.Process.Signal(syscall.SIGINT); err != nil {
		return err
	}
	err := p.cmd.Wait()
	p.cmd = nil
	return err
}

// Running returns true if a process is still running.
func (p *procBase) Running() bool {
	return p.cmd != nil
}

// Pid returns the process ID if it's running, 0 otherwise.
func (p *procBase) Pid() int {
	if !p.Running() {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *procBase) failureURL() string {
	return "http://" + p.serviceAddr + failures.DefaultFailureServicePath
}

func doPost(url string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if resp, err := http.Post(url, "application/json", bytes.NewReader(data)); err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		// The detailed error message is stored in the body of the response.
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%s:%s", resp.Status, body)
	}
	return nil
}

func (p *procBase) SetFailureConfig(config map[string]interface{}) error {
	return doPost(p.failureURL(), config)
}

func (p *procBase) UpdateFailureConfig(updates map[string]interface{}) error {
	// We first obtain current failure configurations and only update the keys
	// with the values that are specified in 'updates'.
	config, err := p.GetFailureConfig()
	if err != nil {
		return err
	}

	newConfig := make(map[string]interface{})
	for key, value := range config {
		newConfig[key] = value
	}
	for key, value := range updates {
		newConfig[key] = value
	}
	return p.SetFailureConfig(newConfig)
}

func (p *procBase) GetFailureConfig() (map[string]*json.RawMessage, error) {
	resp, err := http.Get(p.failureURL())
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// If the response indicates an error(other than 202), the detailed error
	// message is stored in the body of the response.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s:%s", resp.Status, body)
	}
	// Otherwise the body stores configuration of the failure service in JSON
	// format.
	var config map[string]*json.RawMessage
	dec := json.NewDecoder(bytes.NewBuffer(body))
	if err = dec.Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}

func (p *procBase) PostInitialMembership(members []string) error {
	url := "http://" + p.serviceAddr + "/reconfig/initial?members=" + strings.Join(members, ",")
	return doPost(url, nil)
}

// Master represents a master process.
type Master struct {
	*procBase

	masterRoot  string   // root directory of this master.
	raftRoot    string   // raft directory of this master.
	raftMembers []string // raft addresses of all members in group.
}

// newMaster creates a master object.
func newMaster(name, binDir, rootDir string, loggers []Logger) *Master {
	return &Master{
		procBase:   newProcBase(name, join(binDir, "master"), genLocalAddr(), loggers),
		masterRoot: join(rootDir, name),
		raftRoot:   join(rootDir, name, "raft"),
	}
}

// Start starts the master process.
func (m *Master) Start() error {
	return m.start("-masterCfg", join(m.masterRoot, "master.cfg"), "-raftCfg", join(m.masterRoot, "raft.cfg"))
}

func (m *Master) setup(raftMembers []string) {
	m.raftMembers = raftMembers
	// Create root directory for this master.
	if err := os.Mkdir(m.masterRoot, os.FileMode(0755)); err != nil {
		panic(fmt.Sprintf("Failed to create master directory: %v", err))
	}
	// Create raft directory.
	if err := os.Mkdir(m.raftRoot, os.FileMode(0755)); err != nil {
		panic(fmt.Sprintf("Failed to create raft directory: %v", err))
	}

	// Write configuration file for master.
	masterCfg := map[string]interface{}{
		"Addr":                     m.serviceAddr,
		"UseFailure":               true,
		"ConsistencyCheckInterval": 1 * time.Second,
	}
	if err := writeConfigFile(join(m.masterRoot, "master.cfg"), masterCfg); err != nil {
		panic(fmt.Sprintf("Failed to write master config file: %v", err))
	}

	// Write configuration file for Raft.
	if err := writeConfigFile(join(m.masterRoot, "raft.cfg"),
		getRaftCfg(m.serviceAddr, m.raftRoot)); err != nil {
		panic(fmt.Sprintf("Failed to write raft config file: %v", err))
	}
}

// String returns a human-readable string of master object.
func (m *Master) String() string {
	return fmt.Sprintf("%s [%s]", m.Name(), m.serviceAddr)
}

// RaftAddr returns address used by Raft of the master replica.
func (m *Master) RaftAddr() string {
	return m.serviceAddr
}

// Curator represents a Curator process.
type Curator struct {
	*procBase

	curatorRoot string   // root directory of this curator.
	raftRoot    string   // raft directory of this curator.
	raftMembers []string // raft addresses of all members in group.
}

// newCurator creates a new curator object.
func newCurator(name, binDir, rootDir string, loggers []Logger) *Curator {
	return &Curator{
		procBase:    newProcBase(name, join(binDir, "curator"), genLocalAddr(), loggers),
		curatorRoot: join(rootDir, name),
		raftRoot:    join(rootDir, name, "raft"),
	}
}

// Start starts the curator process.
func (c *Curator) Start() error {
	return c.start("-curatorCfg", join(c.curatorRoot, "curator.cfg"), "-raftCfg", join(c.curatorRoot, "raft.cfg"))
}

func (c *Curator) setup(raftMembers []string, masters []string) {
	c.raftMembers = raftMembers
	// Create root directory for this curator.
	if err := os.Mkdir(c.curatorRoot, os.FileMode(0755)); err != nil {
		panic(fmt.Sprintf("Failed to create curator directory: %v", err))
	}
	// Create raft directory.
	if err := os.Mkdir(c.raftRoot, os.FileMode(0755)); err != nil {
		panic(fmt.Sprintf("Failed to create raft directory: %v", err))
	}

	// Use test configuration, because the local cluster is only used for testing.
	curatorCfg := curator.DefaultTestConfig
	curatorCfg.Addr = c.serviceAddr
	curatorCfg.MasterSpec = strings.Join(masters, ",")
	curatorCfg.UseFailure = true
	if err := writeConfigFile(join(c.curatorRoot, "curator.cfg"), curatorCfg); err != nil {
		panic(fmt.Sprintf("Failed to write curator config file: %v", err))
	}

	raftCfg := getRaftCfg(c.serviceAddr, c.raftRoot)
	raftCfg["DBDir"] = c.curatorRoot
	// Write configuration file for Raft.
	if err := writeConfigFile(join(c.curatorRoot, "raft.cfg"), raftCfg); err != nil {
		panic(fmt.Sprintf("Failed to write raft config file: %v", err))
	}
}

// String returns a human-readable string of curator object.
func (c *Curator) String() string {
	return fmt.Sprintf("%s [%s]", c.Name(), c.serviceAddr)
}

// RaftAddr returns address used by Raft of the curator replica.
func (c *Curator) RaftAddr() string {
	return c.serviceAddr
}

// TractServer represents a tract server process.
type TractServer struct {
	*procBase

	tsRoot    string
	diskPaths []string
}

// newTractServer creates a new tract server object.
func newTractServer(name, binDir, rootDir string, disksPerTractserver uint, loggers []Logger) *TractServer {
	t := &TractServer{
		procBase: newProcBase(name, join(binDir, "tractserver"), genLocalAddr(), loggers),
		tsRoot:   join(rootDir, name),
	}
	for i := uint(0); i < disksPerTractserver; i++ {
		path := join(t.tsRoot, fmt.Sprintf("disk%d", i))
		t.diskPaths = append(t.diskPaths, path)
	}
	return t
}

// Start starts the tract server process.
func (t *TractServer) Start() error {
	for _, diskPath := range t.diskPaths {
		go t.addDisk(diskPath)
	}
	return t.start("-tractserverCfg", join(t.tsRoot, "ts.cfg"))
}

func (t *TractServer) setup(masters []string) {
	// Create tract server rood directory.
	if err := os.Mkdir(t.tsRoot, os.FileMode(0755)); err != nil {
		panic(fmt.Sprintf("Failed to create tractserver directory: %v", err))
	}
	// Create disk directories.
	for _, diskPath := range t.diskPaths {
		if err := os.Mkdir(diskPath, os.FileMode(0755)); err != nil {
			panic(fmt.Sprintf("Failed to create disk directory: %v", err))
		}
	}

	// Use test configuration, because the local cluster is used only for testing
	// purpose.
	tsCfg := tractserver.DefaultTestConfig
	tsCfg.Addr = t.serviceAddr
	tsCfg.MasterSpec = strings.Join(masters, ",")
	tsCfg.UseFailure = true
	tsCfg.DiskControllerBase = t.controllerDir()
	if err := writeConfigFile(join(t.tsRoot, "ts.cfg"), tsCfg); err != nil {
		panic(fmt.Sprintf("Failed to write tractserver config file: %v", err))
	}
}

func (t *TractServer) controllerDir() string  { return join(t.tsRoot, "controller") }
func (t *TractServer) controllerSock() string { return join(t.controllerDir(), t.serviceAddr) }

func (t *TractServer) addDisk(root string) {
	u := url.URL{
		Scheme: "http",
		Host:   "_", // override in DialContext below
		Path:   "/disk",
		RawQuery: url.Values{
			"root": []string{root},
		}.Encode(),
	}
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		log.Fatalf("NewRequest: %s", err)
	}

	cli := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", t.controllerSock())
		},
	}}

	// Keep doing this loop until it works.
	for delay := 50 * time.Millisecond; ; delay = delay * 3 / 2 {
		time.Sleep(delay)
		resp, err := cli.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			log.V(2).Infof("Added disk %q to %s", root, t)
			return
		}
	}
}

// String returns a human-readable string of TractServer object.
func (t *TractServer) String() string {
	return fmt.Sprintf("%s [%s]", t.Name(), t.serviceAddr)
}

// Cluster represents a Blb cluster with various processes (masters, curators
// and tractservers).
type Cluster struct {
	config      *Config
	procs       []Proc
	loggers     []Logger
	masterAddrs []string

	masters      []*Master
	curators     [][]*Curator
	tractservers []*TractServer
}

// NewCluster creates a new blb cluster object.
func NewCluster(config *Config, loggers []Logger) *Cluster {
	cluster := &Cluster{config: config, loggers: loggers}
	return cluster
}

// MasterAddrs returns addresses of master replicas of the cluster.
func (c *Cluster) MasterAddrs() []string {
	return c.masterAddrs
}

// GetConfig returns config of the cluster.
func (c *Cluster) GetConfig() *Config {
	return c.config
}

// Setup setups the environment before starting the cluster.
func (c *Cluster) Setup() error {
	// Setup masters.
	var masters []*Master
	var raftMembers []string
	for i := 0; i < int(c.config.Masters); i++ {
		m := newMaster(fmt.Sprintf("m%d", i), c.config.BinDir, c.config.RootDir, c.loggers)
		masters = append(masters, m)
		c.masters = append(c.masters, m)
		c.masterAddrs = append(c.masterAddrs, m.serviceAddr)
		c.addProc(m)
	}
	for _, m := range masters {
		m.setup(c.masterAddrs)
	}

	// Setup curators.
	for g := 0; g < int(c.config.CuratorGroups); g++ {
		raftMembers = nil
		var curators []*Curator
		for i := 0; i < int(c.config.Curators); i++ {
			curator := newCurator(fmt.Sprintf("c%d.%d", g, i), c.config.BinDir, c.config.RootDir, c.loggers)
			curators = append(curators, curator)
			raftMembers = append(raftMembers, curator.serviceAddr)
			c.addProc(curator)
		}
		c.curators = append(c.curators, curators)
		for _, curator := range curators {
			curator.setup(raftMembers, c.masterAddrs)
		}
	}

	// Setup tract servers.
	for i := 0; i < int(c.config.Tractservers); i++ {
		ts := newTractServer(fmt.Sprintf("t%d", i), c.config.BinDir, c.config.RootDir, c.config.DisksPerTractserver, c.loggers)
		c.tractservers = append(c.tractservers, ts)
		c.addProc(ts)
		ts.setup(c.masterAddrs)
	}
	return nil
}

// addProc adds a process to the cluster.
func (c *Cluster) addProc(proc Proc) {
	c.procs = append(c.procs, proc)
}

// AllProcs returns all processes of the cluster.
func (c *Cluster) AllProcs() []Proc {
	return c.procs
}

// Start starts the cluster.
func (c *Cluster) Start() error {
	for _, proc := range c.AllProcs() {
		if err := proc.Start(); err != nil {
			return err
		}
	}
	post := func(p Proc, members []string) {
		time.Sleep(500 * time.Millisecond)
		for p.PostInitialMembership(members) != nil {
			time.Sleep(500 * time.Millisecond)
		}
	}
	// Poke masters and curators to set initial configuration.
	go post(c.masters[0], c.masters[0].raftMembers)
	for _, group := range c.curators {
		go post(group[0], group[0].raftMembers)
	}
	return nil
}

// Stop stops the cluster.
func (c *Cluster) Stop() {
	for _, proc := range c.AllProcs() {
		proc.Stop()
	}
}

// Masters returns all masters. The caller must not modify the returned slice.
func (c *Cluster) Masters() []*Master {
	return c.masters
}

// Curators returns all curators, in groups. The caller must not modify the
// returned slices.
func (c *Cluster) Curators() [][]*Curator {
	return c.curators
}

// Tractservers returns all tractservers. The caller must not modify the
// returned slice.
func (c *Cluster) Tractservers() []*TractServer {
	return c.tractservers
}

// FindProc returns the process given its name.
func (c *Cluster) FindProc(name string) Proc {
	for _, proc := range c.AllProcs() {
		if proc.Name() == name {
			return proc
		}
	}
	return nil
}

// FindByServiceAddress looks up a process's name given its service address.
// It only works for masters, curators, and tractservers.
func (c *Cluster) FindByServiceAddress(addr string) (name string) {
	for _, proc := range c.AllProcs() {
		switch proc := proc.(type) {
		case *Master:
			if proc.serviceAddr == addr {
				return proc.name
			}
		case *Curator:
			if proc.serviceAddr == addr {
				return proc.name
			}
		case *TractServer:
			if proc.serviceAddr == addr {
				return proc.name
			}
		}
	}
	return ""
}

// A helper functionto write configuration to a file in JSON format.
func writeConfigFile(fpath string, config interface{}) error {
	file, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	return enc.Encode(config)
}

// A helper function to generate a map which can be serialized to a JSON Raft
// config file.
func getRaftCfg(addr string, dir string) map[string]interface{} {
	return map[string]interface{}{
		"Config": map[string]interface{}{
			"ID": addr,
		},
		"StorageConfig": map[string]interface{}{
			"SnapshotDir": dir,
			"LogDir":      dir,
			"StateDir":    dir,
		},
	}
}
