// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"

	client "github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/cluster"
)

// RunTest runs a test with the given name. If the test succeeds no error
// should be returned.
func RunTest(name string, clusterCfg cluster.Config, testCfg TestConfig) error {
	// 1 - Create the resource.

	tc := &TestCase{
		name:       name,
		capture:    &captureLogger{listeners: make(map[chan logLine]bool)},
		clusterCfg: clusterCfg,
		testCfg:    testCfg,
	}

	// The controller runs each test process with its working directory set to
	// the directory it should use for RootDir.
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}
	tc.clusterCfg.RootDir = wd

	tlog := cluster.NewTerminalLogger(tc.clusterCfg.LogConfig)
	tc.bc = cluster.NewCluster(
		&tc.clusterCfg,
		[]cluster.Logger{tlog, tc.capture},
	)

	// Direct our log output to the demuxer too.
	redirectStderr(tlog)

	log.Infof("setting up the cluster...")
	tc.bc.Setup()
	log.Infof("starting the cluster...")
	capture := tc.captureLogs()
	tc.bc.Start()
	cluster := strings.Join(tc.bc.MasterAddrs(), ",")
	tc.c = client.NewClient(client.Options{Cluster: cluster, DisableCache: testCfg.DisableCache})
	tc.noRetryC = client.NewClient(client.Options{Cluster: cluster, DisableCache: true, DisableRetry: true})

	// 2 - Wait the cluster is fully setup and functional.

	// Wait for the cluster to settle down.
	log.Infof("waiting for the cluster to initialize...")
	// Wait for some master to become leader,
	waitFor := []string{"m.:@@@ became leader"}
	var i uint
	for i = 0; i < tc.clusterCfg.CuratorGroups; i++ {
		// each curator group to get a partition,
		waitFor = append(waitFor, fmt.Sprintf("c%d:@@@ curator group .* initialized", i))
		// each curator group to get a successful heartbeat from all tractservers,
		waitFor = append(waitFor, fmt.Sprintf("c%d:@@@ tsmon: %d hosts, %d healthy",
			i, tc.clusterCfg.Tractservers, tc.clusterCfg.Tractservers))
	}
	for i = 0; i < tc.clusterCfg.Tractservers; i++ {
		// and each tractserver to register with the master.
		waitFor = append(waitFor, "m.:@@@ received registration request from tractserver")
	}
	if capture.WaitFor(waitFor...) != nil {
		log.Fatalf("cluster did not initialize")
	}

	// 3 - Run the test.

	tt := reflect.TypeOf(tc)
	method, ok := tt.MethodByName(name)
	if !ok {
		return fmt.Errorf("Failed to find test %q method", name)
	}

	// We have to stop the blb cluster when test is done because the cluster has
	// spawned child processes to run masters, curators and tract servers and if
	// the process terminates normally without calling "Stop" these child processes
	// will not be terminated.
	//
	// If the process terminates by receiving some signals we don't have to call
	// "Stop" because the process and all these child processes belong to the
	// same process group thus they will receive the same signal and exit at
	// that time.
	defer tc.bc.Stop()
	defer tc.capture.Close()

	log.Infof("[%s] started", method.Name)
	var v reflect.Value
	if v = method.Func.Call([]reflect.Value{reflect.ValueOf(tc)})[0]; v.IsNil() {
		log.Infof("[%s] passed", method.Name)
		return nil
	}
	err = v.Interface().(error)
	log.Errorf("[%s] failed: %s", method.Name, err)
	return err
}

func redirectStderr(logger cluster.Logger) {
	r, w, _ := os.Pipe()
	os.Stderr = w
	demux := cluster.NewLogDemuxer("test", []cluster.Logger{logger})
	go io.Copy(demux, r)
}

// TestCase includes everyting that is needed to run a test of testblb.
type TestCase struct {
	name       string // the name of the test case.
	clusterCfg cluster.Config
	testCfg    TestConfig
	bc         *cluster.Cluster // the cluster that will be used in test.
	c          *client.Client   // the client that will be used to interact with the cluster.
	noRetryC   *client.Client   // a client that doesn't do retries, useful for some tests
	capture    *captureLogger
	err        error
}

// captureLogs allows a goroutine to capture logs from the processes and wait
// until a given set of lines has been observed. To use it, call CaptureLogs to
// begin a section, then perform some action that you expect to have a result
// that will show up in process logs. Finally, call WaitFor on the opaque object
// that was returned. WaitFor will block until all the requested lines have been
// observed.
//
// Lines logged in between CaptureLogs and WaitFor will be buffered in a limited
// go channel, so to avoid issues, the action should be relatively fast (say
// under a few seconds).
func (t *TestCase) captureLogs() *captureInstance {
	return &captureInstance{cl: t.capture, ch: t.capture.listen()}
}

type logLine struct {
	name string
	line string
}

type captureLogger struct {
	lock      sync.Mutex
	listeners map[chan logLine]bool
}

// Log implements the cluster.Logger interface.
func (c *captureLogger) Log(name string, line string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for ch := range c.listeners {
		ch <- logLine{name, line}
	}
}

// Close implements the cluster.Logger interface.
func (c *captureLogger) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for listener := range c.listeners {
		// Notify no further messages to the channels.
		close(listener)
	}
	// Release all listeners.
	c.listeners = make(map[chan logLine]bool)
}

func (c *captureLogger) listen() chan logLine {
	// Use a huge channel so we don't have to worry about buffering.
	ch := make(chan logLine, 20000)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.listeners[ch] = true
	return ch
}

func (c *captureLogger) release(ch chan logLine) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.listeners, ch)
}

type captureInstance struct {
	cl *captureLogger
	ch chan logLine
}

// WaitFor blocks this goroutine until BlbTester has seen all of the
// requested lines logged by processes in the cluster. The lines are specified
// as a pair of regular expressions, joined into a string separated by a ':'
// (this is just for ease of typing), for the process name and log line
// contents, respsctively. The process name regexp is anchored at the start; the
// line regexp is not anchored. The requested lines may show up in any order.
// Each observed line is only used to match one requested line, so if you want
// to see multiple copies of a line, simply use multiple copies of the same
// regexp.
func (c *captureInstance) WaitFor(lines ...string) error {
	defer c.cl.release(c.ch)

	type rePair struct{ name, line *regexp.Regexp }
	wanted := make([]*rePair, len(lines))
	for i, line := range lines {
		parts := strings.SplitN(line, ":", 2)
		wanted[i] = &rePair{
			name: regexp.MustCompile("^" + parts[0]),
			line: regexp.MustCompile(parts[1]),
		}
	}

	timeout := time.After(waitForLogTimeout)
	count := len(lines)

	for {
		select {
		case line := <-c.ch:
			for i, re := range wanted {
				if re != nil &&
					re.name.MatchString(line.name) &&
					re.line.MatchString(line.line) {
					wanted[i] = nil
					count--
					break
				}
			}
			if count == 0 {
				return nil
			}
		case <-timeout:
			return fmt.Errorf("WaitFor timeout reached")
		}
	}
}

// makeRandom returns a slice of n random bytes.
func makeRandom(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
