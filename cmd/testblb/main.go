// Copyright (c) 2015 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/golang/glog"

	"github.com/westerndigitalcorporation/blb/internal/cluster"
	"github.com/westerndigitalcorporation/blb/internal/testblb"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

// Flags for config parameters.
var (
	// Cluster config.
	binDir        = flag.String("bin_dir", "", "Location of binaries")
	logOn         = flag.Bool("log", true, "Display process logs")
	logPrefix     = flag.Bool("logprefix", true, "Include log line prefix")
	level         = flag.String("loglevel", "INFO", "Sets log level")
	masters       = flag.Uint("masters", 3, "Number of master replicas")
	curators      = flag.Uint("curators", 3, "Number of curator replicas per curator group")
	curatorGroups = flag.Uint("curator_groups", 2, "Number of curator groups")
	tractservers  = flag.Uint("tractservers", 10, "Number of tract servers")
	disksPerTs    = flag.Uint("disks_per_tractserver", 4, "Number of disks for each tract server")

	// Test config.
	testTime        = flag.Duration("test_time", 30*time.Minute, "How long each test run")
	writeInterval   = flag.Duration("write_interval", time.Second, "How often a blob is written")
	failureInterval = flag.Duration("failure_interval", 30*time.Second, "How often a server is killed")
	writeSizeLowMB  = flag.Int("write_size_low", 1, "The smallest write size in mb")
	writeSizeHighMB = flag.Int("write_size_high", 64, "The largest write size in mb")
	numWriters      = flag.Int("num_writers", 5, "Number of concurrent writers")
	disableCache    = flag.Bool("disable_cache", false, "Whether client cache is disabled")
	testPattern     = flag.String("tests", ".*", "Regex matching tests to run")
	testlong        = flag.Bool("testlong", false, "whether long tests should be run")
	nWorkers        = flag.Int("n_workers", 6, "number of test workers to run in parallel")
	workDir         = flag.String("work_dir", "testblb-out", "directory for work and test files")

	// Used internally.
	//
	// Clients will start the binary as a controller process, and the controller
	// process will spawn a subprocess for each test. Both processes share the
	// same binary and it uses this flag to distinguish whether it's a controller
	// process or a worker process. If this flag is set, then the binary is
	// started as a worker process that should just run the test that specified
	// in the flag.
	testToRun = flag.String("_test_to_run", "", "internal use, DO NOT set it!")
)

func main() {
	// Parse the flags into config parameters.
	flag.Parse()

	testCfg := testblb.TestConfig{
		TestTime:        *testTime,
		WriteInterval:   *writeInterval,
		FailureInterval: *failureInterval,
		WriteSizeLowMB:  *writeSizeLowMB,
		WriteSizeHighMB: *writeSizeHighMB,
		MaxReplFactor:   5, // At most 5 replicas for each tract.
		NumWriters:      *numWriters,
		DisableCache:    *disableCache,
		TestLong:        *testlong,
		TestPattern:     *testPattern,
		NumWorkers:      *nWorkers,
	}

	// Set ourself to log to stderr.
	flag.Set("logtostderr", "true")

	if *testToRun == "" {
		//
		// ---- controller process ---
		//
		// If "testToRun" is empty then it means the binary is started by clients. We
		// treat it as a controller process that will spawn subprocesses to run
		// specific tests.

		// If bin dir is not specified, find one to use.
		if *binDir == "" {
			*binDir = test.FindBinDir()
		}

		// Make binDir and workDir absolute paths.
		if abs, err := filepath.Abs(*binDir); err == nil {
			*binDir = abs
		}
		if abs, err := filepath.Abs(*workDir); err == nil {
			*workDir = abs
		}

		// Verify we have all required binaries.
		bins := []string{"master", "curator", "tractserver"}
		if err := test.CheckBinaries(*binDir, bins); err != nil {
			log.Fatalf(err.Error())
		}

		// Each test needs a port for each server.
		neededPorts := int(*masters + (*curators)*(*curatorGroups) + *tractservers)

		ret := testblb.RunController(testCfg, *binDir, *workDir, neededPorts)

		var passed []string
		var failed []testblb.TestResult
		for _, result := range ret {
			if result.Err == nil {
				passed = append(passed, result.Name)
			} else {
				failed = append(failed, result)
			}
		}
		log.Infof("Test summary: %d passed, %d failed", len(passed), len(failed))
		log.Infof("Passed: %s", strings.Join(passed, ", "))
		if len(failed) > 0 {
			log.Infof("Failed:")
			for _, result := range failed {
				log.Infof("  %s (%.0fs), logs at %q", result.Name, result.Elapsed.Seconds(), result.LogPath)
			}
			os.Exit(1)
		}
	} else {
		//
		// ---- worker process ---
		//
		// If "testToRun" is not empty then the binary is started by the controller
		// process to run a specific test.

		clusterCfg := cluster.Config{
			LogConfig:           cluster.DefaultLogConfig(),
			BinDir:              *binDir,
			Masters:             *masters,
			Curators:            *curators,
			Tractservers:        *tractservers,
			CuratorGroups:       *curatorGroups,
			DisksPerTractserver: *disksPerTs,
		}
		clusterCfg.SetTermLogOn(*logOn)
		clusterCfg.SetLogPrefix(*logPrefix)
		clusterCfg.SetRewriteTimes(true)

		if err := testblb.RunTest(*testToRun, clusterCfg, testCfg); err != nil {
			log.Errorf("Test failed: %s", err)
			os.Exit(1)
		}
	}
}
