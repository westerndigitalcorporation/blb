// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package testblb

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	log "github.com/golang/glog"
)

const (
	kb = 1024               // How many bytes are there in a kilobyte,
	mb = 1024 * 1024        // ... and in a megabyte,
	gb = 1024 * 1024 * 1024 // ... and in a gigabyte.

	waitForLogTimeout = 5 * time.Minute // When to give up on waiting.
)

type TestResult struct {
	Name    string
	Err     error
	LogPath string
	Elapsed time.Duration
}

// RunController is called by the controller process to initialize resources
// and spawn worker processes to run tests that need to be run. The test results
// will be returned as a map which has test names as keys and results as values.
func RunController(testCfg TestConfig, binDir, workDir string, neededPorts int) (ret []TestResult) {
	// Cleanup all stuff left by previous tests, if there's any.
	log.Infof("Cleaning up %q...", workDir)
	os.RemoveAll(workDir)

	// Create root directory for subsequent tests.
	log.Infof("Creating directory %q...", workDir)
	if err := os.Mkdir(workDir, os.FileMode(0755)); err != nil {
		log.Fatalf("Failed to create directory: %q", workDir)
	}

	// Tests to run must be named "TestXXX" and also match the user's requested
	// test pattern. They accept no arguments and return a single error.
	match := regexp.MustCompile("^Test.+")
	// Make the user's pattern case-insensitive and fully anchored.
	userMatch := regexp.MustCompile("(?i)" + testCfg.TestPattern)

	// Use reflection to find out tests(methods of TestCase) that need to be run.
	tt := reflect.TypeOf(&TestCase{})
	var numTested int

	testMethodsCh := make(chan string)
	testResultsCh := make(chan TestResult, tt.NumMethod())

	// Start workers for running tests in parallel.
	log.Infof("Using %d workers to run tests in parallel", testCfg.NumWorkers)
	for i := 0; i < testCfg.NumWorkers; i++ {
		go testWorker(workDir, binDir, neededPorts, testMethodsCh, testResultsCh)
	}

	for i := 0; i < tt.NumMethod(); i++ {
		method := tt.Method(i)
		methodName := method.Name

		if !match.MatchString(methodName) || !userMatch.MatchString(methodName) {
			continue
		}

		// Tests named with "Long" as suffix tend to run for an extended period
		// of time and thus are disabled by default.
		if !testCfg.TestLong && strings.HasSuffix(methodName, "Long") {
			continue
		}

		testMethodsCh <- method.Name
		numTested++
	}

	for i := 0; i < numTested; i++ {
		ret = append(ret, <-testResultsCh)
	}
	close(testMethodsCh)

	return
}

// Each test worker picks tests from the channel and starts a subprocess to run
// each test one at a time.
func testWorker(rootDir, binDir string, neededPorts int, testMethodsCh chan string, testResultsCh chan TestResult) {
	for testName := range testMethodsCh {
		log.Infof("[%s] starting...", testName)
		// Create root directory for this test.
		var err error
		testRoot := filepath.Join(rootDir, testName)
		if err := os.Mkdir(testRoot, os.FileMode(0755)); err != nil {
			log.Fatalf("Failed to create test directory: %v", err)
		}
		// Create log file for this test.
		logPath := filepath.Join(testRoot, "test.log")
		logFile, err := os.Create(logPath)
		if err != nil {
			log.Fatalf("Failed to create log file for test %q: %v", testName, err)
		}

		// Create a command to run the subprocess, passes the controller process's
		// flags to the subprocess plus the special flag that tells the subprocess
		// which test to run.
		// We explicitly override -bin_dir because the value we're using may not
		// have come directly from a flag.
		bin := filepath.Join(binDir, filepath.Base(os.Args[0]))
		cmd := exec.Command(bin, append([]string{"-bin_dir", binDir, "-_test_to_run", testName}, os.Args[1:]...)...)
		// Redirect the subprocess's output to the log file of the test.
		cmd.Stderr = logFile
		cmd.Stdout = logFile
		cmd.Dir = testRoot // Run the test in its root dir.
		start := time.Now()
		err = cmd.Run()
		elapsed := time.Since(start)

		if err == nil {
			log.Infof("[%s] passed (%.0f seconds)", testName, elapsed.Seconds())
		} else {
			logFile.WriteString(fmt.Sprintf("-----\nProcess exited: %v\n", err))
			log.Infof("[%s] failed (%.0f seconds)", testName, elapsed.Seconds())
		}

		logFile.Close()
		testResultsCh <- TestResult{Name: testName, Err: err, LogPath: logPath, Elapsed: elapsed}
	}
}
