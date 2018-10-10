// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// We should send our own log output to stderr.
	flag.Set("logtostderr", "true")
	flag.Parse()

	cli := newBlbCli()

	// Catch INT and TERM singals so we can still cleanup the temp dirs when
	// the process is forced to quit.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func() {
		<-c
		// Clean up Once received an INT or TERM signal we'll do some cleanups before exit.
		cli.stop()
		os.Exit(1)
	}()

	cli.run(os.Args)
	cli.stop()
}
