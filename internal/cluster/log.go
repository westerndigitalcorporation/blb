// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

var (
	processStartTime = time.Now()
	thisYear         = processStartTime.Format("2006")
	timestampRe      = regexp.MustCompile(`^.\d\d\d\d \d\d:\d\d:\d\d\.\d{6}`)
	glogTimeFormat   = "20060102 15:04:05.000000"
)

// Logger defines the interface for different loggers.
type Logger interface {
	// Log submits a log message to logger.
	Log(name string, line string)
	// Close closes the logger.
	Close()
}

// TerminalLogger logs messages to terminal.
type TerminalLogger struct {
	config *LogConfig
}

// NewTerminalLogger creates a new TerminalLogger.
func NewTerminalLogger(config *LogConfig) *TerminalLogger {
	return &TerminalLogger{config: config}
}

// Log implements logger.
func (t *TerminalLogger) Log(name, line string) {
	// The terminal log is disabled, discard the line.
	if !t.config.GetTermLogOn() {
		return
	}
	// Strip the "threadid" (pid). It's useless in go.
	line = line[:22] + line[30:]
	// If required, strip the log prefix.
	if !t.config.GetLogPrefix() {
		if strings.Contains(line, "] ") {
			line = line[strings.Index(line, "] ")+len("] "):]
		}
	}
	if t.config.GetRewriteTimes() {
		line = timestampRe.ReplaceAllStringFunc(line, fromProcessStartTime)
	}
	line = fmt.Sprintf("%4s %s\n", name, line)
	pattern := t.config.GetPattern()
	if pattern != "" {
		if ok, _ := regexp.MatchString(pattern, line); !ok {
			return
		}
	}
	fmt.Print(line)
}

// Close implements Logger.
func (t *TerminalLogger) Close() {}

// FileLogger logs messages to a file.
type FileLogger struct {
	file *os.File
}

// NewFileLogger returns a new FileLogger.
func NewFileLogger(path string) (*FileLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &FileLogger{file: f}, nil
}

// NewFileLoggerFromFile returns a new FileLogger.
func NewFileLoggerFromFile(f *os.File) *FileLogger {
	return &FileLogger{file: f}
}

// Log implements Logger.
func (f *FileLogger) Log(name, line string) {
	fmt.Fprintf(f.file, "%s : %s\n", name, line)
}

// Close implements Logger.
func (f *FileLogger) Close() {
	f.file.Close()
}

// LogDemuxer is a Writer that sends line-based output to a set of Loggers.
type LogDemuxer struct {
	name    string
	loggers []Logger
	buffer  []byte
}

// NewLogDemuxer creates a LogDemuxer with the given name and loggers.
func NewLogDemuxer(name string, loggers []Logger) *LogDemuxer {
	return &LogDemuxer{name: name, loggers: loggers}
}

// Write implements Writer interface so LogDemuxer can capture the output of
// the running process.
func (l *LogDemuxer) Write(s []byte) (n int, err error) {
	// 's' might contain multiple lines, we have to split it into
	// lines ourselves.
	reader := bufio.NewReader(io.MultiReader(
		bytes.NewReader(l.buffer),
		bytes.NewReader(s)))
	for {
		line, e := reader.ReadBytes('\n')
		if e == nil {
			// Got a full line.
			lineStr := string(bytes.TrimSpace(line))
			for _, logger := range l.loggers {
				logger.Log(l.name, lineStr)
			}
		} else if e == io.EOF {
			// Hit the end, buffer whatever's left.
			l.buffer = line
			return len(s), nil
		} else {
			// Unexpected error
			return 0, e
		}
	}
}

func fromProcessStartTime(s string) string {
	// Note s[0] is the level character
	t, err := time.ParseInLocation(glogTimeFormat, thisYear+s[1:], time.Local)
	if err != nil {
		return s
	}
	const M = time.Second / time.Microsecond
	µs := t.Sub(processStartTime) / time.Microsecond
	return fmt.Sprintf("%c%4d.%06d", s[0], µs/M, µs%M)
}
