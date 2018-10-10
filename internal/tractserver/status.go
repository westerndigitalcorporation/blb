// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package tractserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	sigar "github.com/cloudfoundry/gosigar"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/internal/core"
)

const statusTemplateStr = `
<!doctype html>
<html lang="en">
<head>
  <title>blb tractserver status</title>
  <style>
    caption {
      caption-side: top;
      text-align: left;
      font-weight: bold;
    }
    table.status {
      border-collapse: collapse;
    }
    table.status td {
      border: 1px solid #DDD;
      text-align: left;
      padding-left: 8px;
      padding-right: 8px;
      padding-top: 4px;
      padding-bottom: 4px;
    }
    table.status th {
      border: 1px solid #DDD;
      text-align: left;
      padding: 8px;
      background-color: #009900;
      color: white;
    }
    table.status tr:nth-child(even) {background-color: #F2F2F2;}
    table.status tr:hover {background-color: #DDD;}

    table.disks th {
      background-color: #3399FF;
    }
  </style>
</head>

<body>

<h3>
{{if .JobName}}
	{{.JobName}}
{{else}}
	blb-tractserver
{{end}}
</h3>

<table>
  <tr>
    <td>ID:</td>
    <td>{{.ID}}</td>
  </tr>
  <tr>
    <td>Address:</td>
    <td><a href="http://{{.Cfg.Addr}}">{{.Cfg.Addr}}</a></td>
  </tr>
  <tr>
    <td>Num of disks:</td>
    <td>{{len .Cfg.DiskRoots}}</td>
  </tr>
  <tr>
    <td>Free memory:</td>
    <td>{{byteToMB .FreeMem}} / {{byteToMB .TotalMem}} mb</td>
  </tr>
	<tr>
    <td>Last reboot:</td>
    <td>{{.Reboot}}</td>
  </tr>
  <tr>
    <td>Set logging level:</td>
    <td>
      <form action="/loglevel" target="levelframe" method="get">
        <input type="radio" name="v" value="TRACE"> TRACE
        <input type="radio" name="v" value="INFO" checked> INFO
        <input type="radio" name="v" value="ERROR"> ERROR
	<input type="submit" value="Apply">
	Current server logging level is: <iframe src="/loglevel" name="levelframe" width=200 height=40></iframe>
      </form>
  </tr>
</table>

<br>
Show logs:
<form action="/logs" target="logsframe" method="get">
  <input type="radio" name="v" value="TRACE"> TRACE
  <input type="radio" name="v" value="INFO" checked> INFO
  <input type="radio" name="v" value="ERROR"> ERROR
  | Max age: <input name="from" type="text" value="30s">
  | Regex: <input name="pattern" type="text">
  <input type="submit" value="Apply">
</form>
<br>
<iframe src="/logs?from=30s" name="logsframe" width=100% height=40%></iframe>

<br>
<br>
<table class="status">
  <caption>Control RPC Metrics</caption>
  <tr>
    <th>Metric</th>
    <th>Stats</th>
  </tr>
  {{range $k, $v := .CtlRPC}}
  <tr>
    <td>{{$k}}</td>
    <td>{{$v}}</td>
  </tr>
  {{end}}
</table>

<br>
<table class="status">
  <caption>Service RPC Metrics</caption>
  <tr>
    <th>Metric</th>
    <th>Stats</th>
  </tr>
  {{range $k, $v := .SrvRPC}}
  <tr>
    <td>{{$k}}</td>
    <td>{{$v}}</td>
  </tr>
  {{end}}
</table>

<br>
<table class="status disks">
  <caption>Disks</caption>
  <tr>
    <th>Root</th>
    <th>Num of Tracts</th>
    <th>Num Deleted / Unknown</th>
    <th>Available Space</th>
    <th>Full</th>
    <th>Healthy</th>
    <th>StopAllocating</th>
    <th>Drain</th>
    <th>DrainLocal</th>
  </tr>
  {{range .FsStatus}}
  <tr>
    <td>{{.Status.Root}}</td>
    <td>{{.NumTracts}}</td>
    <td>{{.NumDeletedTracts}} / {{.NumUnknownFiles}}</td>
    <td>{{byteToMB .AvailSpace}} / {{byteToMB .TotalSpace}} MB</td>
    <td>{{.Status.Full}}</td>
    <td>{{.Status.Healthy}}</td>
    <td>{{.Status.Flags.StopAllocating}}</td>
    <td>{{.Status.Flags.Drain}}</td>
    <td>{{.Status.Flags.DrainLocal}}</td>
  </tr>
  {{end}}
</table>

<br>
<h2>Per-disk per-op stats</h2>
<br>
{{range .FsStatus}}
<br>
<table class="status disks">
  <caption>{{.Status.Root}}</caption>
  <tr>
    <th>Op</th>
    <th>Stats</th>
  </tr>
  {{range $op, $stat := .Ops}}
  <tr>
    <td>{{$op}}</td>
    <td>{{$stat}}</td>
  </tr>
  {{end}}
</table>
{{end}}

<br>
status update time: {{.Now}}
</body>
</html>
`

// StatusData includes tractserver status info.
type StatusData struct {
	JobName  string
	Cfg      Config
	ID       core.TractserverID
	FreeMem  uint64
	TotalMem uint64

	FsStatus []core.FsStatus

	Reboot time.Time // When was the last reboot?
	CtlRPC map[string]string
	SrvRPC map[string]string
	Now    time.Time
}

// Convert bytes into mbs.
func byteToMB(in uint64) uint64 {
	return in / 1024 / 1024
}

var (
	// When was the last reboot?
	reboot = time.Now()

	// Add custom functions.
	funcMap = template.FuncMap{"byteToMB": byteToMB}

	// Status html template.
	statusTemplate = template.Must(template.New("status_html").Funcs(funcMap).Parse(statusTemplateStr))
)

// statusHandler is called when an http request is received at the status port.
// If the "Accept" header is set to be "application/json", it sends json encoded
// status; otherwise it sends html.
func (s *Server) statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("Accept") == "application/json" {
		s.handleJSON(w)
	} else {
		s.handleHTML(w)
	}
}

// Generate status data.
func (s *Server) genStatus() StatusData {
	// Pull memory info.
	mem := sigar.Mem{}
	if err := mem.Get(); nil != err {
		log.Errorf("failed to get memory info: %s", err)
		mem.ActualFree = 0
		mem.Total = 0
	}

	// Prepare data.
	return StatusData{
		JobName:  "tractserver",
		Cfg:      *s.cfg,
		ID:       s.store.GetID(),
		FreeMem:  mem.ActualFree,
		TotalMem: mem.Total,
		FsStatus: s.getStatus(),
		Reboot:   reboot,
		CtlRPC:   s.ctlHandler.rpcStats(),
		SrvRPC:   s.srvHandler.rpcStats(),
		Now:      time.Now(),
	}
}

func (s *Server) handleHTML(w http.ResponseWriter) {
	var b bytes.Buffer
	if err := statusTemplate.Execute(&b, s.genStatus()); err != nil {
		e := fmt.Sprintf("failed to encode html status data: %s", err)
		log.Errorf(e)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(e))
		return
	}

	w.Header().Set("Content-Type", "text/html")
	w.Write(b.Bytes())
}

func (s *Server) handleJSON(w http.ResponseWriter) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(s.genStatus()); err != nil {
		e := fmt.Sprintf("failed to encode json status data: %s", err)
		log.Errorf(e)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(e))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(b.Bytes())
}
