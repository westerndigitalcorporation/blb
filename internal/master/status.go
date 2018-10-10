// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	sigar "github.com/cloudfoundry/gosigar"

	log "github.com/golang/glog"
)

const statusTemplateStr = `
<!doctype html>
<html lang="en">
<head>
  <title>blb master status</title>
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

    table.curators th {
      background-color: #3399FF;
    }
  </style>
</head>

<body>

<h3>
{{if .JobName}}
	{{.JobName}}
{{else}}
	blb-master
{{end}}
{{if ne .LeaderAddr .Cfg.Addr}}  / not the leader, data below will be stale! {{end}}
</h3>

<table>
  <tr>
    <td>Addr:</td>
    <td><a href="http://{{.Cfg.Addr}}">{{.Cfg.Addr}}</a></td>
  </tr>
	<tr>
    <td>Master leader address:</td>
		{{if .LeaderAddr}}
			<td><a href="http://{{.LeaderAddr}}">{{.LeaderAddr}}</a></td>
		{{else}}
			<td>Unknown</td>
		{{end}}
  </tr>
	<tr>
    <td>Master cluster members:</td>
		<td>
			{{range .Members}}
				<a href="http://{{.}}">{{.}}</a>&nbsp
			{{end}}
		</td>
  </tr>
  <tr>
    <td>Raft term:</td>
    <td>{{.RaftTerm}}</td>
  </tr>
  <tr>
    <td>Free memory:</td>
    <td>{{.FreeMem}} / {{.TotalMem}} mb</td>
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
        <iframe src="/loglevel" name="levelframe" width=200 height=40></iframe>
      </form>
    </td>
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
<hr></hr>
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
<hr></hr>
<table class="status curators">
  <caption>Curators</caption>
  <tr>
    <th>ID</th>
    <th>Status Page</th>
    <th>Last HB</th>
    <th>New Partition Quota</th>
  </tr>
  {{range .Curators}}
  <tr>
    <td>{{.ID}}</td>
    <td><a href="http://{{.Addr}}">{{.Addr}}</a></td>
    <td>{{.LastHeartbeat}}</td>
    <td>{{.NewPartitionQuota}}</td>
  </tr>
  {{end}}
</table>

<br>
<hr></hr>
<table class="status curators">
  <caption>Tractservers</caption>
  <tr>
    <th>ID</th>
    <th>Status Page</th>
    <th>Last HB</th>
  </tr>
  {{range .Tractservers}}
  <tr>
    <td>{{.ID}}</td>
    <td><a href="http://{{.Addr}}">{{.Addr}}</a></td>
    <td>{{.LastHeartbeat}}</td>
  </tr>
{{end}}
</table>

status update time: {{.Now}}
</body>
</html>
`

// StatusData includes master status info.
type StatusData struct {
	JobName      string
	Cfg          Config
	LeaderAddr   string
	Members      []string
	RaftTerm     uint64
	FreeMem      uint64
	TotalMem     uint64
	Curators     []CuratorInfo
	Tractservers []TractserverInfo

	Reboot time.Time
	CtlRPC map[string]string
	SrvRPC map[string]string
	Now    time.Time
}

const mb = 1024 * 1024

var (
	// When was the last reboot?
	reboot = time.Now()

	// Status html template.
	statusTemplate = template.Must(template.New("status_html").Parse(statusTemplateStr))
)

// statusHandler is called when somebody makes a http request to a status port.
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

	// Pull curator status.
	curators, tractservers := s.master.getHeartbeats()

	// Prepare data.
	return StatusData{
		JobName:      "master",
		Cfg:          s.cfg,
		LeaderAddr:   s.master.stateHandler.LeaderID(),
		Members:      s.master.stateHandler.GetClusterMembers(),
		RaftTerm:     s.master.stateHandler.GetTerm(),
		FreeMem:      mem.ActualFree / mb,
		TotalMem:     mem.Total / mb,
		Curators:     curators,
		Tractservers: tractservers,
		Reboot:       reboot,
		CtlRPC:       s.ctlHandler.rpcStats(),
		SrvRPC:       s.srvHandler.rpcStats(),
		Now:          time.Now(),
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
