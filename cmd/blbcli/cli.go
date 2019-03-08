// Copyright (c) 2016 Western Digital Corporation or its affiliates.  All rights reserved.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	shlex "github.com/flynn-archive/go-shlex"
	"github.com/peterh/liner"

	log "github.com/golang/glog"
	"github.com/westerndigitalcorporation/blb/client/blb"
	"github.com/westerndigitalcorporation/blb/internal/cluster"
	"github.com/westerndigitalcorporation/blb/internal/core"
	"github.com/westerndigitalcorporation/blb/internal/fuse"
	test "github.com/westerndigitalcorporation/blb/pkg/testutil"
)

const (
	// shortDiskTemplate allows using short disk names with the tsadmin and
	// related commands. If a disk name contains no slashes, it's substituted in
	// the X in this string.
	shortDiskTemplate = "/disk/X/data/tractserver"
)

var usage = `
	blbcli is a tool to interact with a running Blb cluster. It also provides a
	way to start a new cluster locally and manipulate the local cluster while
	interacting with it.

	You can use blbcli in two modes: either issue one command to a given cluster
	or start a command line interpreter to issue commands interactively. You can
	issue just one command to a given cluster by typing something like:

		cli [--cluster <cluster>] [(--setup <setup-commands>)...] <subcommand> [<flags>...]

	You can see a list of sub-commands in command section, any sub-commands can be
	used as setup commands.

	Alternatively, you can start a command line interpreter by typing something like:

		cli [--cluster <cluster>] [(--setup <setup-commands>)...] shell

	In this mode you are able to issue commands in an interpreter interactively.

	blbcli can either connect to a running Blb cluster by giving its cluster
	name or specific master addresses in the --cluster flag, or it can start a
	local cluster by issuing command 'start_cluster' to it, this will let the
	blbcli create a local cluster and connect to it.

	Flag 'setup' allows the cli to run some commands at startup time so you can
	use it to setup envinronment. For example, the command below will start
	a local cluster and create a blob in it, then it will start the shell to wait
	for further commands from users.

		cli --setup start_cluster --setup create shell
	`

// blbCli lets users interact(e.g. create/write/read) with a Blb cluster no
// matter it's remote or local. But blbCli can only manipulate(e.g. kill/restart a
// services) the connected cluster when the cluster is started locally.
type blbCli struct {
	// the actual client we'll use to talk Blb cluster.
	clt *blb.Client
	// Cache key to know when we can reuse clt.
	cltCacheKey string
	// the command line framework we'll use to launch commands.
	app *cli.App
	// blbCli can connect to either a local(started by itself) or remote cluster. When it
	// connects to a local cluster it can manipulate the cluster by killing, restarting
	// servers. 'cluster' is not nil when it's connecting to a local cluster, otherwise
	// 'cluster' will be nil.
	cluster *cluster.Cluster
	// State for fuse mounts.
	mountState *fuse.MountState
	// True if we are running a shell.
	inShell bool
}

// newBlbCli creates a new blbCli object.
func newBlbCli() *blbCli {
	b := &blbCli{}
	app := cli.NewApp()
	app.Name = "blbcli"

	app.Usage = usage
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "cluster, master, c, m",
			Usage: "Cluster[/User[/Service]] to look up blb masters using service discovery, or comma-separated hostnames",
		},
		cli.StringSliceFlag{
			Name:  "setup",
			Usage: "Commands to run before doing anything else, separated by semicolon",
		},
		cli.BoolTFlag{
			Name:  "reconstruct",
			Usage: "Enable client-side erasure coding reconstruction",
		},
		cli.IntFlag{
			Name:  "buffer",
			Usage: "Buffer size for reads and writes without explicit length",
			Value: 8 << 20,
		},
	}

	blobflag := cli.StringFlag{
		Name:  "blob, b",
		Usage: "blob id",
	}
	offsetflag := cli.IntFlag{
		Name:  "offset, o",
		Usage: "offset within blob to read/write (default: 0)",
	}
	lengthflag := cli.IntFlag{
		Name:  "length, l",
		Usage: "data length to read (unset or <= 0 means 'all')",
	}
	datafileflag := cli.StringFlag{
		Name:  "file, f",
		Usage: "file to read or write data from (required for input, output defaults to stdout)",
	}
	replflag := cli.IntFlag{
		Name:  "repl",
		Usage: "replication factor for new blob (default: 3)",
		Value: 3,
	}
	verboseFlag := cli.BoolFlag{
		Name:  "verbose, v",
		Usage: "set this flag to add additional verbosity",
	}

	// flags used by blb cluster.
	masterFlag := cli.IntFlag{
		Name:  "masters",
		Usage: "Number of master replicas",
		Value: 3,
	}
	curatorFlag := cli.IntFlag{
		Name:  "curators",
		Usage: "Number of curator replicas per curator group",
		Value: 3,
	}
	curatorGroupFlag := cli.IntFlag{
		Name:  "curator_groups",
		Usage: "Number of curator groups",
		Value: 2,
	}
	tsFlag := cli.IntFlag{
		Name:  "tractservers",
		Usage: "Number of tract servers",
		Value: 5,
	}
	diskFlag := cli.IntFlag{
		Name:  "disks_per_tractserver",
		Usage: "Number of disks per tract server",
		Value: 4,
	}
	binFlag := cli.StringFlag{
		Name:  "bin_dir",
		Usage: "Location of binaries",
		Value: "",
	}

	app.Commands = []cli.Command{
		{
			Name:    "info",
			Aliases: []string{"i"},
			Usage:   "Prints info about this blb cluster.",
			Action:  b.cmdInfo,
		},
		{
			Name:    "create",
			Aliases: []string{"c"},
			Usage:   "Creates a new blob.",
			Flags: []cli.Flag{
				replflag,
			},
			Action: b.cmdCreate,
		},
		{
			Name:    "stat",
			Aliases: []string{"s"},
			Usage:   "Stats a blob.",
			Flags: []cli.Flag{
				blobflag,
				verboseFlag,
			},
			Action: b.cmdStat,
		},
		{
			Name:    "rm",
			Aliases: []string{"delete"},
			Usage:   "Deletes a blob.",
			Flags: []cli.Flag{
				blobflag,
			},
			Action: b.cmdRm,
		},
		{
			Name:    "unrm",
			Aliases: []string{"undelete"},
			Usage:   "Un-deletes a blob.",
			Flags: []cli.Flag{
				blobflag,
			},
			Action: b.cmdUnRm,
		},
		{
			Name:    "setmd",
			Aliases: []string{"setmetadata"},
			Usage:   "Changes blob metadata",
			Flags: []cli.Flag{
				blobflag,
				cli.StringFlag{
					Name:  "hint",
					Usage: "storage hint",
				},
				cli.StringFlag{
					Name:  "mtime",
					Usage: "modified (write) time",
				},
				cli.StringFlag{
					Name:  "atime",
					Usage: "access (read) time",
				},
				cli.StringFlag{
					Name:  "expires",
					Usage: "expiry time (auto-delete after this time)",
				},
			},
			Action: b.cmdSetMd,
		},
		{
			Name:   "ls",
			Usage:  "Lists all blobs.",
			Action: b.cmdList,
		},
		{
			Name:   "tsinfo",
			Usage:  "Displays a summary of all tractservers and disks",
			Action: b.cmdTsInfo,
		},
		{
			Name:  "tsadmin",
			Usage: "Sets control flags on specific disks",
			Description: strings.Replace(`
Does an RPC to tractservers to modify control flags. This has the same effect as ssh-ing
to the tractserver and manipulating the control flag files (but may take effect faster).

Disks are specified in the form: host/disk

"host" can be a plain hostname, or host:port.

"disk" can be a single letter, which expands to TEMPLATE
or a full path to a disk root.
`, "TEMPLATE", shortDiskTemplate, 1),
			ArgsUsage: "[--stopallocating] [--drain=rate] [--drainlocal=rate] host/disk ...",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "stopallocating",
					Usage: "Stop allocating new tracts to this disk",
				},
				cli.IntFlag{
					Name:  "drain",
					Usage: "Drain rate (tracts/second)",
				},
				cli.IntFlag{
					Name:  "drainlocal",
					Usage: "Local drain rate (tracts/second)",
				},
			},
			Action: b.cmdTsAdmin,
		},
		{
			Name:    "read",
			Aliases: []string{"r"},
			Usage:   "Reads from a blob.",
			Flags: []cli.Flag{
				blobflag,
				offsetflag,
				datafileflag,
				lengthflag,
			},
			Action: b.cmdRead,
		},
		{
			Name:    "write",
			Aliases: []string{"w"},
			Usage:   "Writes to a blob.",
			Flags: []cli.Flag{
				blobflag,
				offsetflag,
				datafileflag,
			},
			Action: b.cmdWrite,
		},
		{
			Name:  "mount",
			Usage: "Mounts blb as a filesystem using fuse.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "path, p",
					Usage: "Path to mount on",
				},
			},
			Action: b.cmdMount,
		},
		{
			Name:    "umount",
			Aliases: []string{"unmount"},
			Usage:   "Unmounts a previously-mounted filesystem.",
			Action:  b.cmdUmount,
		},
		{
			Name:   "shell",
			Usage:  "Starts a shell for interaction.",
			Action: b.cmdShell,
		},
		{
			Name:  "start_cluster",
			Usage: "Starts a local Blb cluster and connects to it. The local cluster will be stopped either command 'stop_cluster' is issued or the cli gets exited.",
			Flags: []cli.Flag{
				masterFlag,
				curatorFlag,
				curatorGroupFlag,
				tsFlag,
				diskFlag,
				binFlag,
			},
			Action: b.cmdStartCluster,
		},
		{
			Name:   "stop_cluster",
			Usage:  "Stop current running local cluster.",
			Action: b.cmdStopCluster,
		},

		// Commands that only work if we are connecting to a local cluster.
		{
			Name:   "ps",
			Usage:  "Prints status of running processes(only works when connected to a local cluster).",
			Action: b.cmdPs,
		},
		{
			Name:      "kill",
			Usage:     "Kills the specified process(only works when connected to a local cluster).",
			ArgsUsage: "<service-name>",
			Action:    b.cmdKill,
		},
		{
			Name:      "restart",
			ArgsUsage: "<service-name>",
			Usage:     "Restarts the specified process(only works when connected to a local cluster).",
			Action:    b.cmdRestart,
		},
		{
			Name:   "log",
			Usage:  "Turn on/off logging(only works when connected to a local cluster).",
			Action: b.cmdLog,
		},
		{
			Name:   "logprefix",
			Usage:  "Turn on/off logprefix(only works when connected to a local cluster).",
			Action: b.cmdLogPrefix,
		},
		{
			Name:      "grep",
			Usage:     "Grep log output, it will only be applied to future log messages to terminal(only works when connected to a local cluster).",
			ArgsUsage: "<regexp>",
			Action:    b.cmdGrep,
		},
		{
			Name:      "fget",
			Usage:     "Return current failure configuration of a given service.",
			ArgsUsage: "<service-name>",
			Action:    b.cmdFailureConfigGet,
		},
		{
			Name:  "fset",
			Usage: "Update the current failure configuration of a given service.",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "replace",
					Usage: "Replace the failure configuration in entirety, the missing keys will be reset to values 'null(nil)'",
				},
			},
			ArgsUsage: "<key1> <value1> <key2> <value2> ...",
			Description: `
Update the failure configurations of a service by giving it a list of key-value pairs of failure configurations
that will be updated. If "--replace or --r" is specified then the entire failure configuration will be replaced(the
configurations that are missing in the arguments will be reset to null(nil)).
If no key-values have been specified then the entire failure configuration will be reset in the case of "replace"
or nothing will be updated in the case of "update".`,
			Action: b.cmdFailureConfigSet,
		},
	}
	app.Before = b.beforeSubcommandRun
	b.app = app

	// By default 'HelpName' will be the parent command name('cli' in our case) +
	// command name. Overwrite 'HelpName' to be command name only.
	for i := range b.app.Commands {
		b.app.Commands[i].HelpName = b.app.Commands[i].Name
	}
	return b
}

// run starts a command specified by users.
func (b *blbCli) run(args []string) error {
	return b.app.Run(args)
}

// stop frees up all resource used by the blbCli object.
func (b *blbCli) stop() {
	if b.mountState != nil {
		b.mountState.Unmount()
	}
	b.stopCluster()
}

func (b *blbCli) getCluster(c *cli.Context) (cluster string) {
	if b.cluster != nil {
		// If we have a local cluster, use it.
		cluster = strings.Join(b.cluster.MasterAddrs(), ",")
	} else {
		// User might have specified masters.
		cluster = c.GlobalString("cluster")
	}
	if cluster == "" {
		log.Errorf("No cluster name or master address provided. Use --cluster/-c.")
		os.Exit(1)
	}
	return
}

// getClient returns a client object that will be used to talk to the Blb
// cluster. If there's already a connection, reuse it, otherwise create a new
// one.
func (b *blbCli) getClient(c *cli.Context) *blb.Client {
	cluster := b.getCluster(c)
	// If there's already a client object and it's connecting to the same blb
	// cluster, return it directly.
	if b.clt != nil && b.cltCacheKey == cluster {
		// Reuse the old client if new client will connect to the same a set of
		// masters.
		return b.clt
	}
	options := blb.Options{
		Cluster:             cluster,
		DisableRetry:        false,
		RetryTimeout:        30 * time.Second,
		DisableCache:        false,
		ReconstructBehavior: blb.ReconstructBehavior{Enabled: c.GlobalBoolT("reconstruct")},
	}
	b.clt = blb.NewClient(options)
	b.cltCacheKey = cluster
	return b.clt
}

// This function will be called before any subcommand gets started so some setup
// can be done here.
func (b *blbCli) beforeSubcommandRun(c *cli.Context) error {
	// See if users have some setup commands to run before any subcommand starts.
	commands := c.GlobalStringSlice("setup")
	if len(commands) != 0 {
		log.Infof("Running setup commands...")
		for _, command := range commands {
			log.Infof("Running command %q", command)
			if err := b.runCommand(c, strings.Fields(command)...); err != nil {
				log.Errorf("error: %v", err)
				return err
			}
		}
		log.Infof("Setup is done!")
	}
	return nil
}

// cmdInfo implements the "info" subcommand.
func (b *blbCli) cmdInfo(c *cli.Context) {
	client := b.getClient(c)
	_ = client // TODO: implement something here
}

// cmdCreate implements the "create" subcommand.
func (b *blbCli) cmdCreate(c *cli.Context) {
	client := b.getClient(c)
	repl := c.Int("repl")
	blob, err := client.Create(blb.ReplFactor(repl))
	if err != nil {
		log.Errorf("Couldn't create blob: %s", err)
		return
	}
	log.Infof("New blob id: %s", blob.ID())
}

// cmdStat implements the "stat" subcommand.
func (b *blbCli) cmdStat(c *cli.Context) {
	client := b.getClient(c)
	blobid, err := blb.ParseBlobID(c.String("blob"))
	if err != nil {
		log.Errorf("Failed to parse blobID: %v", err)
		return
	}
	blob, err := client.Open(blobid, "s")
	if err != nil {
		log.Errorf("Couldn't open blob: %v", err)
		return
	}

	info, err := blob.Stat()
	if err != nil {
		log.Errorf("Error statting blob: %v", err)
	}

	byteLen, err := blob.ByteLength()
	if err != nil {
		log.Errorf("Error getting blob length: %v", err)
	}

	log.Infof("blob %s Repl=%d NumTracts=%d ByteLen=%d", blobid, info.Repl, info.NumTracts, byteLen)
	mt := info.MTime.Format(time.RFC3339)
	at := info.ATime.Format(time.RFC3339)
	et := "---"
	if !info.Expires.IsZero() {
		et = info.Expires.Format(time.RFC3339)
	}
	log.Infof("     %16s MTime=%s ATime=%s", "", mt, at)
	log.Infof("     %16s Expires=%s", "", et)
	log.Infof("     %16s StorageHint=%s StorageClass=%s", "", info.Hint, info.Class)

	if c.Bool("verbose") {
		tracts, gtErr := client.GetTracts(context.Background(), blobid, 0, info.NumTracts)
		if gtErr != nil {
			log.Errorf("GetTracts failed: %v", gtErr)
			return
		}

		for i := range tracts {
			log.Infof("Tract: %v", tracts[i])
		}
	}
}

// cmdRm implements the "rm" subcommand.
func (b *blbCli) cmdRm(c *cli.Context) {
	client := b.getClient(c)
	blobid, err := blb.ParseBlobID(c.String("blob"))
	if err != nil {
		log.Errorf("Failed to parse blobID: %v", err)
		return
	}
	blbErr := client.Delete(context.Background(), blobid)
	if blbErr == nil {
		log.Infof("Blob %s deleted", blobid)
	} else {
		log.Errorf("Error: %s", blobid)
	}
}

// cmdUnRm implements the "unrm" subcommand.
func (b *blbCli) cmdUnRm(c *cli.Context) {
	client := b.getClient(c)
	blobid, err := blb.ParseBlobID(c.String("blob"))
	if err != nil {
		log.Errorf("Failed to parse blobID: %v", err)
		return
	}
	blbErr := client.Undelete(context.Background(), blobid)
	if blbErr == nil {
		log.Infof("Blob %s undeleted", blobid)
	} else {
		log.Errorf("Error: %s", blobid)
	}
}

// parses a time as a string, in RFC3339 or UnixDate format.
func parseTime(v string) (t time.Time, e error) {
	if t, e = time.Parse(time.RFC3339, v); e == nil {
		return
	}
	return time.Parse(time.UnixDate, v)
}

// cmdSetMd implements the "setmd" subcommand.
func (b *blbCli) cmdSetMd(c *cli.Context) {
	client := b.getClient(c)
	blobid, err := blb.ParseBlobID(c.String("blob"))
	if err != nil {
		log.Errorf("Failed to parse blobID: %v", err)
		return
	}

	var md core.BlobInfo
	if hintStr := c.String("hint"); hintStr != "" {
		hintStr = strings.ToUpper(hintStr)
		if hint, ok := core.ParseStorageHint(hintStr); ok {
			md.Hint = hint
		} else {
			log.Errorf("Couldn't parse storage hint %q", hintStr)
			return
		}
	}

	if timeStr := c.String("mtime"); timeStr != "" {
		if md.MTime, err = parseTime(timeStr); err != nil {
			log.Errorf("Couldn't parse time %q: %s", timeStr, err)
			return
		}
	}

	if timeStr := c.String("atime"); timeStr != "" {
		if md.ATime, err = parseTime(timeStr); err != nil {
			log.Errorf("Couldn't parse time %q: %s", timeStr, err)
			return
		}
	}

	if timeStr := c.String("expires"); timeStr != "" {
		if md.Expires, err = parseTime(timeStr); err != nil {
			log.Errorf("Couldn't parse time %q: %s", timeStr, err)
			return
		}
	}

	blbErr := client.SetMetadata(context.Background(), blobid, md)
	if blbErr == nil {
		log.Infof("Blob %s updated", blobid)
	} else {
		log.Errorf("Error: %s", blobid)
	}
}

// cmdList implements the "ls" command
func (b *blbCli) cmdList(c *cli.Context) {
	client := b.getClient(c)
	next := client.ListBlobs(context.Background())
	for {
		ids, err := next()
		if err != nil {
			log.Errorf("Error: %s", err)
			break
		}
		if ids == nil {
			break
		}
		for _, id := range ids {
			log.Infof("%s", id)
		}
	}
}

// cmdTsInfo implements the "tsinfo" command
func (b *blbCli) cmdTsInfo(c *cli.Context) {
	const GB = 1000000000
	const TB = GB * 1000

	mc := blb.NewRPCMasterConnection(b.getCluster(c))
	info, err := mc.GetTractserverInfo(context.Background())
	if err != core.NoError {
		log.Errorf("Error: %s", err)
		return
	}
	now := time.Now()
	var totalAvail, totalSpace uint64
	fmt.Printf("  FUSDL  %7s  %5s / %5s  used  %s\n", "tracts", "avail", "totGB", "disk")
	for _, ts := range info {
		ago := now.Sub(ts.LastHeartbeat)
		fmt.Printf("%s (#%d)  last heartbeat %2.0fs ago\n", ts.Addr, ts.ID, ago.Seconds())
		for _, d := range ts.Disks {
			totalAvail += d.AvailSpace
			totalSpace += d.TotalSpace
			fmt.Printf("  %c%c%c%c%c  %7d  %5d / %5d  %3d%%  %s\n",
				boolToChar(d.Status.Full, 'F'),
				boolToChar(!d.Status.Healthy, 'U'),
				boolToChar(d.Status.Flags.StopAllocating, 'S'),
				boolToChar(d.Status.Flags.Drain > 0, 'D'),
				boolToChar(d.Status.Flags.DrainLocal > 0, 'L'),
				d.NumTracts,
				d.AvailSpace/GB,
				d.TotalSpace/GB,
				100*(d.TotalSpace-d.AvailSpace)/d.TotalSpace,
				shortDiskName(d.Status.Root))
		}
	}
	fmt.Printf("cluster total: %d / %dTB avail  %d%% used\n",
		totalAvail/TB, totalSpace/TB, 100*(totalSpace-totalAvail)/totalSpace)
}

func boolToChar(val bool, char rune) rune {
	if val {
		return char
	}
	return '-'
}

// cmdTsAdmin implements the "tsadmin" command
func (b *blbCli) cmdTsAdmin(c *cli.Context) {
	tt := blb.NewRPCTractserverTalker()

	flags := core.DiskControlFlags{
		StopAllocating: c.Bool("stopallocating"),
		Drain:          c.Int("drain"),
		DrainLocal:     c.Int("drainlocal"),
	}

	for _, disk := range c.Args() {
		pair := strings.SplitN(disk, "/", 2)
		if len(pair) != 2 {
			log.Errorf("Invalid disk specifier %q", disk)
			continue
		}
		addr := addPortIfMissing(pair[0], 4002)
		root := fullDiskName(pair[1])
		err := tt.SetControlFlags(context.Background(), addr, root, flags)
		if err == core.NoError {
			log.Infof("Set flags on %s %s to %+v", addr, root, flags)
		} else {
			log.Errorf("Error setting flags on %s %s: %s", addr, root, err)
		}
	}
}

// cmdRead implements the "read" subcommand.
func (b *blbCli) cmdRead(c *cli.Context) {
	client := b.getClient(c)
	blobid, err := blb.ParseBlobID(c.String("blob"))
	if err != nil {
		log.Errorf("Failed to parse blobID: %v", err)
		return
	}
	blob, blbErr := client.Open(blobid, "r")
	if blbErr != nil {
		log.Errorf("Couldn't open blob: %v", err)
		return
	}

	off := c.Int("offset")
	if off != 0 {
		blob.Seek(int64(off), os.SEEK_SET)
	}

	var output io.WriteCloser = os.Stdout

	filename := c.String("file")
	if filename != "" {
		var e error
		output, e = os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if e != nil {
			log.Errorf("Couldn't open output file: %v", e)
			return
		}
	}

	length := c.Int("length")
	if length > 0 {
		// Do a fixed-length read in one call.
		// (Doesn't use io.CopyN because that will use a tiny buffer.)
		p := make([]byte, length)
		n, err := blob.Read(p)
		if err != nil && err != io.EOF {
			log.Errorf("Read error: %v", err)
			return
		}
		output.Write(p[:n])
	} else {
		_, err := io.CopyBuffer(output, blob, make([]byte, c.GlobalInt("buffer")))
		if err != nil && err != io.EOF {
			log.Errorf("Read error: %v", err)
			return
		}
	}
}

// cmdWrite implements the "write" subcommand.
func (b *blbCli) cmdWrite(c *cli.Context) {
	client := b.getClient(c)
	blobid, err := blb.ParseBlobID(c.String("blob"))
	if err != nil {
		log.Errorf("Failed to parse blobID: %v", err)
		return
	}
	blob, blbErr := client.Open(blobid, "w")
	if blbErr != nil {
		log.Errorf("Couldn't open blob: %v", err)
		return
	}

	off := c.Int("offset")
	if off != 0 {
		blob.Seek(int64(off), os.SEEK_SET)
	}

	filename := c.String("file")
	if filename == "" {
		log.Errorf("Input file required.")
		return
	}
	bytes, e := ioutil.ReadFile(filename)
	if e != nil {
		log.Errorf("Couldn't open input file: %v", e)
		return
	}

	n, e := blob.Write(bytes)
	if e != nil {
		log.Errorf("Write error: %v", e)
		return
	} else if n < len(bytes) {
		log.Errorf("Short write: %d < %d", n, len(bytes))
		return
	}
}

// checkMountState cleans up mountState and prints a message if the fuse
// goroutine has exited. It returns true if a fuse goroutine is still running.
func (b *blbCli) checkMountState() bool {
	if b.mountState != nil && b.mountState.Exited() {
		log.Infof("Mount exited: %s", b.mountState)
		b.mountState = nil
	}
	return b.mountState != nil
}

func (b *blbCli) cmdMount(c *cli.Context) {
	if b.checkMountState() {
		log.Infof("Already mounted %s", b.mountState)
		return
	}

	path := c.String("path")
	if path == "" {
		log.Errorf("missing path")
		return
	}
	client := b.getClient(c)
	log.Infof("Mounting on %q...", path)
	b.mountState = fuse.Mount(client, path)

	if !b.inShell {
		for b.checkMountState() {
			time.Sleep(time.Second)
		}
	}
}

func (b *blbCli) cmdUmount(c *cli.Context) {
	if !b.checkMountState() {
		log.Infof("No blb fuse system is mounted.")
		return
	}
	err := b.mountState.Unmount()
	if err != nil {
		log.Errorf("Unmount error: %s", err)
	} else {
		// Wait for the fuse goroutine to exit.
		for b.checkMountState() {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// cmdShell implements "shell" subcommand.
func (b *blbCli) cmdShell(c *cli.Context) {
	b.inShell = true
	defer func() { b.inShell = false }()

	// Make cli not exit on errors.
	cli.OsExiter = func(int) {}

	liner := liner.NewLiner()
	liner.SetCtrlCAborts(true)

	// Add commands auto completion.
	// SetCompleter accepts a function that will be called when users type something
	// in shell. The func takes the currently edited line content at the left of the
	// cursor(stored in 'line') and returns a list of completion candidates.
	liner.SetCompleter(func(line string) (c []string) {
		for _, cmd := range b.app.Commands {
			if strings.HasPrefix(cmd.Name, line) {
				c = append(c, cmd.Name)
			}
		}
		return
	})

	defer liner.Close()

	for {
		input, err := liner.Prompt(fmt.Sprintf("(%s) ", "blb"))
		if err != nil {
			log.Errorf("error: %v", err)
			return
		}

		// We use 'shlex' because we want split input line in to tokens using
		// shell-style rules for quoting and commenting.
		args, err := shlex.Split(input)
		if err != nil {
			log.Errorf("error:%v", err)
			continue
		}

		// Skip empty line.
		if 0 == len(args) {
			continue
		}

		if args[0] == "exit" {
			return
		}

		if b.runCommand(c, args...) == nil {
			// Adds succeeded command to command history.
			liner.AppendHistory(input)
		}
	}
}

// Below are the commands that only work when connected to a locally started
// cluster

// cmdStartCluster implements "start_cluster" subcommand.
func (b *blbCli) cmdStartCluster(c *cli.Context) {
	if b.cluster != nil {
		log.Errorf("There's already a local cluster running, must stop it first")
		return
	}

	config := &cluster.Config{
		LogConfig:           cluster.DefaultLogConfig(),
		Masters:             uint(c.Int("masters")),
		Curators:            uint(c.Int("curators")),
		CuratorGroups:       uint(c.Int("curator_groups")),
		Tractservers:        uint(c.Int("tractservers")),
		DisksPerTractserver: uint(c.Int("disks_per_tractserver")),
		BinDir:              c.String("bin_dir"),
	}

	var err error
	if config.BinDir == "" {
		config.BinDir = test.FindBinDir()
	}

	// Verify we have all required binaries.
	bins := []string{"master", "curator", "tractserver"}
	if err = test.CheckBinaries(config.BinDir, bins); err != nil {
		panic(err)
	}

	// Create root dir.
	config.RootDir, err = ioutil.TempDir(test.TempDir(), "blbcli")
	if nil != err {
		log.Fatalf("failed to create root dir: %s", err)
	}
	log.Infof("Root directory of the cluster: %s", config.RootDir)

	// Log everyting to log file and terminal.
	logFile := filepath.Join(config.RootDir, "log.txt")
	fLogger, err := cluster.NewFileLogger(logFile)
	if err != nil {
		log.Fatalf("failed to create log file %s: %s", logFile, err)
	}

	log.Infof("Log file path: %s", logFile)
	loggers := []cluster.Logger{cluster.NewTerminalLogger(config.LogConfig), fLogger}

	b.cluster = cluster.NewCluster(config, loggers)
	b.cluster.Setup()
	b.cluster.Start()
	log.Infof("Local cluster is started!")
}

// cmdStopCluster implements "stop_cluster" subcommand.
func (b *blbCli) cmdStopCluster(c *cli.Context) {
	b.stopCluster()
}

// cmdPs implements "ps" subcommand.
func (b *blbCli) cmdPs(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'ps' only works when connected to a local cluster.")
		return
	}
	for _, proc := range b.cluster.AllProcs() {
		if proc.Running() {
			log.Infof("%s is running as pid: %d", proc, proc.Pid())
		} else {
			log.Infof("%s exited", proc)
		}
	}
}

// cmdKill implements "kill" subcommand.
func (b *blbCli) cmdKill(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'kill' only works when connected to a local cluster.")
		return
	}
	name := c.Args().First()
	proc := b.cluster.FindProc(name)
	if proc == nil {
		log.Errorf("error: Can't find process %s", name)
		return
	}
	proc.Stop()
}

// cmdRestart implements "restart" subcommand.
func (b *blbCli) cmdRestart(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'restart' only works when connected to a local cluster.")
		return
	}
	name := c.Args().First()
	proc := b.cluster.FindProc(name)
	if proc == nil {
		log.Errorf("error: Can't find process %s", name)
		return
	}
	proc.Stop()
	proc.Start()
}

// cmdLog implements "log" subcommand.
func (b *blbCli) cmdLog(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'log' only works when connected to a local cluster.")
		return
	}
	b.cluster.GetConfig().SetTermLogOn(!b.cluster.GetConfig().GetTermLogOn())
}

// cmdLogPrefix implements "logprefix" subcommand.
func (b *blbCli) cmdLogPrefix(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'logprefix' only works when connected to a local cluster.")
		return
	}
	b.cluster.GetConfig().SetLogPrefix(!b.cluster.GetConfig().GetLogPrefix())
}

// cmdGrep implements "grep" subcommand.
func (b *blbCli) cmdGrep(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'grep' only works when connected to a local cluster.")
		return
	}
	b.cluster.GetConfig().SetPattern(c.Args().First())
}

// cmdFailureConfigGet implements "fget" subcommand.
func (b *blbCli) cmdFailureConfigGet(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'fget' only works when connected to a local cluster.")
		return
	}

	if len(c.Args()) != 1 {
		// Display help message of the command if the number of arguments is wrong.
		b.app.Run([]string{"cli", c.Command.Name, "-h"})
		return
	}

	// The only argument should be the name of service.
	name := c.Args().Get(0)
	proc := b.cluster.FindProc(name)
	if proc == nil {
		log.Errorf("error: Can't find process %s", name)
		return
	}

	config, err := proc.GetFailureConfig()
	if err != nil {
		log.Errorf("Failed to get failure config: %v", err)
		return
	}

	// We have to encode the response back to JSON format so we can print it
	// to users.
	data, err := json.Marshal(config)
	if err != nil {
		log.Errorf("Failed to encode response to JSON: %v", err)
		return
	}
	log.Infof("%s", string(data))
}

// cmdFailureConfigSet implements "fset" subcommand.
func (b *blbCli) cmdFailureConfigSet(c *cli.Context) {
	if b.cluster == nil {
		log.Errorf("command 'fset' only works when connected to a local cluster.")
		return
	}

	// The first argument must be the name of the service.
	name := c.Args().Get(0)
	// The rest are the key-value pairs.
	kvs := c.Args().Tail()

	// Users must specify at least the host name of the failure configuration.
	// Also if the users have specified key-value pairs they must be paired.
	if len(c.Args()) < 1 || len(kvs)%2 != 0 {
		// Display help message of the command if the number of arguments is wrong.
		b.app.Run([]string{"cli", c.Command.Name, "-h"})
		return
	}

	config := make(map[string]interface{})
	for i := 0; i < len(kvs); i += 2 {
		key, value := kvs[i], json.RawMessage(kvs[i+1])
		config[key] = &value
	}

	proc := b.cluster.FindProc(name)
	if proc == nil {
		log.Errorf("error: Can't find process %s", name)
		return
	}

	var err error
	var method string
	if c.Bool("replace") {
		// If flag "replace" is specified users want to replace the entire failure
		// configuration.
		err = proc.SetFailureConfig(config)
		method = "replace"
	} else {
		// Otherwise only update the current configuration.
		err = proc.UpdateFailureConfig(config)
		method = "update"
	}
	if err != nil {
		log.Errorf("Failed to %s the failure config: %v", method, err)
		return
	}
	log.Infof("Successfully %sd the failure config", method)
}

// Stop local cluster that is connected to, if there's one.
func (b *blbCli) stopCluster() {
	if b.cluster != nil {
		b.cluster.Stop()
		// Cleanup temporary directory created by this cluster.
		os.RemoveAll(b.cluster.GetConfig().RootDir)
		b.cluster = nil
	}
}

// runCommand runs a command after the cli gets started already(either from command
// interpreter or setup flags).
func (b *blbCli) runCommand(c *cli.Context, args ...string) error {
	blbArgs := []string{"cli", "--master", c.GlobalString("master")}
	blbArgs = append(blbArgs, args...)
	return b.run(blbArgs)
}

// shortDiskName turns a disk name matching shortDiskTemplate into the disk
// name. If root doesn't look like the template, it just returns root.
func shortDiskName(root string) (disk string) {
	parts := strings.Split(root, "/")
	tParts := strings.Split(shortDiskTemplate, "/")
	if len(parts) != len(tParts) {
		return root
	}
	for i, tPart := range tParts {
		if tPart == "X" {
			disk = parts[i]
		} else if parts[i] != tPart {
			return root
		}
	}
	return
}

// fullDiskName is the reverse of shortDiskName: when given a string with no
// slashes, it returns shortDiskTemplate with the disk name substituted in,
// otherwise it returns the string it was given.
func fullDiskName(root string) string {
	if !strings.Contains(root, "/") {
		return strings.Replace(shortDiskTemplate, "X", root, 1)
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return root
}

// addPortIfMissing adds a port number to the given address if it has none (i.e. if it
// has no ':'), otherwise returns it as-is.
func addPortIfMissing(addr string, port int) string {
	parts := strings.Split(addr, ":")
	if len(parts) == 1 {
		return fmt.Sprintf("%s:%d", addr, port)
	} else if len(parts) == 2 {
		return addr
	}
	log.Errorf("Invalid address %q", addr)
	return ""
}
