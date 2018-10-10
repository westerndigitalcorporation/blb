// Copyright (c) 2016 Western Digital Corporation or its affiliates. All Rights Reserved
// SPDX-License-Identifier: MIT

package failimpl

import (
	"fmt"
	"io/ioutil"
	"os/exec"

	log "github.com/golang/glog"
)

const (
	// The special environment variable that is used to identify the chaos
	// processes that were created by evilblb.
	evilEnv = "_proc_tag=evilblb"
)

// Failer provides some recipes of chaos that you might want to inject to
// remote machines to test the robustness of your system. These recipes will
// be injected via ssh and some will be run under sudo mode so you must ensure
// you have privileges to ssh to these machines and run sudo commands.
type Failer struct {
	user string
}

// NewFailer creates a new Failer object that you can use to inject
// chaos. 'user' will be the user name that will be used for ssh. You
// must ensure that you have the right to ssh via this user and this
// user has sudo privileges.
//
// TODO: some tasks have lasting effects(e.g. blocking traffic to certain
// hosts, burning CPU/disk, etc). Find a good way to reset the cluster into
// a sane state.
//
// For network-related chaos we'll keep ssh port unaffected so we can
// ssh to hosts and cleanup.
func NewFailer(user string) *Failer {
	return &Failer{user: user}
}

// CorruptFile writes some randomly generated junks to a file on a remote
// machine. It will only corrupt existing files, no files will be created.
func (f *Failer) CorruptFile(host, path string) error {
	// Generate 64 bytes of junks starting from a random position of first 10 bytes.
	script := "sudo dd if=/dev/urandom of=%s bs=64 seek=$((RANDOM%%10)) count=1 conv=notrunc,nocreat"
	log.V(1).Infof("Corrupting file %s on host %s", path, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, path))
}

// CorruptRandomFiles corrupts "num" random files under directory 'dir' with
// names that match the wild cards 'pattern'. If the number of matched files
// is less than 'num', then only matched files will be corrupted.
func (f *Failer) CorruptRandomFiles(host, dir, pattern string, num int) error {
	script := `
	for f in $(ls %s/%s 2>/dev/null | sort -R | head -n %d); do
		sudo dd if=/dev/urandom of=$f bs=64 seek=$((RANDOM%%10)) count=1 conv=notrunc,nocreat
	done
	`
	log.V(1).Infof("Corrupting %d random file(%s) under %s on host %s", num, pattern, dir, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, dir, pattern, num))
}

// KillTask kills a task on a remote machine.
func (f *Failer) KillTask(host, task string) error {
	script := "sudo pkill -x %s"
	log.V(1).Infof("Killing task %s on host %s", task, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, task))
}

// Reboot reboots a remote machine.
func (f *Failer) Reboot(host string) error {
	script := "sudo shutdown -r"
	log.V(1).Infof("Rebooting host %s", host)
	return f.runWithEvilTag(host, script)
}

// DropOutPacketsWithProb enforces a probability of 'prob' of dropping an
// outging packet
func (f *Failer) DropOutPacketsWithProb(host string, prob float32) error {
	if prob < 0.0 || prob > 1.0 {
		return fmt.Errorf("Drop probability must be in range [0.0, 1.0]")
	}
	log.V(1).Infof("Drop outgoing packets with probabilities of %f on host %s", prob, host)
	// The first rule is ensuring SSH traffic is unaffected.
	script := `
	sudo iptables -I OUTPUT 1 -p tcp --sport 22 -j ACCEPT
	sudo iptables -I OUTPUT -m statistic --mode random --probability %f -j DROP
	`
	return f.runWithEvilTag(host, fmt.Sprintf(script, prob))
}

// DropInPacketsWithProb enforces a probability of 'prob' to drop an
// incoming packet.
func (f *Failer) DropInPacketsWithProb(host string, prob float32) error {
	if prob < 0.0 || prob > 1.0 {
		return fmt.Errorf("Drop probability must be in range [0.0, 1.0]")
	}
	log.V(1).Infof("Drop incoming packets with probabilities of %f on host %s", prob, host)
	// The first rule is ensuring SSH traffic is unaffected.
	script := `
	sudo iptables -I INPUT 1 -p tcp --dport 22 -j ACCEPT
	sudo iptables -I INPUT 2 -m statistic --mode random --probability %f -j DROP
	`
	return f.runWithEvilTag(host, fmt.Sprintf(script, prob))
}

// BurnIO executes some disk IO intensive tasks on a remote machine, reducing
// I/O capacity.
func (f *Failer) BurnIO(host string) error {
	script := `
cat << EOF > /tmp/loopburnio.sh
#!/bin/sh
while true; do
    dd if=/dev/urandom of=\$1/burnio bs=1M count=1024 iflag=fullblock
done
EOF
for disk in /disk/*; do
  nohup /bin/sh /tmp/loopburnio.sh $disk &
done
	`
	log.V(1).Infof("Burning IO on host %s", host)
	return f.runWithEvilTag(host, fmt.Sprintf(script))
}

// BurnCPU executes some CPU intensive tasks on a remote machine, reducing CPU
// capacity. The length of the tasks will be specified in 'secs'.
func (f *Failer) BurnCPU(host string, secs int) error {
	script := `
cat << EOF > /tmp/infiniteburn.sh
#!/bin/sh
while true; do
  openssl speed
done
EOF

# 8 parallel 100 percent CPU tasks
for i in {1..8}; do
  nohup timeout %d /bin/sh /tmp/infiniteburn.sh &
done
	`
	log.V(1).Infof("Burning CPU on host %s", host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, secs))
}

// BlockInPacketsOnPort blocks all incoming packets on 'port'.
func (f *Failer) BlockInPacketsOnPort(host string, port int) error {
	// The first rule is ensuring the SSH traffic is unaffected.
	if port == 22 {
		return fmt.Errorf("Can't block port 22")
	}
	script := `
	sudo iptables -I INPUT 1 -p tcp --dport 22 -j ACCEPT
	sudo iptables -I INPUT 2 -p tcp -m tcp --dport %d -j DROP
	sudo iptables -I INPUT 3 -p udp -m udp --dport %d -j DROP
	`
	log.V(1).Infof("Blocking port number %d on host %s", port, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, port, port))
}

// BlockInPacketsOnPortFromHost blocks all TCP traffics that have
// source "from" and to port "port". This rule is applied to "host"
func (f *Failer) BlockInPacketsOnPortFromHost(host, from string, port int) error {
	if port == 22 {
		return fmt.Errorf("Can't block port 22")
	}
	script := `sudo iptables -I INPUT -p tcp --dport %d -s %s -j DROP`
	log.V(1).Infof("Blocking port number %d from host %s, applied to %s", port, from, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, port, from))
}

// BlockOutPacketsOnPortToHost blocks all TCP traffics that have
// desitionation "to" and port "port". This rule is applied to "host".
func (f *Failer) BlockOutPacketsOnPortToHost(host, to string, port int) error {
	if port == 22 {
		return fmt.Errorf("Can't block port 22")
	}
	script := `sudo iptables -I OUTPUT -p tcp --dport %d -d %s -j DROP`
	log.V(1).Infof("Blocking port number %d to host %s, applied to %s", port, to, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, port, to))
}

// BlockOutPacketsOnPort blocks all outgoing packets on 'port'.
func (f *Failer) BlockOutPacketsOnPort(host string, port int) error {
	// The first rule is ensuring the SSH traffic is unaffected.
	if port == 22 {
		return fmt.Errorf("Can't block port 22")
	}
	script := `
	sudo iptables -I OUTPUT 1 -p tcp --sport 22 -j ACCEPT
	sudo iptables -I OUTPUT -p tcp -m tcp --dport %d -j DROP
	sudo iptables -I OUTPUT -p udp -m udp --dport %d -j DROP
	`
	log.V(1).Infof("Blocking port number %d on host %s", port, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, port, port))
}

// BlockHostTo blocks all traffic to host 'dest'.
func (f *Failer) BlockHostTo(host string, dest string) error {
	// The first rule is ensuring the SSH traffic is unaffected.
	script := `
	sudo iptables -I OUTPUT 1 -p tcp --sport 22 -j ACCEPT
	sudo iptables -A OUTPUT -d %s -j DROP
	`
	log.V(1).Infof("Blocking traffic to %s on %s", dest, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, dest))
}

// BlockHostFrom blocks all traffic from host 'dest'.
func (f *Failer) BlockHostFrom(host string, dest string) error {
	// The first rule is ensuring the SSH traffic is unaffected.
	script := `
	sudo iptables -I INPUT 1 -p tcp --dport 22 -j ACCEPT
	sudo iptables -I INPUT 2 -s %s -j DROP
	`
	log.V(1).Infof("Blocking traffic from %s on %s", dest, host)
	return f.runWithEvilTag(host, fmt.Sprintf(script, dest))
}

// BlockDNS blocks all traffics to DNS servers.
func (f *Failer) BlockDNS(host string) error {
	script := `
	sudo iptables -I OUTPUT -p udp --dport 53 -j DROP
	sudo iptables -I INPUT -p udp --sport 53 -j DROP
	`
	log.V(1).Infof("Blocking DNS on %s", host)
	return f.runWithEvilTag(host, script)
}

// Heal will try to fix all the chaos caused on 'host'.
func (f *Failer) Heal(host string) error {
	// This script does:
	// 1. Reset iptable rules.
	// 2. Kill all processes with the evil tag.
	script := `
sudo iptables -F

for pid in $(ls /proc | egrep '^[0-9]+$'); do
  if [ -f /proc/$pid/environ ]; then
    if [[ $(sudo cat /proc/$pid/environ | grep %s) ]]; then
      sudo kill -SIGKILL $pid
    fi
  fi
done
	`
	log.V(1).Infof("Healing host %s", host)
	return f.runScript(host, fmt.Sprintf(script, evilEnv), "")
}

// Runs a shell script on a remote machine, the shell script and all
// processes spawned under it will be tagged with the evil tag. All
// processes with the evil tag will be killed once 'Heal' is called.
func (f *Failer) runWithEvilTag(host, script string) error {
	return f.runScript(host, script, evilEnv)
}

// Runs shell scripts on a remote machine. This function will return without
// waiting the command to be finished.
func (f *Failer) runScript(host, script, env string) error {
	options := []string{
		"-oStrictHostKeyChecking=no",
		fmt.Sprintf("%s@%s", f.user, host),
		fmt.Sprintf("%s bash -s", env),
	}
	cmd := exec.Command("ssh", options...)
	log.Infof("running ssh %+v", options)

	// Not interested in its output
	cmd.Stdout = ioutil.Discard
	cmd.Stderr = ioutil.Discard

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("Failed to get stdin pipe")
	}

	if err := cmd.Start(); err != nil {
		log.Errorf("Failed to start the command: %v", err)
		cmd.Process.Kill()
		return err
	}

	go cmd.Wait()
	// Send the scripts for execution.
	_, err = stdinPipe.Write([]byte(script))
	if err != nil {
		stdinPipe.Close()
		cmd.Process.Kill()
		return err
	}
	stdinPipe.Close()
	return nil
}
