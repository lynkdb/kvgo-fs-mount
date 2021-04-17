// Copyright 2020 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo-fs-mount/config"
	"github.com/lynkdb/kvgo-fs-mount/data"
)

var (
	version = "git"
	release = "1"
)

func main() {

	if err := config.Setup(version, release); err != nil {
		hlog.Printf("warn", "kvgo-fs-mount/config/setup err %s", err.Error())
		hlog.Flush()
		os.Exit(1)
	}

	if err := data.Setup(); err != nil {
		hlog.Printf("warn", "kvgo-fs-mount/data/setup err %s", err.Error())
		hlog.Flush()
		os.Exit(1)
	}

	quit := make(chan os.Signal, 2)

	//
	signal.Notify(quit,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGKILL)
	sg := <-quit

	data.Close()
	time.Sleep(1e9)

	hlog.Printf("warn", "kvgo-fs-mount signal quit %s", sg.String())
	hlog.Flush()
}
