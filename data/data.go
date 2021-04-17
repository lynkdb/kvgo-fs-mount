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

package data

import (
	"fmt"
	"os"

	"github.com/hooto/hlog4g/hlog"

	"github.com/lynkdb/kvgo-fs-mount/config"
	"github.com/lynkdb/kvgo-fs-mount/kvfs"
	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

var (
	err      error
	fsMounts []*kvfs.KvFsMountService
	clients  []kv2.Client
	tables   []kv2.ClientTable
)

func Setup() error {

	for _, v := range config.Config.Mounts {

		c, err := v.DataServer.NewClient()
		if err != nil {
			return err
		}
		clients = append(clients, c)

		tbl := c.OpenTable(v.TableName)

		if rs := tbl.NewWriter([]byte("ping-test"), 1).Commit(); !rs.OK() {
			return fmt.Errorf("kvgo connect to %s, fail %s", v.DataServer.Addr, rs.Message)
		}
		hlog.Printf("info", "kvgo connect to %s ok", v.DataServer.Addr)

		mountPoint := fmt.Sprintf("%s/%s", config.Config.Common.BaseDirectory, v.TableName)

		err = os.MkdirAll(mountPoint, 0755)
		if err != nil {
			break
		}

		fss, e := kvfs.Setup(&kvfs.Config{
			MountPoint: mountPoint,
		}, tbl)
		if e != nil {
			err = e
			break
		}

		hlog.Printf("info", "setup mount fs %s ok", mountPoint)

		tables = append(tables, tbl)
		fsMounts = append(fsMounts, fss)
	}

	if err != nil {
		for _, v := range fsMounts {
			v.Close()
		}
		for _, v := range clients {
			v.Close()
		}
	}

	return nil
}

func Close() error {
	for _, v := range clients {
		v.Close()
	}
	return nil
}
