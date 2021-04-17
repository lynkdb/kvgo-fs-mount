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

package config

import (
	"os"
	"path/filepath"

	"github.com/hooto/hauth/go/hauth/v1"
	"github.com/hooto/htoml4g/htoml"
	// "github.com/lessos/lessgo/crypto/idhash"

	"github.com/lynkdb/kvgo"
)

type ConfigMount struct {
	TableName      string            `toml:"table_name"`
	DataServer     kvgo.ClientConfig `toml:"data_server"`
	MountDirectory string            `toml:"mount_directory,omitempty"`
}

type ConfigCommon struct {
	Common struct {
		BaseDirectory string `toml:"base_directory,omitempty"`
	} `toml:"common"`
	Mounts []*ConfigMount `toml:"mounts"`
}

var (
	version  = "0.1.0"
	release  = "1"
	AppName  = "kvgo-fs-mount"
	Prefix   = ""
	err      error
	confFile = ""
	Config   ConfigCommon
)

func Setup(ver, rel string) error {

	version = ver
	release = rel

	if Prefix, err = filepath.Abs(filepath.Dir(os.Args[0]) + "/.."); err != nil {
		Prefix = "/opt/lynkdb/" + AppName
	}

	confFile = Prefix + "/etc/kvgo-fs-mount.conf"

	err = htoml.DecodeFromFile(&Config, confFile)

	if err != nil {
		return err
	}

	if len(Config.Mounts) == 0 {

		Config.Mounts = append(Config.Mounts, &ConfigMount{
			TableName: "main",
			DataServer: kvgo.ClientConfig{
				Addr: "127.0.0.1:9100",
				AccessKey: &hauth.AccessKey{
					Id:     "access-key-id",
					Secret: "access-key-secret",
				},
			},
		})

		return Flush()
		// return fmt.Errorf("no mounts setting")
	}

	return nil
}

func Flush() error {
	return htoml.EncodeToFile(Config, confFile, nil)
}
