// Copyright 2020 Eryx <evorui аt gmail dοt com>, All rights reserved.
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

package kvfs

//go:generate protoc --proto_path=./ --go_out=./ --go_opt=paths=source_relative kvfs.proto

import (
	"os/user"
	"strconv"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/hooto/hlog4g/hlog"
	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

const (
	KiB = int64(1024)
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
	PiB = 1024 * TiB
)

const (
	kvFsSizeMax   = 64 * GiB
	kvFsChunkSize = 1 * MiB
	kvFsBlockSize = 8 * kvFsChunkSize
)

const (
	ExtMetaAttrFileMeta uint64 = 1 << 36
	ExtMetaAttrFoldMeta uint64 = 1 << 37
	FileMeta            byte   = 0x01
	FileData            byte   = 0x02
	FileRemove          byte   = 0x08
	fsInodeNamespace    string = "inode"
)

type Config struct {
	MountPoint string `toml:"mount_point"`
}

var (
	osUserUid uint32 = 0
	osUserGid uint32 = 0
)

func init() {

	if c, err := user.Current(); err == nil {
		if v, err := strconv.ParseUint(c.Uid, 10, 32); err == nil {
			osUserUid = uint32(v)
		}
		if v, err := strconv.ParseUint(c.Gid, 10, 32); err == nil {
			osUserGid = uint32(v)
		}
	}
}

type KvFsMountService struct {
	mu     sync.Mutex
	tbl    kv2.ClientTable
	fsConn *fuse.Conn
	cache  *fsNodeList
}

var _ = fs.FS(&KvFsMountService{})

func (it *KvFsMountService) Root() (fs.Node, error) {
	return &kvFsDir{fs: it}, nil
}

func (it *KvFsMountService) Close() error {
	if it.fsConn == nil {
		return nil
	}
	go it.fsConn.Close() // TODO
	time.Sleep(1e9)
	return nil
}

func Setup(cfg *Config, tbl kv2.ClientTable) (*KvFsMountService, error) {

	c, err := fuse.Mount(
		cfg.MountPoint,
		fuse.FSName("kvfs"),
	)
	if err != nil {
		return nil, err
	}

	/**
	key := []byte{FileMeta}
	if rs := db.NewReader(nil).
		KeyRangeSet(key, append(key, []byte{0xff}...)).
		AttrSet(kv2.ObjectMetaAttrDataOff).
		LimitNumSet(10000).Query(); rs.OK() {

		for _, v := range rs.Items {
			var fsMeta KvFsMeta
			if err := v.DataValue().Decode(&fsMeta, nil); err != nil {
				continue
			}
			fmt.Println("db meta", v.Meta.Key, len(v.Meta.Key), fsMeta.Path, "isdir",
				kv2.AttrAllow(v.Meta.Attrs, ExtMetaAttrFoldMeta),
				"isfile",
				kv2.AttrAllow(v.Meta.Attrs, ExtMetaAttrFileMeta),
			)
		}
	}
	*/

	fss := &KvFsMountService{
		tbl:    tbl,
		fsConn: c,
		cache:  newFsNodeList(tbl),
	}

	go func() {
		if err := fs.Serve(c, fss); err != nil {
			hlog.Printf("info", "kvfs setup err %s", err.Error())
			fss.Close()
		}
	}()

	/**
	go func() {
		// check if the mount process has an error to report
		<-c.Ready

		if err := c.MountError; err != nil {
			fmt.Println("mount error", err)
			return
		}
	}()
	*/

	return fss, nil
}
