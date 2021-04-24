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

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

type kvFsFile struct {
	mu   sync.Mutex
	fs   *KvFsMountService
	node *fsNodeItem

	rChkNum uint32
	rOffset int64
	rBuf    []byte

	wChkNum    uint32
	wOffset    int64
	wBuf       []byte
	wNum       int
	wMetaFlush bool
}

var _ fs.Node = &kvFsFile{}

func (it *kvFsFile) Attr(ctx context.Context, a *fuse.Attr) error {

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.node != nil {

		a.Mode = os.FileMode(it.node.fsMeta.Mode)

		a.Size = uint64(it.node.fsMeta.Size)

		a.Ctime = nsTime(it.node.fsMeta.Ctime)
		a.Mtime = nsTime(it.node.fsMeta.Mtime)
		a.Atime = nsTime(it.node.fsMeta.Atime)

		a.Blocks = a.Size / 512
		if n := a.Size % 512; n > 0 {
			a.Blocks++
		}

	} else {
		a.Mode = 0644
	}

	a.Uid = osUserUid
	a.Gid = osUserGid

	return nil
}

var _ fs.NodeSetattrer = &kvFsFile{}

func (it *kvFsFile) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.node == nil {
		return fuse.ENOENT
	}

	flush := false

	if req.Valid.Mode() {
		if it.node.fsMeta.Mode != uint32(req.Mode) {
			it.node.fsMeta.Mode, flush = uint32(req.Mode), true
		}
	}

	if req.Valid.Mtime() {
		if it.node.fsMeta.Mtime != req.Mtime.UnixNano() {
			it.node.fsMeta.Mtime, flush = req.Mtime.UnixNano(), true
		}
	}

	if req.Valid.Atime() {
		if it.node.fsMeta.Atime != req.Atime.UnixNano() {
			it.node.fsMeta.Atime, flush = req.Atime.UnixNano(), true
		}
	}

	if req.Valid.Size() {

		if int64(req.Size) > kvFsSizeMax {
			return fuse.Errno(syscall.EFBIG)
		}

		if it.node.fsMeta.Size != int64(req.Size) {
			it.node.fsMeta.Size, flush = int64(req.Size), true
		}
	}

	if flush {

		if it.node.fsMeta.Ctime == 0 {
			if it.node.fsMeta.Mtime > 0 {
				it.node.fsMeta.Ctime = it.node.fsMeta.Mtime
			} else {
				it.node.fsMeta.Ctime = time.Now().UnixNano()
			}
		}

		return it.fs.cache.flush(it.node)
	}

	return nil
}

var _ fs.NodeOpener = &kvFsFile{}

func (it *kvFsFile) Open(ctx context.Context,
	req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	it.mu.Lock()
	defer it.mu.Unlock()

	if req.Flags.IsReadOnly() {

		if it.node != nil &&
			kv2.AttrAllow(it.node.dbMeta.Attrs, ExtMetaAttrFoldMeta) {
			return &kvFsDir{
				fs:   it.fs,
				path: it.node.fsMeta.Path,
			}, nil
		}

		return it, nil
	}

	it.wNum++
	return it, nil
}

var _ fs.HandleReader = &kvFsFile{}

func (it *kvFsFile) Read(ctx context.Context,
	req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.node == nil {
		return fuse.ENOENT
	}

	if it.wNum > 0 {
		return nil
	}

	var (
		reqOffset = req.Offset
		reqSize   = req.Size
	)

	resp.Data = []byte{} // make([]byte, req.Size)

	for len(resp.Data) < req.Size && reqOffset < it.node.fsMeta.Size {

		blkNum := uint32(reqOffset / kvFsChunkSize)

		if blkNum != it.rChkNum || len(it.rBuf) < 1 {

			fsDataKey := pathDataChunkKey(it.node.dbMeta.IncrId, blkNum)

			r := it.fs.tbl.NewReader(fsDataKey)
			rs := r.Query()
			if !rs.OK() {
				return rs.Error()
			}

			var fsData KvFsChunk
			if err := rs.DataValue().Decode(&fsData, kvFsDataCodec); err != nil {
				return err
			}

			it.rBuf = fsData.Data
			it.rChkNum = blkNum
		}

		var (
			bOffset = int(reqOffset % kvFsChunkSize)
			bCutset = int(kvFsChunkSize) - bOffset
		)

		if bCutset > reqSize {
			bCutset = reqSize
		}

		if (bOffset + bCutset) > len(it.rBuf) {
			bCutset = len(it.rBuf) - bOffset
		}

		if bCutset > 0 {
			resp.Data = append(resp.Data, bytesClone(it.rBuf[bOffset:bOffset+bCutset])...)

			reqOffset += int64(bCutset)
			reqSize -= bCutset
		}

		if len(resp.Data) >= req.Size {
			break
		}

		if len(it.rBuf) < int(kvFsChunkSize) {
			break
		}

		if uint32(it.node.fsMeta.Size/kvFsChunkSize) >= it.rChkNum {
			break
		}
	}

	return nil
}

var _ fs.HandleWriter = &kvFsFile{}

func (it *kvFsFile) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.node == nil {
		return fuse.ENOENT
	}

	if req.Offset != it.wOffset {
		return fuse.Errno(syscall.EIO)
	}

	if it.node.dbMeta == nil || it.node.dbMeta.IncrId < 1 {
		return fuse.Errno(syscall.EIO)
	}

	var (
		bufOffset = req.Offset - (kvFsChunkSize * int64(it.wChkNum))
		bufCutset = req.Offset + int64(len(req.Data))
	)
	if bufCutset > kvFsSizeMax {
		return fuse.Errno(syscall.EFBIG)
	}

	bufSize := int(bufCutset - (kvFsChunkSize * int64(it.wChkNum)))
	if len(it.wBuf) < bufSize {
		it.wBuf = append(it.wBuf, make([]byte, bufSize-len(it.wBuf))...)
	}

	n := copy(it.wBuf[bufOffset:], req.Data)
	resp.Size = n
	it.wOffset += int64(n)

	if len(it.wBuf) < int(kvFsChunkSize) {
		return nil
	}

	for len(it.wBuf) >= int(kvFsChunkSize) {

		cutset := int(kvFsChunkSize)
		if cutset > len(it.wBuf) {
			cutset = len(it.wBuf)
		}

		data := it.wBuf[:cutset]

		batch := it.fs.tbl.NewBatch()

		// fs/data:block
		w := batch.Put(pathDataChunkKey(it.node.dbMeta.IncrId, it.wChkNum), KvFsChunk{
			Num:  it.wChkNum,
			Path: it.node.fsMeta.Path,
			Data: data,
		}, kvFsDataCodec)

		w.Meta.Attrs |= kv2.ObjectMetaAttrMetaOff

		var rs *kv2.BatchResult
		for t := 0; t < 10; t++ {
			rs = batch.Commit()
			if rs.OK() {
				break
			}
			time.Sleep(1e9)
		}
		if !rs.OK() {
			return errors.New(rs.Message)
		}

		it.wChkNum += 1
		it.wBuf = it.wBuf[cutset:]

		it.wMetaFlush = true
	}

	return nil
}

var _ = fs.HandleFlusher(&kvFsFile{})

func (it *kvFsFile) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	it.mu.Lock()
	defer it.mu.Unlock()

	if it.node == nil {
		return fuse.ENOENT
	}

	if it.wNum == 0 {
		return nil
	}

	if len(it.wBuf) < 1 && !it.wMetaFlush {
		return nil
	}

	if it.node.dbMeta.IncrId < 1 {
		return fuse.Errno(syscall.EIO)
	}

	batch := it.fs.tbl.NewBatch()

	// fs/data:block
	if len(it.wBuf) > 0 {
		w := batch.Put(pathDataChunkKey(it.node.dbMeta.IncrId, it.wChkNum), KvFsChunk{
			Num:  it.wChkNum,
			Path: it.node.fsMeta.Path,
			Data: it.wBuf,
		}, kvFsDataCodec)
		w.Meta.Attrs |= kv2.ObjectMetaAttrMetaOff
	}

	// fs/meta

	it.node.fsMeta.Size = it.wOffset

	w := batch.Put(it.node.fsMeta.metaKey(), it.node.fsMeta, kvFsDataCodec).
		IncrNamespaceSet(fsInodeNamespace)
	w.Meta.IncrId = it.node.dbMeta.IncrId
	w.Meta.Attrs |= ExtMetaAttrFileMeta
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	rs := batch.Commit()
	if !rs.OK() {
		return errors.New(rs.Message)
	}

	it.fs.cache.set(w.Meta, it.node.fsMeta)

	it.wMetaFlush = false

	return nil
}

var _ = fs.HandleReleaser(&kvFsFile{})

func (it *kvFsFile) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	if req.Flags.IsReadOnly() {
		return nil
	}

	it.mu.Lock()
	defer it.mu.Unlock()

	it.wNum--
	if it.wNum == 0 {
		it.wChkNum = 0
		it.wOffset = 0
		it.wBuf = nil
	}

	return nil
}
