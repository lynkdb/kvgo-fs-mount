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

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

type kvFsDir struct {
	mu   sync.Mutex
	fs   *KvFsMountService
	path string
	node *fsNodeItem
	err  error
}

func (it *kvFsDir) metaRefresh() error {

	if it.node == nil {
		it.node, it.err = it.fs.cache.get(it.path)
	}

	return it.err
}

var _ fs.Node = &kvFsDir{}

func (it *kvFsDir) Attr(ctx context.Context, a *fuse.Attr) error {

	if err := it.metaRefresh(); err == nil {

		a.Mode = os.FileMode(it.node.fsMeta.Mode)

		a.Size = uint64(it.node.fsMeta.Size)

		a.Ctime = nsTime(it.node.fsMeta.Ctime)
		a.Mtime = nsTime(it.node.fsMeta.Mtime)
		a.Atime = nsTime(it.node.fsMeta.Atime)

	} else {
		a.Mode = os.ModeDir | 0755
	}

	// a.BlockSize = 4096 //uint32(a.Size) // 4096
	a.Blocks = a.Size / 512
	if n := a.Size % 512; n > 0 {
		a.Blocks++
	}

	a.Uid = osUserUid
	a.Gid = osUserGid

	return nil
}

var _ fs.NodeSetattrer = &kvFsDir{}

func (it *kvFsDir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

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

	if req.Valid.Atime() {
		if it.node.fsMeta.Atime != req.Atime.UnixNano() {
			it.node.fsMeta.Atime, flush = req.Atime.UnixNano(), true
		}
	}

	if req.Valid.Mtime() {
		if it.node.fsMeta.Mtime != req.Mtime.UnixNano() {
			it.node.fsMeta.Mtime, flush = req.Mtime.UnixNano(), true
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
				it.node.fsMeta.Ctime = timens()
			}
		}

		return it.fs.cache.flush(it.node)
	}

	return nil
}

var _ fs.HandleReadDirAller = &kvFsDir{}

func (it *kvFsDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	fold := newPathItem(it.path)

	rs := it.fs.tbl.NewReader(nil).
		KeyRangeSet(fold.foldKey(), append(fold.foldKey(), []byte{0xff}...)).
		AttrSet(kv2.ObjectMetaAttrDataOff).
		LimitNumSet(10000).
		Query()

	if !rs.OK() {
		return nil, errors.New(rs.Message)
	}

	ls := []fuse.Dirent{}
	for _, v := range rs.Items {

		var fsMeta KvFsMeta
		if err := v.DataValue().Decode(&fsMeta, kvFsDataCodec); err != nil {
			continue
		}

		node := it.fs.cache.set(v.Meta, &fsMeta)

		item := fuse.Dirent{
			Name: pathName(node.fsMeta.Path),
		}

		if kv2.AttrAllow(node.dbMeta.Attrs, ExtMetaAttrFoldMeta) {
			item.Type = fuse.DT_Dir
		} else {
			item.Type = fuse.DT_File
		}

		ls = append(ls, item)
	}

	return ls, nil
}

var _ fs.NodeStringLookuper = &kvFsDir{}

func (it *kvFsDir) Lookup(ctx context.Context, name string) (fs.Node, error) {

	var (
		n    fs.Node
		path = pathFilter(it.path + "/" + name)
	)

	node, err := it.fs.cache.get(path)
	if err != nil {
		return n, err
	}

	if kv2.AttrAllow(node.dbMeta.Attrs, ExtMetaAttrFoldMeta) {
		n = &kvFsDir{
			fs:   it.fs,
			path: path,
			node: node,
		}
	} else if kv2.AttrAllow(node.dbMeta.Attrs, ExtMetaAttrFileMeta) {
		n = &kvFsFile{
			fs:   it.fs,
			node: node,
		}
	} else {
		return nil, fuse.ENOENT
	}

	return n, nil
}

var _ fs.NodeMkdirer = &kvFsDir{}

func (it *kvFsDir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

	fsMetaKey := newPathItem(it.path + "/" + req.Name)

	batch := it.fs.tbl.NewBatch()

	var (
		tn     = timens()
		fsMeta = KvFsMeta{
			Path:  fsMetaKey.path,
			Mode:  uint32(req.Mode),
			Ctime: tn,
			Mtime: tn,
			Atime: tn,
		}
	)
	w := batch.Create(fsMetaKey.metaKey(), fsMeta, kvFsDataCodec)
	w.Meta.Attrs |= ExtMetaAttrFoldMeta
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	rs := batch.Commit()
	if !rs.OK() {
		return nil, fuse.EIO
	}

	return &kvFsDir{
		fs:   it.fs,
		path: fsMetaKey.path,
		node: it.fs.cache.set(w.Meta, &fsMeta),
	}, nil
}

var _ fs.NodeCreater = &kvFsDir{}

func (it *kvFsDir) Create(ctx context.Context,
	req *fuse.CreateRequest, rsp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	fsMetaKey := newPathItem(it.path + "/" + req.Name)

	node, err := it.fs.cache.get(fsMetaKey.path)
	if err == nil {
		f := &kvFsFile{
			fs:   it.fs,
			wNum: 1,
			node: node,
		}
		return f, f, nil
	}

	batch := it.fs.tbl.NewBatch()

	rs := it.fs.tbl.NewReader(fsMetaKey.metaKey()).
		AttrSet(kv2.ObjectMetaAttrDataOff).
		Query()

	if !rs.OK() {

		if !rs.NotFound() {
			return nil, nil, fuse.EIO
		}

	} else {

		var fsMeta KvFsMeta
		if err := rs.DataValue().Decode(&fsMeta, kvFsDataCodec); err != nil {
			return nil, nil, err
		}

		if kv2.AttrAllow(rs.Meta.Attrs, ExtMetaAttrFoldMeta) {
			return nil, nil, errors.New("dir exists")
		}

		dk := nsKeyEncode(FileRemove, uint64ToBytes(rs.Meta.IncrId))

		batch.Put(dk, fsMeta, kvFsDataCodec)
		batch.Delete(fsMetaKey.metaKey())
	}

	var (
		tn     = timens()
		fsMeta = KvFsMeta{
			Path:  fsMetaKey.path,
			Mode:  uint32(req.Mode),
			Ctime: tn,
			Mtime: tn,
			Atime: tn,
		}
	)

	w := batch.Create(fsMetaKey.metaKey(), fsMeta, kvFsDataCodec).
		IncrNamespaceSet(fsInodeNamespace)
	w.Meta.Attrs |= ExtMetaAttrFileMeta
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	rs2 := batch.Commit()
	if !rs2.OK() || len(rs2.Items) < 1 {
		return nil, nil, fuse.EIO
	}

	rsMeta := rs2.Items[len(rs2.Items)-1]
	if rsMeta.Meta == nil || rsMeta.Meta.IncrId < 1 {
		return nil, nil, fuse.EIO
	}

	f := &kvFsFile{
		fs:   it.fs,
		wNum: 1,
		node: it.fs.cache.set(rsMeta.Meta, &fsMeta),
	}

	return f, f, nil
}

var _ fs.NodeRenamer = &kvFsDir{}

func (it *kvFsDir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {

	nodePre, err := it.fs.cache.get(it.path + "/" + req.OldName)
	if err != nil {
		return fuse.ENOENT
	}

	var (
		// dirNew  = pathFilter(it.path) //  + "/" + req.NewName)
		pathNew = pathFilter(it.path + "/" + req.NewName)
	)

	if newDir != nil {
		if pd, ok := newDir.(*kvFsDir); ok {
			pathNew = pathFilter(pd.path + "/" + req.NewName)
		}
	}

	nodeNew, err := it.fs.cache.get(pathNew)
	if err == nil {
		if nodeNew.dbMeta.IncrId == nodePre.dbMeta.IncrId {
			return nil
		}
		// force rewrite
		// return fuse.EIO
	}

	var (
		fsMetaKeyPre = newPathItem(nodePre.fsMeta.Path)
		fsMetaKeyNew = newPathItem(pathNew)
		batch        = it.fs.tbl.NewBatch()
	)

	fsMetaNew := nodePre.fsMeta.Copy()
	fsMetaNew.Path = fsMetaKeyNew.path

	w := batch.Put(fsMetaKeyNew.metaKey(), fsMetaNew, kvFsDataCodec).
		IncrNamespaceSet(fsInodeNamespace)
	w.Meta.IncrId = nodePre.dbMeta.IncrId
	w.Meta.Attrs = nodePre.dbMeta.Attrs
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	w = batch.Delete(fsMetaKeyPre.metaKey())
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	rs := batch.Commit()
	if !rs.OK() {
		return errors.New(rs.Message)
	}

	it.fs.cache.rename(nodePre.fsMeta.Path, pathNew)

	return nil
}

var _ fs.NodeRemover = &kvFsDir{}

func (it *kvFsDir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	if req.Dir {
		return nil
	}

	nodePre, err := it.fs.cache.get(it.path + "/" + req.Name)
	if err != nil {
		if err == fuse.ENOENT {
			return nil
		}
		return err
	}

	var (
		batch  = it.fs.tbl.NewBatch()
		chkNum = uint32(nodePre.fsMeta.Size / kvFsChunkSize)
	)

	if a := nodePre.fsMeta.Size % kvFsChunkSize; a > 0 {
		chkNum++
	}

	for i := uint32(0); i <= chkNum; i++ {
		w := batch.Delete(pathDataChunkKey(nodePre.dbMeta.IncrId, i))
		w.Meta.Attrs |= kv2.ObjectMetaAttrMetaOff
	}

	w := batch.Delete(nodePre.fsMeta.metaKey())
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	/**
	dk := nsKeyEncode(FileRemove, uint64ToBytes(nodePre.dbMeta.IncrId))

	w := batch.Put(dk, nodePre.fsMeta, kvFsDataCodec)
	w.Meta.IncrId = nodePre.dbMeta.IncrId
	w.Meta.Attrs = nodePre.dbMeta.Attrs
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	w = batch.Delete(nodePre.fsMeta.metaKey())
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff
	*/

	rs := batch.Commit()
	if !rs.OK() {
		return errors.New(rs.Message)
	}

	it.fs.cache.del(it.path + "/" + req.Name)

	return nil
}
