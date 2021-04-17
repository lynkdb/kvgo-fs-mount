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
	"errors"
	"sync"
	"time"

	"bazil.org/fuse"

	kv2 "github.com/lynkdb/kvspec/go/kvspec/v2"
)

const (
	fsNodeTTLMin int64 = 200  // ms
	fsNodeTTLDef int64 = 1000 // ms
)

type fsNodeList struct {
	mu    sync.Mutex
	tbl   kv2.ClientTable
	items map[string]*fsNodeItem
}

type fsNodeItem struct {
	dbMeta *kv2.ObjectMeta
	fsMeta *KvFsMeta
	tto    int64
}

func newFsNodeList(tbl kv2.ClientTable) *fsNodeList {
	return &fsNodeList{
		tbl:   tbl,
		items: map[string]*fsNodeItem{},
	}
}

func (it *fsNodeList) set(dbMeta *kv2.ObjectMeta, fsMeta *KvFsMeta) *fsNodeItem {

	it.mu.Lock()
	defer it.mu.Unlock()

	item, ok := it.items[fsMeta.Path]
	if ok {

		if item.dbMeta != dbMeta {
			item.dbMeta = dbMeta
		}

		if item.fsMeta != fsMeta {
			item.fsMeta = fsMeta
		}

	} else {

		item = &fsNodeItem{
			dbMeta: dbMeta,
			fsMeta: fsMeta,
		}

		it.items[fsMeta.Path] = item
	}

	item.tto = timems() + fsNodeTTLDef

	return item
}

func (it *fsNodeList) get(path string) (*fsNodeItem, error) {

	it.mu.Lock()
	defer it.mu.Unlock()

	tn := timems()

	dels := []string{}
	for k, v := range it.items {
		if v.tto <= tn {
			dels = append(dels, k)
		}
	}
	for _, k := range dels {
		delete(it.items, k)
	}

	path = pathFilter(path)

	item, ok := it.items[path]
	if ok {
		return item, nil
	}

	fsMetaKey := newPathItem(path)

	rs := it.tbl.NewReader(fsMetaKey.metaKey()).
		AttrSet(kv2.ObjectMetaAttrDataOff).
		Query()

	if (!rs.OK() && !rs.NotFound()) || len(rs.Items) == 0 {
		return nil, fuse.ENOENT
	}

	var fsMetaItem KvFsMeta
	if err := rs.DataValue().Decode(&fsMetaItem, kvFsDataCodec); err != nil {
		return nil, fuse.ENOENT
	}

	item = &fsNodeItem{
		dbMeta: rs.Items[0].Meta,
		fsMeta: &fsMetaItem,
		tto:    timems() + fsNodeTTLDef,
	}

	it.items[item.fsMeta.Path] = item

	return item, nil
}

func (it *fsNodeList) flush(node *fsNodeItem) error {

	it.mu.Lock()
	defer it.mu.Unlock()

	batch := it.tbl.NewBatch()

	fsMetaKey := newPathItem(node.fsMeta.Path)

	w := batch.Put(fsMetaKey.metaKey(), node.fsMeta, kvFsDataCodec).
		IncrNamespaceSet(fsInodeNamespace)

	w.Meta.IncrId = node.dbMeta.IncrId
	w.Meta.Attrs = node.dbMeta.Attrs
	w.Meta.Attrs |= kv2.ObjectMetaAttrDataOff

	rs := batch.Commit()
	if !rs.OK() {
		return errors.New(rs.Message)
	}

	node.tto = (time.Now().UnixNano() / 1e6) + fsNodeTTLDef
	it.items[node.fsMeta.Path] = node

	return nil
}

func (it *fsNodeList) rename(path1, path2 string) {

	it.mu.Lock()
	defer it.mu.Unlock()

	path1 = pathFilter(path1)
	v, ok := it.items[path1]
	if !ok {
		return
	}
	delete(it.items, path1)

	v.fsMeta.Path = path2
	v.tto = (time.Now().UnixNano() / 1e6) + fsNodeTTLDef

	it.items[path2] = v
}

func (it *fsNodeList) del(path string) {

	it.mu.Lock()
	defer it.mu.Unlock()

	delete(it.items, pathFilter(path))

}
