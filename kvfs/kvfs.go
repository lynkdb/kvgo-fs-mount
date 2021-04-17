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

	"github.com/golang/protobuf/proto"
)

var (
	kvFsDataCodec = &fsDataValueCodec{}
)

type fsDataValueCodec struct{}

func (it *fsDataValueCodec) Decode(bs []byte, obj interface{}) error {
	if obj2, ok := obj.(proto.Message); ok {
		return proto.Unmarshal(bs, obj2)
	}
	return errors.New("invalid object type")
}

func (fsDataValueCodec) Encode(obj interface{}) ([]byte, error) {

	var (
		bs  []byte
		err error
	)

	switch obj.(type) {

	case KvFsMeta:
		obj2, _ := obj.(KvFsMeta)
		bs, err = proto.Marshal(&obj2)

	case *KvFsMeta:
		obj2, _ := obj.(*KvFsMeta)
		bs, err = proto.Marshal(obj2)

	case KvFsChunk:
		obj2, _ := obj.(KvFsChunk)
		bs, err = proto.Marshal(&obj2)

	case *KvFsChunk:
		obj2, _ := obj.(*KvFsChunk)
		bs, err = proto.Marshal(obj2)

	default:
		err = errors.New("invalid object type")
	}

	if err != nil {
		// bs = append([]byte{0x00}, bs...)
	}

	return bs, err
}

func (it *KvFsMeta) Copy() *KvFsMeta {
	return &KvFsMeta{
		Path:  it.Path,
		Size:  it.Size,
		Attrs: it.Attrs,
		Mode:  it.Mode,
		Ctime: it.Ctime,
		Mtime: it.Mtime,
		Atime: it.Atime,
	}
}

func (it *KvFsMeta) metaKey() []byte {
	return newPathItem(it.Path).metaKey()
}
