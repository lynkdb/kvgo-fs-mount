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
	"encoding/binary"
	"hash/crc32"
	"time"
)

func bytesClone(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func bytesCrc32Checksum(bs []byte) uint64 {
	sumCheck := crc32.ChecksumIEEE(bs)
	if sumCheck == 0 {
		sumCheck = 1
	}
	return uint64(sumCheck)
}

func uint32ToBytes(v uint32) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, v)
	return bs
}

func uint64ToBytes(v uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, v)
	return bs
}

func nsKeyEncode(ns uint8, args ...[]byte) []byte {
	k := []byte{ns}
	for _, v := range args {
		k = append(k, bytesClone(v)...)
	}
	return k
}

func nsTime(t int64) time.Time {
	return time.Unix(t/1e9, (t % 1e9))
}

func timems() int64 {
	return time.Now().UnixNano() / 1e6
}

func timens() int64 {
	return time.Now().UnixNano()
}
