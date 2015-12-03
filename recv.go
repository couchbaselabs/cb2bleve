//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve"
	"github.com/couchbase/gomemcached"
)

type BleveIndexingDCPReceiver struct {
	index   bleve.Index
	updates int64
	deletes int64

	m sync.Mutex

	seqs map[uint16]uint64 // To track max seq #'s we received per vbucketID.
	meta map[uint16][]byte // To track metadata blob's per vbucketID.
}

func NewBleveIndxingDCPReceiver(path, storeType, indexType, mappingPath string) (*BleveIndexingDCPReceiver, error) {
	mapping := bleve.NewIndexMapping()
	if mappingPath != "" {
		mappingBytes, err := ioutil.ReadFile(mappingPath)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(mappingBytes, &mapping)
		if err != nil {
			return nil, err
		}
	}
	index, err := bleve.NewUsing(path, mapping, indexType, storeType, nil)
	if err != nil {
		return nil, err
	}
	return &BleveIndexingDCPReceiver{index: index}, nil
}

func (b *BleveIndexingDCPReceiver) OnError(err error) {
	log.Printf("dcp err: %v", err)
}

func (b *BleveIndexingDCPReceiver) DataUpdate(vbucketID uint16, key []byte, seq uint64,
	r *gomemcached.MCRequest) error {
	var v interface{}
	err := json.Unmarshal(r.Body, &v)
	if err != nil {
		return err
	}
	err = b.index.Index(string(key), v)
	if err != nil {
		return err
	}
	updates := atomic.LoadInt64(&b.updates)
	log.Printf(" updating (#%d), vb: %d, key: %s", updates, vbucketID, string(key))
	atomic.AddInt64(&b.updates, 1)

	b.updateSeq(vbucketID, seq)

	return nil
}

func (b *BleveIndexingDCPReceiver) DataDelete(vbucketID uint16, key []byte, seq uint64,
	r *gomemcached.MCRequest) error {
	deletes := atomic.LoadInt64(&b.deletes)
	log.Printf(" deleting (#%d), vb: %d, key: %s", deletes, vbucketID, string(key))
	atomic.AddInt64(&b.deletes, 1)

	b.updateSeq(vbucketID, seq)

	return b.index.Delete(string(key))
}

func (b *BleveIndexingDCPReceiver) updateSeq(vbucketID uint16, seq uint64) {
	b.m.Lock()
	defer b.m.Unlock()

	if b.seqs == nil {
		b.seqs = make(map[uint16]uint64)
	}
	if b.seqs[vbucketID] < seq {
		b.seqs[vbucketID] = seq // Remember the max seq for GetMetaData().
	}
}

func (b *BleveIndexingDCPReceiver) SnapshotStart(vbucketID uint16, snapStart, snapEnd uint64, snapType uint32) error {
	log.Printf("ss start, vb: %d, [%d - %d]", vbucketID, snapStart, snapEnd)
	return nil
}

func (b *BleveIndexingDCPReceiver) SetMetaData(vbucketID uint16, value []byte) error {
	log.Printf("setMetaData, vb: %d", vbucketID)

	b.m.Lock()
	defer b.m.Unlock()

	if b.meta == nil {
		b.meta = make(map[uint16][]byte)
	}
	b.meta[vbucketID] = value

	return nil
}

func (b *BleveIndexingDCPReceiver) GetMetaData(vbucketID uint16) (value []byte, lastSeq uint64, err error) {
	log.Printf("getMetaData, vb: %d", vbucketID)

	b.m.Lock()
	defer b.m.Unlock()

	value = []byte(nil)
	if b.meta != nil {
		value = b.meta[vbucketID]
	}

	if b.seqs != nil {
		lastSeq = b.seqs[vbucketID]
	}

	return value, lastSeq, nil
}

func (b *BleveIndexingDCPReceiver) Rollback(vbucketID uint16, rollbackSeq uint64) error {
	log.Printf("ROLLBACK, vb: %d, rollbackSeq: %d", vbucketID, rollbackSeq)
	return fmt.Errorf("unable to rollback")
}

func (b *BleveIndexingDCPReceiver) Close() error {
	log.Printf("Close")
	return b.index.Close()
}

func (b *BleveIndexingDCPReceiver) Report() {
	updates := atomic.LoadInt64(&b.updates)
	deletes := atomic.LoadInt64(&b.deletes)
	fmt.Printf("Updates: %d, deletes: %d\n", updates, deletes)
}
