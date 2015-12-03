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
	"sync/atomic"

	"github.com/blevesearch/bleve"
	"github.com/couchbase/gomemcached"
)

type BleveIndexingDCPReceiver struct {
	index   bleve.Index
	updates int64
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
	log.Printf("updating key: %s", string(key))
	atomic.AddInt64(&b.updates, 1)
	return nil
}

func (b *BleveIndexingDCPReceiver) DataDelete(vbucketID uint16, key []byte, seq uint64,
	r *gomemcached.MCRequest) error {
	return b.index.Delete(string(key))
}

func (b *BleveIndexingDCPReceiver) SnapshotStart(vbucketID uint16, snapStart, snapEnd uint64, snapType uint32) error {
	return nil
}

func (b *BleveIndexingDCPReceiver) SetMetaData(vbucketID uint16, value []byte) error {
	return nil
}

func (b *BleveIndexingDCPReceiver) GetMetaData(vbucketID uint16) (value []byte, lastSeq uint64, err error) {
	return nil, 0, nil
}

func (b *BleveIndexingDCPReceiver) Rollback(vbucketID uint16, rollbackSeq uint64) error {
	return fmt.Errorf("unable to rollback")
}

func (b *BleveIndexingDCPReceiver) Close() error {
	return b.index.Close()
}

func (b *BleveIndexingDCPReceiver) Report() {
	updates := atomic.LoadInt64(&b.updates)
	fmt.Printf("Updates: %d\n", updates)
}
