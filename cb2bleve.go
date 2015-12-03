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
	"flag"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
)

var cbUrl = flag.String("url", "http://localhost:8091", "server url")
var cbPool = flag.String("pool", "default", "pool name")
var cbBucket = flag.String("bucket", "default", "bucket name")
var authUser = flag.String("authUser", "",
	"auth user name (probably same as bucketName)")
var authPswd = flag.String("authPswd", "",
	"auth password")

var indexPath = flag.String("index", "cb.bleve", "index path")
var mappingFile = flag.String("mapping", "", "mapping file")
var storeType = flag.String("store", bleve.Config.DefaultKVStore, "store type")
var indexType = flag.String("indexType", bleve.Config.DefaultIndexType, "index type")

func main() {
	flag.Parse()

	// get the end point for this export
	// highWaterMark, err := HighWaterMark(*cbUrl, *cbPool, *cbBucket)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	options := cbdatasource.DefaultBucketDataSourceOptions
	options.Name = "cb2bleve"
	//options.SeqEnd = highWaterMark

	r, err := NewBleveIndxingDCPReceiver(*indexPath, *storeType, *indexType, *mappingFile)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	var auth couchbase.AuthHandler = nil
	if *authUser != "" {
		auth = &authUserPswd{}
	}

	cbd, err := cbdatasource.NewBucketDataSource([]string{*cbUrl}, *cbPool, *cbBucket, "", nil, auth, r, options)
	if err != nil {
		log.Fatal(err)
	}

	err = cbd.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Started...")
	defer cbd.Close()

	for {
		time.Sleep(1000 * time.Millisecond)
		r.Report()
	}
}

func HighWaterMark(cbUrl, cbPool, cbBucket string) (map[uint16]uint64, error) {
	bucket, err := couchbase.GetBucket(cbUrl, cbPool, cbBucket)
	if err != nil {
		return nil, err
	}
	rv := make(map[uint16]uint64, 1024)
	stats := bucket.GetStats("vbucket-seqno")
	for _, hostStats := range stats {
		for statKey, statValue := range hostStats {
			keyParts := strings.SplitN(statKey, ":", 2)
			if keyParts[1] == "high_seqno" {
				vbucketParts := strings.SplitN(keyParts[0], "_", 2)
				vbucket, err := strconv.Atoi(vbucketParts[1])
				if err != nil {
					return nil, err
				}
				seqNum, err := strconv.Atoi(statValue)
				if err != nil {
					return nil, err
				}
				rv[uint16(vbucket)] = uint64(seqNum)
			}
		}
	}
	bucket.Close()
	return rv, nil
}
