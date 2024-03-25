/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package etcdutil

import (
	"context"
	"path"

	"github.com/apache/incubator-horaedb-meta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func Get(ctx context.Context, client *clientv3.Client, key string) (string, error) {
	resp, err := client.Get(ctx, key)
	if err != nil {
		return "", ErrEtcdKVGet.WithCause(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", ErrEtcdKVGetNotFound.WithMessagef("empty key values")
	} else if n > 1 {
		return "", ErrEtcdKVGetResponse.WithMessagef("%v", resp.Kvs)
	}

	return string(resp.Kvs[0].Value), nil
}

func List(ctx context.Context, client *clientv3.Client, prefix string) ([]string, error) {
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return []string{}, ErrEtcdKVGet.WithCause(err)
	}
	var result []string
	for _, kv := range resp.Kvs {
		result = append(result, string(kv.Key))
	}
	return result, nil
}

func Scan(ctx context.Context, client *clientv3.Client, startKey, endKey string, batchSize int, do func(key string, val []byte) error) error {
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(batchSize))

	// Take a special process for the first batch.
	resp, err := client.Get(ctx, startKey, withRange, withLimit)
	if err != nil {
		return ErrEtcdKVGet.WithCause(err)
	}
	if len(resp.Kvs) == 0 {
		return nil
	}

	doIfNotEndKey := func(key, val []byte) error {
		// TODO: avoid such a copy on key.
		keyStr := string(key)
		if keyStr == endKey {
			return nil
		}

		return do(keyStr, val)
	}

	for _, item := range resp.Kvs {
		err := doIfNotEndKey(item.Key, item.Value)
		if err != nil {
			return err
		}
	}

	lastKeyInPrevBatch := string(resp.Kvs[len(resp.Kvs)-1].Key)
	// The following batches always contain one key in the previous batch, so we have to increment the batchSize to batchSize + 1;
	withLimit = clientv3.WithLimit(int64(batchSize + 1))
	for {
		if lastKeyInPrevBatch == endKey {
			log.Warn("Stop scanning because the end key is reached", zap.String("endKey", endKey))
			return nil
		}
		startKey = lastKeyInPrevBatch

		// Get the keys range [startKey, endKey).
		resp, err := client.Get(ctx, startKey, withRange, withLimit)
		if err != nil {
			return ErrEtcdKVGet.WithCause(err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(resp.Kvs) <= 1 {
			// The only one key is `startKey` which is actually processed already.
			return nil
		}

		// Skip the first key which is processed already.
		for _, item := range resp.Kvs[1:] {
			err := doIfNotEndKey(item.Key, item.Value)
			if err != nil {
				return err
			}
		}

		// Check whether the keys are exhausted.
		if len(resp.Kvs) < batchSize {
			return nil
		}

		lastKeyInPrevBatch = string(resp.Kvs[len(resp.Kvs)-1].Key)
	}
}

func ScanWithPrefix(ctx context.Context, client *clientv3.Client, prefix string, do func(key string, val []byte) error) error {
	rangeEnd := clientv3.GetPrefixRangeEnd(prefix)
	resp, err := client.Get(ctx, prefix, clientv3.WithRange(rangeEnd))
	if err != nil {
		return ErrEtcdKVGet.WithCause(err)
	}
	// Check whether the keys are exhausted.
	if len(resp.Kvs) == 0 {
		return nil
	}

	for _, item := range resp.Kvs {
		err := do(string(item.Key), item.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetLastPathSegment get the last path segment from completePath, path is split by '/'.
func GetLastPathSegment(completePath string) string {
	return path.Base(path.Clean(completePath))
}
