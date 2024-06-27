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

package id

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/server/etcdutil"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.uber.org/zap"
)

type AllocatorImpl struct {
	logger *zap.Logger
	// RWMutex is used to protect following fields.
	lock sync.Mutex
	base uint64
	end  uint64

	kv            clientv3.KV
	key           string
	allocStep     uint
	isInitialized bool
}

func NewAllocatorImpl(logger *zap.Logger, kv clientv3.KV, key string, allocStep uint) Allocator {
	return &AllocatorImpl{
		logger:        logger,
		lock:          sync.Mutex{},
		base:          0,
		end:           0,
		kv:            kv,
		key:           key,
		allocStep:     allocStep,
		isInitialized: false,
	}
}

func (a *AllocatorImpl) isExhausted() bool {
	return a.base == a.end
}

func (a *AllocatorImpl) Alloc(ctx context.Context) (uint64, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if !a.isInitialized {
		if err := a.slowRebaseLocked(ctx); err != nil {
			return 0, errors.WithMessage(err, "alloc id")
		}
		a.isInitialized = true
	}

	if a.isExhausted() {
		if err := a.fastRebaseLocked(ctx); err != nil {
			a.logger.Warn("fast rebase failed", zap.Error(err))

			if err = a.slowRebaseLocked(ctx); err != nil {
				return 0, errors.WithMessage(err, "alloc id")
			}
		}
	}

	ret := a.base
	a.base++
	return ret, nil
}

func (a *AllocatorImpl) Collect(_ context.Context, _ uint64) error {
	return ErrCollectNotSupported.WithMessagef("")
}

func (a *AllocatorImpl) slowRebaseLocked(ctx context.Context) error {
	resp, err := a.kv.Get(ctx, a.key)
	if err != nil {
		a.logger.Error("get end id", zap.String("resp", fmt.Sprintf("%v", resp)), zap.String("key", a.key))
		return coderr.Wrapf(err, "get end id failed, key:%s", a.key)
	}

	if n := len(resp.Kvs); n > 1 {
		return etcdutil.ErrEtcdKVGetResponse.WithMessagef("%v", resp.Kvs)
	}

	// Key is not exist, create key in kv storage.
	if len(resp.Kvs) == 0 {
		return a.firstDoRebaseLocked(ctx)
	}

	currEnd := string(resp.Kvs[0].Value)
	return a.doRebaseLocked(ctx, decodeID(a.logger, currEnd))
}

func (a *AllocatorImpl) fastRebaseLocked(ctx context.Context) error {
	return a.doRebaseLocked(ctx, a.end)
}

func (a *AllocatorImpl) firstDoRebaseLocked(ctx context.Context) error {
	newEnd := a.allocStep

	keyMissing := clientv3util.KeyMissing(a.key)
	opPutEnd := clientv3.OpPut(a.key, encodeID(uint64(newEnd)))

	resp, err := a.kv.Txn(ctx).
		If(keyMissing).
		Then(opPutEnd).
		Commit()
	if err != nil {
		return coderr.Wrapf(err, "put end id failed, key:%s", a.key)
	} else if !resp.Succeeded {
		return ErrTxnPutEndID.WithMessagef("txn put end id failed, key is exist, key:%s, resp:%v", a.key, resp)
	}

	a.end = uint64(newEnd)

	a.logger.Info("Allocator allocates a new base id", zap.String("key", a.key), zap.Uint64("id", a.base))
	return nil
}

func (a *AllocatorImpl) doRebaseLocked(ctx context.Context, currEnd uint64) error {
	if currEnd < a.base {
		return ErrAllocID.WithMessagef("ID in storage can't less than memory, base:%d, end:%d", a.base, currEnd)
	}

	newEnd := currEnd + uint64(a.allocStep)

	endEquals := clientv3.Compare(clientv3.Value(a.key), "=", encodeID(currEnd))
	opPutEnd := clientv3.OpPut(a.key, encodeID(newEnd))

	resp, err := a.kv.Txn(ctx).
		If(endEquals).
		Then(opPutEnd).
		Commit()
	if err != nil {
		return coderr.Wrapf(err, "put end id failed, key:%s, old value:%d, new value:%d", a.key, currEnd, newEnd)
	} else if !resp.Succeeded {
		return ErrTxnPutEndID.WithMessagef("txn put end id failed, endEquals failed, key:%s, value:%d, resp:%v", a.key, currEnd, resp)
	}

	a.base = currEnd
	a.end = newEnd

	a.logger.Info("Allocator allocates a new base id", zap.String("key", a.key), zap.Uint64("id", a.base))

	return nil
}

func encodeID(value uint64) string {
	return fmt.Sprintf("%d", value)
}

func decodeID(logger *zap.Logger, value string) uint64 {
	res, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		logger.Error("convert string to int failed", zap.Error(err), zap.String("val", value))
	}
	return res
}
