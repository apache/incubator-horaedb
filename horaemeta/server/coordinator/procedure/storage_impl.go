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

package procedure

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"

	"github.com/apache/incubator-horaedb-meta/pkg/coderr"
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/apache/incubator-horaedb-meta/server/etcdutil"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
	"go.uber.org/zap"
)

const (
	Version              = "v1"
	PathProcedure        = "procedure"
	PathDeletedProcedure = "deletedProcedure"
)

type EtcdStorageImpl struct {
	client    *clientv3.Client
	clusterID uint32
	rootPath  string
}

func NewEtcdStorageImpl(client *clientv3.Client, rootPath string, clusterID uint32) Storage {
	return &EtcdStorageImpl{
		client:    client,
		clusterID: clusterID,
		rootPath:  rootPath,
	}
}

// CreateOrUpdate example:
// /{rootPath}/v1/procedure/{procedureType}/{procedureID} ->  {procedureState} + {data}
// ttl is only valid when greater than 0, if it is less than or equal to 0, it will be ignored.
func (e EtcdStorageImpl) CreateOrUpdate(ctx context.Context, meta Meta) error {
	s, err := encode(&meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}

	keyPath := e.generaNormalKeyPath(meta.Kind, meta.ID)

	opPut := clientv3.OpPut(keyPath, s)

	if _, err = e.client.Do(ctx, opPut); err != nil {
		return errors.WithMessage(err, "etcd put data failed")
	}

	return nil
}

// CreateOrUpdateWithTTL
// ttl is only valid when greater than 0, if it is less than or equal to 0, it will be ignored.
func (e EtcdStorageImpl) CreateOrUpdateWithTTL(ctx context.Context, meta Meta, ttlSec int64) error {
	s, err := encode(&meta)
	if err != nil {
		return errors.WithMessage(err, "encode meta failed")
	}

	keyPath := e.generaNormalKeyPath(meta.Kind, meta.ID)

	// TODO: This implementation will cause each procedure to correspond to an etcd lease, which may cause too much pressure on etcd? May need to optimize implementation.
	resp, err := e.client.Grant(ctx, ttlSec)
	if err != nil {
		return errors.WithMessage(err, "etcd get lease failed")
	}
	opPut := clientv3.OpPut(keyPath, s, clientv3.WithLease(resp.ID))

	if _, err = e.client.Do(ctx, opPut); err != nil {
		return errors.WithMessage(err, "etcd put data failed")
	}

	return nil
}

// Delete will delete the specified procedure, and try to delete its corresponding history procedure if it exists.
func (e EtcdStorageImpl) Delete(ctx context.Context, procedureType Kind, id uint64) error {
	keyPath := e.generaNormalKeyPath(procedureType, id)
	opDelete := clientv3.OpDelete(keyPath)

	if _, err := e.client.Txn(ctx).Then(opDelete).Commit(); err != nil {
		return err
	}

	deletedKeyPath := e.generaDeletedKeyPath(procedureType, id)
	opDeleteMark := clientv3.OpDelete(deletedKeyPath)
	// Try to delete history procedure if it exists.
	keyExists := clientv3util.KeyExists(deletedKeyPath)
	if _, err := e.client.Txn(ctx).If(keyExists).Then(opDeleteMark).Commit(); err != nil {
		log.Warn("drop history procedure failed", zap.String("deletedKeyPath", deletedKeyPath), zap.Error(err))
	}

	return nil
}

// MarkDeleted Do a soft deletion, and the deleted key's format is:
// /{rootPath}/v1/historyProcedure/{clusterID}/{procedureID}
func (e EtcdStorageImpl) MarkDeleted(ctx context.Context, procedureType Kind, id uint64) error {
	keyPath := e.generaNormalKeyPath(procedureType, id)
	meta, err := etcdutil.Get(ctx, e.client, keyPath)
	if err != nil {
		return errors.WithMessage(err, "get meta failed")
	}

	deletedKeyPath := e.generaDeletedKeyPath(procedureType, id)
	opDelete := clientv3.OpDelete(keyPath)
	opPut := clientv3.OpPut(deletedKeyPath, meta)

	_, err = e.client.Txn(ctx).Then(opDelete, opPut).Commit()

	return err
}

func (e EtcdStorageImpl) List(ctx context.Context, procedureType Kind, batchSize int) ([]*Meta, error) {
	var metas []*Meta
	do := func(key string, value []byte) error {
		meta, err := decodeMeta(string(value))
		if err != nil {
			return coderr.Wrapf(err, "decode meta failed, key:%s, value:%v", key, value)
		}

		metas = append(metas, meta)
		return nil
	}

	startKey := e.generaNormalKeyPath(procedureType, uint64(0))
	endKey := e.generaNormalKeyPath(procedureType, math.MaxUint64)

	err := etcdutil.Scan(ctx, e.client, startKey, endKey, batchSize, do)
	if err != nil {
		return nil, errors.WithMessage(err, "scan procedure failed")
	}
	return metas, nil
}

func (e EtcdStorageImpl) generaNormalKeyPath(procedureType Kind, procedureID uint64) string {
	return e.generateKeyPath(procedureID, procedureType, false)
}

func (e EtcdStorageImpl) generaDeletedKeyPath(procedureType Kind, procedureID uint64) string {
	return e.generateKeyPath(procedureID, procedureType, true)
}

func (e EtcdStorageImpl) generateKeyPath(procedureID uint64, procedureType Kind, isDeleted bool) string {
	var procedurePath string
	if isDeleted {
		procedurePath = PathDeletedProcedure
	} else {
		procedurePath = PathProcedure
	}
	return path.Join(e.rootPath, Version, procedurePath, fmtID(uint64(e.clusterID)), strconv.Itoa(int(procedureType)), fmtID(procedureID))
}

func fmtID(id uint64) string {
	return fmt.Sprintf("%020d", id)
}

// TODO: Use proto.Marshal replace json.Marshal
func encode(meta *Meta) (string, error) {
	bytes, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// TODO: Use proto.Unmarshal replace json.unmarshal
func decodeMeta(meta string) (*Meta, error) {
	var m Meta
	err := json.Unmarshal([]byte(meta), &m)
	return &m, err
}
