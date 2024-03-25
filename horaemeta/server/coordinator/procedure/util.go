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
	"github.com/apache/incubator-horaedb-meta/pkg/log"
	"github.com/looplab/fsm"
	"go.uber.org/zap"
)

// CancelEventWithLog Cancel event when error is not nil. If error is nil, do nothing.
func CancelEventWithLog(event *fsm.Event, err error) {
	if err == nil {
		return
	}
	log.Error("cancel the event for the error occurs", zap.Error(err))
	event.Cancel(err)
}

// nolint
func GetRequestFromEvent[T any](event *fsm.Event) (T, error) {
	if len(event.Args) != 1 {
		return *new(T), ErrGetRequest.WithMessagef("event args length must be 1, actual length:%v", len(event.Args))
	}

	switch request := event.Args[0].(type) {
	case T:
		return request, nil
	default:
		return *new(T), ErrGetRequest.WithMessagef("event arg type must be same as return type")
	}
}
