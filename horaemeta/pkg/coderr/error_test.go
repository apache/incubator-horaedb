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

package coderr

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var testError0 CodeErrorDef = NewCodeErrorDef(Internal, "drop table")
var testError1 CodeErrorDef = NewCodeErrorDef(BadRequest, "table not found")

func stackFunc0() error {
	return testError0.WithCause(stackFunc1())
}

func stackFunc1() error {
	return testError1.WithMessagef("table:demo")
}

func TestErrorMsg(t *testing.T) {
	err := stackFunc0()
	errMsg := err.Error()
	assert.Equal(t, "[err_code=500]drop table, cause:[err_code=400]table not found, table:demo", errMsg)
}

func TestErrorMsgWithStack(t *testing.T) {
	err := stackFunc0()
	errMsgWithStack := FormatErrorWithStack(err)
	assert.True(t, strings.Contains(errMsgWithStack, "horaemeta/pkg/coderr"))
	assert.True(t, strings.Contains(errMsgWithStack, "\n"))
}

func TestCauseCode(t *testing.T) {
	err := stackFunc1()
	code, ok := GetCauseCode(err)
	assert.True(t, ok)
	assert.Equal(t, BadRequest, code)

	err = stackFunc0()
	code, ok = GetCauseCode(err)
	assert.True(t, ok)
	assert.Equal(t, Internal, code)

	err = errors.WithMessage(stackFunc0(), "wrapped error")
	code, ok = GetCauseCode(err)
	assert.True(t, ok)
	assert.Equal(t, Internal, code)
}
