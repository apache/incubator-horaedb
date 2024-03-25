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
	"fmt"

	"github.com/pkg/errors"
)

var _ CodeErrorDef = &codeErrorDef{code: 0, desc: ""}
var _ CodeError = &codeError{code: 0, msg: "", hasCause: false, causeWithStack: nil}

type CodeError interface {
	error
	Code() Code
}

// CodeErrorDef is an error definition, and able to generate a real CodeError
type CodeErrorDef interface {
	Code() Code
	// WithMessagef should generate a new CodeError instance with the provided message.
	WithMessagef(format string, a ...any) CodeError
	// WithCause should generate a new CodeError instance with the provided cause details.
	WithCause(cause error) CodeError
	// WithCausef should generate a new CodeError instance with the provided cause details and error message.
	WithCausef(cause error, format string, a ...any) CodeError
}

// Is checks whether the cause of `err` is the kind of error specified by the `expectCode`.
// Returns false if the cause of `err` is not CodeError.
func Is(err error, expectCode Code) bool {
	code, ok := GetCauseCode(err)
	if ok && code == expectCode {
		return true
	}

	return false
}

// codeError is the default implementation of CodeError.
type codeError struct {
	code           Code
	msg            string
	causeWithStack error
	hasCause       bool
}

func (e *codeError) Error() string {
	if e.hasCause {
		return fmt.Sprintf("[err_code=%d]%s, cause:%v", e.code, e.msg, e.causeWithStack)
	} else {
		return fmt.Sprintf("[err_code=%d]%s", e.code, e.msg)
	}
}

func (e *codeError) Code() Code {
	return e.code
}

// NewCodeErrorDef creates an CodeError definition.
// The provided code should be defined in the code.go in this package.
func NewCodeErrorDef(code Code, desc string) CodeErrorDef {
	return &codeErrorDef{
		code: code,
		desc: desc,
	}
}

// codeErrorDef is the default implementation of CodeErrorDef.
type codeErrorDef struct {
	code Code
	desc string
}

func (e *codeErrorDef) Code() Code {
	return e.code
}

func (e *codeErrorDef) WithMessagef(format string, a ...any) CodeError {
	errMsg := fmt.Sprintf(format, a...)
	causeWithStack := errors.WithStack(errors.New(""))
	return &codeError{
		code:           e.code,
		msg:            fmt.Sprintf("%s, %s", e.desc, errMsg),
		causeWithStack: causeWithStack,
		hasCause:       false,
	}
}

func (e *codeErrorDef) WithCause(cause error) CodeError {
	causeWithStack := errors.WithStack(cause)
	return &codeError{
		code:           e.code,
		msg:            e.desc,
		causeWithStack: causeWithStack,
		hasCause:       true,
	}
}

func (e *codeErrorDef) WithCausef(cause error, format string, a ...any) CodeError {
	errMsg := fmt.Sprintf(format, a...)
	causeWithStack := errors.WithStack(cause)
	return &codeError{
		code:           e.code,
		msg:            fmt.Sprintf("%s, %s", e.desc, errMsg),
		causeWithStack: causeWithStack,
		hasCause:       true,
	}
}

func Wrapf(err error, format string, args ...interface{}) error {
	return errors.WithMessagef(err, format, args...)
}

// FormatErrorWithStack prints the error message with the call stack information if it contains.
func FormatErrorWithStack(e error) string {
	cErr, ok := e.(*codeError)
	if !ok {
		return e.Error()
	}

	if cErr.hasCause {
		return fmt.Sprintf("[err_code=%d]%s, causeWithStack:%+v", cErr.code, cErr.msg, cErr.causeWithStack)
	} else {
		return fmt.Sprintf("[err_code=%d]%s, stack:%+v", cErr.code, cErr.msg, cErr.causeWithStack)
	}
}

// GetCauseCode extract the error code of the first layer.
func GetCauseCode(err error) (Code, bool) {
	if err == nil {
		return Invalid, false
	}

	cause := errors.Cause(err)
	cErr, ok := cause.(CodeError)
	if !ok {
		return Invalid, false
	}
	return cErr.Code(), true
}
