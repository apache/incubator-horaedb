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

import "net/http"

type Code int

const (
	Invalid           = Code(-1)
	Ok                = Code(0)
	InvalidParams     = Code(http.StatusBadRequest)
	BadRequest        = Code(http.StatusBadRequest)
	NotFound          = Code(http.StatusNotFound)
	TooManyRequests   = Code(http.StatusTooManyRequests)
	Internal          = Code(http.StatusInternalServerError)
	ErrNotImplemented = Code(http.StatusNotImplemented)

	// HTTPCodeUpperBound is a bound under which any Code should have the same meaning with the http status code.
	HTTPCodeUpperBound   = Code(1000)
	PrintHelpUsage       = Code(1001)
	ClusterAlreadyExists = Code(1002)
)

// ToHTTPCode converts the Code to http code.
// The Code below the HTTPCodeUpperBound has the same meaning as the http status code. However, for the other codes, we
// should define the conversion rules by ourselves.
func (c Code) ToHTTPCode() int {
	if c < HTTPCodeUpperBound {
		return int(c)
	}

	// TODO: use switch to convert the code to http code.
	return int(c)
}

func (c Code) ToInt() int {
	return int(c)
}
