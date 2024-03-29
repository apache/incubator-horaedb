// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is copied from:
// https://github.com/prometheus/common/blob/8c9cb3fa6d01832ea16937b20ea561eed81abd2f/route/route.go

package http

import (
	"context"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type param string

const DebugPrefix = "/debug"

// Router wraps httprouter.Router and adds support for prefixed sub-routers,
// per-request context injections and instrumentation.
type Router struct {
	rtr    *httprouter.Router
	prefix string
	instrh func(handlerName string, handler http.HandlerFunc) http.HandlerFunc
}

func New() *Router {
	return &Router{
		rtr:    httprouter.New(),
		prefix: "",
		instrh: nil,
	}
}

// WithPrefix returns a router that prefixes all registered routes with prefix.
func (r *Router) WithPrefix(prefix string) *Router {
	return &Router{rtr: r.rtr, prefix: r.prefix + prefix, instrh: r.instrh}
}

// WithInstrumentation returns a router with instrumentation support.
func (r *Router) WithInstrumentation(instrh func(handlerName string, handler http.HandlerFunc) http.HandlerFunc) *Router {
	if r.instrh != nil {
		newInstrh := instrh
		instrh = func(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
			return newInstrh(handlerName, r.instrh(handlerName, handler))
		}
	}
	return &Router{rtr: r.rtr, prefix: r.prefix, instrh: instrh}
}

// ServeHTTP implements http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.rtr.ServeHTTP(w, req)
}

// Get registers a new GET route.
func (r *Router) Get(path string, h http.HandlerFunc) {
	r.rtr.GET(r.prefix+path, r.handle(path, h))
}

// DebugGet registers a new GET route without prefix.
func (r *Router) DebugGet(path string, h http.HandlerFunc) {
	r.rtr.GET(DebugPrefix+path, r.handle(path, h))
}

// Options registers a new OPTIONS route.
func (r *Router) Options(path string, h http.HandlerFunc) {
	r.rtr.OPTIONS(r.prefix+path, r.handle(path, h))
}

// Del registers a new DELETE route.
func (r *Router) Del(path string, h http.HandlerFunc) {
	r.rtr.DELETE(r.prefix+path, r.handle(path, h))
}

// Put registers a new PUT route.
func (r *Router) Put(path string, h http.HandlerFunc) {
	r.rtr.PUT(r.prefix+path, r.handle(path, h))
}

// DebugPut registers a new PUT route without prefix.
func (r *Router) DebugPut(path string, h http.HandlerFunc) {
	r.rtr.PUT(DebugPrefix+path, r.handle(path, h))
}

// Post registers a new POST route.
func (r *Router) Post(path string, h http.HandlerFunc) {
	r.rtr.POST(r.prefix+path, r.handle(path, h))
}

// Head registers a new HEAD route.
func (r *Router) Head(path string, h http.HandlerFunc) {
	r.rtr.HEAD(r.prefix+path, r.handle(path, h))
}

// handle turns a HandlerFunc into a httprouter.Handle.
func (r *Router) handle(handlerName string, h http.HandlerFunc) httprouter.Handle {
	if r.instrh != nil {
		// This needs to be outside the closure to avoid data race when reading and writing to 'h'.
		h = r.instrh(handlerName, h)
	}
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		for _, p := range params {
			ctx = context.WithValue(ctx, param(p.Key), p.Value)
		}
		h(w, req.WithContext(ctx))
	}
}

// Param returns param p for the context, or the empty string when
// param does not exist in context.
func Param(ctx context.Context, p string) string {
	if v := ctx.Value(param(p)); v != nil {
		return v.(string)
	}
	return ""
}
