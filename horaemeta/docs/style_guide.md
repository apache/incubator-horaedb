# Style Guide

HoraeMeta is written in Golang so the basic code style we adhere to is the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments).

Besides the [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments), there are also some custom rules for the project:

- Error Handling
- Logging

## Error Handling

### Principles

- Global error code:
  - Any error defined in the repo should be assigned an error code,
  - An error code can be used by multiple different errors,
  - The error codes are defined in the single global package [coderr](https://github.com/apache/incubator-horaedb-meta/tree/main/pkg/coderr).
- Construct: define leaf errors on package level (often in a separate `error.go` file) by package [coderr](https://github.com/apache/incubator-horaedb-meta/tree/main/pkg/coderr).
- Wrap: wrap errors by `coderr.Wrapf`.
- Check: test the error identity by calling `coderr.Is`.
- Log: only log the error on the top level package.
- Respond: respond the `CodeError`(defined in package [coderr](https://github.com/apache/incubator-horaedb-meta/tree/main/pkg/coderr)) unwrapped by `errors.Cause` to client on service level.

### Example

`errors.go` in the package `server`:

```golang
var ErrStartEtcd        = coderr.NewCodeError(coderr.Internal, "start embed etcd")
var ErrStartEtcdTimeout = coderr.NewCodeError(coderr.Internal, "start etcd server timeout")
```

`server.go` in the package `server`:

```go
func (srv *Server) startEtcd() error {
    etcdSrv, err := embed.StartEtcd(srv.etcdCfg)
    if err != nil {
        return ErrStartEtcd.WithCause(err)
    }

    newCtx, cancel := context.WithTimeout(srv.ctx, srv.cfg.EtcdStartTimeout())
    defer cancel()

    select {
    case <-etcdSrv.Server.ReadyNotify():
    case <-newCtx.Done():
        return ErrStartEtcdTimeout.WithMessagef("timeout is:%v", srv.cfg.EtcdStartTimeout())
    }
 
    return nil
}
```

`main.go` in the package `main`:

```go
func main() {
    err := srv.startEtcd()
    if err != nil {
        return 
    }
    if coderr.Is(err, coderr.Internal) {
        log.Error("internal error")
    }
 
    _, ok := err.(coderr.CodeError)
    if ok {
        log.Error("found a CodeError") 
    } else {
        log.Error("not a CodeError) 
    }
  
    return
}
```

## Logging

### Principles

- Structured log by [zap](https://github.com/uber-go/zap).
- Use the package `github.com/horaemeta/pkg/log` which is based on [zap](https://github.com/uber-go/zap).
- Create local logger with common fields if necessary.

### Example

Normal usage:

```go
import "github.com/horaemeta/pkg/log"

func main() {
  if err := srv.Run(); err != nil {
    log.Error("fail to run server", zap.Error(err))
    return
  }
}
```

Local logger:

```go
import "github.com/horaemeta/pkg/log"

type lease struct {
    ID int64
    logger *zap.Logger
}

func NewLease(ID int64) *lease {
    logger := log.With(zap.Int64("lease-id", ID))
 
    return &lease {
        ID,
        logger,
    }
}

func (l *lease) Close() {
    l.logger.Info("lease is closed")
    l.ID = 0
}
```
