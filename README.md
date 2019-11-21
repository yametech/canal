# canal
[![Build Status](https://github.com/yametech/canal/workflows/canal/badge.svg?event=push&branch=master)](https://github.com/yametech/canal/actions?workflow=canal)
redis-canal support redis 5.0.5 and backward compatibility with mixed protocol replication

### Simple Example

```go
go get github.com/yametech/canal
```

```go
// start a new replication

package main

import (
	"github.com/yametech/canal"
	"log"
	"os"
	"time"
)

type printer struct{}

func (p *printer) Command(cmd *canal.Command) error {
	log.Printf("[PRINTER] cmd=%v\n", cmd)
	return nil
}

func main() {
	log.SetOutput(os.Stdout)

	cfg, err := canal.NewConfig(
		"127.0.0.1:8888",
		canal.DialKeepAlive(time.Minute*5),
	)

	if err != nil {
		panic(err)
	}

	repl, err := canal.NewCanal(cfg)
	if err != nil {
		panic(err)
	}

	defer repl.Close()

	if err := repl.Run(&printer{}); err != nil {
		panic(err)
	}
}

// starting from the location of an instance example
package main

import (
	"github.com/yametech/canal"
	"log"
	"os"
	"time"
)

type printer struct{}

func (p *printer) Command(cmd *canal.Command) error {
	log.Printf("[PRINTER] cmd=%v\n", cmd)
	return nil
}

func main() {
	log.SetOutput(os.Stdout)

	cfg, err := canal.NewConfig(
		"127.0.0.1:8888",
		canal.DialKeepAlive(time.Minute*5),
	)

	if err != nil {
		panic(err)
	}

	repl, err := canal.FromOffsetCanal(cfg, "0cc79e52c7cdcaa58535bb2ce23f46ee1343246c", 111)
	if err != nil {
		panic(err)
	}

	defer repl.Close()

	if err := repl.Run(&printer{}); err != nil {
		panic(err)
	}
}
```
