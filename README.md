# canal
redis-canal

```go
go get github.com/laik/canal
```


### Simple Example
```go
package main

import (
	"github.com/laik/canal"
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
```


### FromOffset Example

```go
package main

import (
	"github.com/laik/canal"
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