# canal

## 简介
[![Build Status](https://github.com/yametech/canal/workflows/canal/badge.svg?event=push&branch=master)](https://github.com/yametech/canal/actions?workflow=canal)

 canal 支持 redis 5.0.5 和向前兼容且 具有mixed协议的复制工具

 ## 场景

* Redis数据的跨机房同步
* 异构数据的迁移；比如Redis到mysql，MQ，ES等

## 设计

模拟redis slave,然后去dump redis master的rdb和aof

## 特性

* 支持redis 2.x 到 5.x的数据同步
* 支持断点续传
* 支持故障转移
* 更快

### 使用

```go
go get github.com/yametech/canal
```

### 基本使用

```go
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
```

### 断点续传使用

``` go

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

### 故障转移使用

```go

type printer struct{}

func (p *printer) Command(cmd *canal.Command) error {
	log.Printf("[PRINTER] cmd=%v\n", cmd)
	return nil
}

func main() {
	log.SetOutput(os.Stdout)

	cfg, err := canal.NewConfig(
		"127.0.0.1:6379",
		canal.DialKeepAlive(time.Hour*5),
	)

	if err != nil {
		panic(err)
	}

	repl, err := canal.FromOffsetCanal(cfg, "092f679803f3c0fed71f3dc5a28d18a21addb09a", 10315258513)
	if err != nil {
		panic(err)
	}

	defer repl.Close()

	if err := repl.Run(&printer{}); err != nil {
		log.Fatalf("error %s", err)
	}
}

```

## TODO

- [ ] redis6.x
- [ ] 支持etcd,zk,consul等存储位点
- [ ] 支持集群版本
- [ ] 支持自动维护redis Topology结构

