# Canal 　　　　　　　　　　　　　　　　　　　　　　[English](README.md)

## 简介
[![Build Status](https://github.com/yametech/canal/workflows/canal/badge.svg?event=push&branch=master)](https://github.com/yametech/canal/actions?workflow=canal)
[![Go Report Card](https://goreportcard.com/badge/github.com/yametech/canal)](https://goreportcard.com/report/github.com/yametech/canal)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://github.com/yametech/canal/blob/master/LICENSE)


 canal 支持到redis2.x到 5.x 和向前兼容且具有mixed(rdb+aof)协议的复制工具

 ## 场景

* Redis数据的跨机房同步
* 异构数据的迁移；比如Redis到mysql，MQ，ES等

## 设计

模拟redis slave,然后去dump redis master的rdb和aof（后续补上架构设计图）

## 特性

* 支持redis 2.x 到 5.x的数据同步
* 支持全量同步和增量同步(断点续传)
* 支持故障转移
* 更快

## 内部使用情况

* 2k+ redis实例数据同步,基本跑满网卡。

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

//
type printer struct{}

func (p *printer) Command(cmd *canal.Command) error {
	// log.Printf("[PRINTER] cmd=%s\n", cmd.String())
	return nil
}

func main() {
	log.SetOutput(os.Stdout)

	cfg, err := canal.NewConfig(
		"127.0.0.1:6379",
		canal.DialKeepAlive(time.Minute*5),
		// canal.DialPassword(""),
	)
	if err != nil {
		panic(err)
	}
	// 自动找主
	cfg.ReplMaster()

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

## TODO

- [ ] 支持c/s结构,grpc跨平台使用
- [ ] redis6.x
- [ ] 支持etcd,zk,consul等存储位点
- [ ] 支持集群版本
- [ ] 支持自动维护redis Topology结构

