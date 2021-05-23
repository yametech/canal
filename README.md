# Canal 　　　　　　　　　　　　　　　　　　　　　　[中文](README_zh.md)

[![Build Status](https://github.com/yametech/canal/workflows/canal/badge.svg?event=push&branch=master)](https://github.com/yametech/canal/actions?workflow=canal)
[![Go Report Card](https://goreportcard.com/badge/github.com/yametech/canal)](https://goreportcard.com/report/github.com/yametech/canal)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://github.com/yametech/canal/blob/master/LICENSE)

## Introduction

 Canal supports redis 2.x to 5.x and forward compatible replication tools with hybrid (rdb + aof) protocol

 ## Scenes

* Redis data synchronization across computer rooms
* Heterogeneous data migration; such as Redis to mysql, MQ, ES, etc.

## Design

Simulate the redis slave, then go to dump the rdb and aof of the redis master (add the architecture design diagram later)

## Features

* Support redis 2.x to 5.x data synchronization
* Support full synchronization and incremental synchronization (continued resume)
* Support failover
* Faster

## Company Internal use

* 2k + redis instance data synchronization

### Usage

```go

go get github.com/yametech/canal

```

### Basic Usage

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
		"127.0.0.1:6379",
		canal.DialKeepAlive(time.Hour*16800),
		canal.DialWithLocalPort(6379), // use specified local port
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

### Use of breakpoint resume

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

### Failover usage

```go

//
type printer struct{}

func (p *printer) Command(cmd *canal.Command) error {
	log.Printf("[PRINTER] cmd=%s\n", cmd.String())
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

- [ ] Support c / s structure, grpc cross platform use
- [ ] redis 6.x
- [ ] Support etcd, zk, consul and other storage position
- [ ] Support cluster
- [ ] Automatic maintenance of redis topology structure

