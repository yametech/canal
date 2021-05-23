/*
Copyright 2019 yametech.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"log"
	"os"
	"time"

	"github.com/yametech/canal"
)

// update trigger

type printer struct{}

var x int64
var last int64

func (p *printer) Command(cmd *canal.Command) error {
	//log.Printf("[PRINTER] cmd=%s\n", cmd)
	x++

	return nil
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	tk := time.NewTicker(1 * time.Second)
	go func() {
		for range tk.C {
			log.Printf("rec  %d current %d", x, x-last)
			last = x
		}
	}()

	cfg, err := canal.NewConfig(
		"10.200.100.200:6379",
		canal.DialKeepAlive(time.Hour*16800),
		canal.DialWithLocalPort(6379),
		// canal.DialReadTimeout(time.Second*300),
		// canal.DialWriteTimeout(time.Second*300),
		// canal.DialPassword("wtf"),
	)

	if err != nil {
		log.Printf("%s", err)
		os.Exit(1)
	}

	repl, err := canal.NewCanal(cfg)
	if err != nil {
		log.Printf("%s", err)
		os.Exit(1)
	}

	defer repl.Close()

	if err := repl.Run(&printer{}); err != nil {
		// panic("eof")
		log.Printf("%s", err)
		os.Exit(1)
		// panic(err) panic 当遇到io.EOF 时，panic不退出？ bug?
	}
}
