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
		canal.DialPassword("wtf"),
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
