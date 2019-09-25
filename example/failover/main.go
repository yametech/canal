package main

import (
	"log"
	"os"
	"time"

	"github.com/laik/canal"
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
		"10.1.1.228:8001",
		// "127.0.0.1:6379",
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
