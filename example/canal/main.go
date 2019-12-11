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
		// "10.1.1.228:8001",
		"127.0.0.1:6379",
		// "127.0.0.1:6379",
		// "127.0.0.1:6377",
		canal.DialKeepAlive(time.Hour*16800),
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
