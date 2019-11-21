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
	log.Printf("[PRINTER] cmd=%v\n", cmd)
	return nil
}

func main() {
	log.SetOutput(os.Stdout)

	cfg, err := canal.NewConfig(
		"10.200.10.19:7003",
		// "127.0.0.1:6379",
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
