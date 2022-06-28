package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/vearutop/flatjsonl/flatjsonl"
)

func main() {
	f := flatjsonl.Flags{}

	f.Register()
	f.Parse()

	inputs := f.Inputs()
	if len(inputs) == 0 {
		flag.Usage()

		return
	}

	var cfg flatjsonl.Config

	if f.Config != "" {
		b, err := ioutil.ReadFile(f.Config)
		if err != nil {
			log.Fatalf("failed to read config file: %v", err)
		}

		err = json.Unmarshal(b, &cfg)
		if err != nil {
			log.Fatalf("failed to decode config file: %v", err)
		}
	}

	proc := flatjsonl.NewProcessor(f, cfg, inputs)

	if err := proc.Process(); err != nil {
		log.Fatal(err)
	}
}
