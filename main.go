package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"

	"github.com/bool64/dev/version"
	"github.com/swaggest/assertjson/json5"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

func main() {
	var showVersion bool

	f := flatjsonl.Flags{}

	f.Register()
	flag.BoolVar(&showVersion, "version", false, "Show version and exit.")
	f.Parse()

	if showVersion {
		fmt.Println(version.Info().Version)

		return
	}

	if f.CPUProfile != "" {
		f, err := os.Create(f.CPUProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if f.MemProfile != "" {
		f, err := os.Create(f.MemProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}

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

		err = json5.Unmarshal(b, &cfg)
		if err != nil {
			log.Fatalf("failed to decode config file: %v", err)
		}
	}

	proc := flatjsonl.NewProcessor(f, cfg, inputs)

	if err := proc.Process(); err != nil {
		log.Fatal(err)
	}
}
