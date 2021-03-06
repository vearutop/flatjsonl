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
	var (
		showVersion bool
		cpuProfile  string
		memProfile  string
	)

	f := flatjsonl.Flags{}

	f.Register()
	flag.BoolVar(&showVersion, "version", false, "Show version and exit.")
	flag.StringVar(&cpuProfile, "cpu-prof", "", "Write CPU profile to file.")
	flag.StringVar(&memProfile, "mem-prof", "", "Write mem profile to file.")

	f.Parse()

	if showVersion {
		fmt.Println(version.Info().Version)

		return
	}

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err)
		}

		if err = pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}

		defer pprof.StopCPUProfile()
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

	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			log.Fatal(err)
		}

		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal(err)
		}

		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}
}
