package flatjsonl

import (
	"log"
	"os"
	"runtime/pprof"

	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/bool64/dev/version"
	"github.com/swaggest/assertjson/json5"
	"gopkg.in/yaml.v3"
)

// Main is the entry point for flatjsonl CLI tool.
func Main() { //nolint:cyclop
	var (
		cpuProfile    = kingpin.Flag("dbg-cpu-prof", "Write CPU profile to file.").String()
		memProfile    = kingpin.Flag("dbg-mem-prof", "Write mem profile to file.").String()
		loopInputSize = kingpin.Flag("dbg-loop-input-size", "(benchmark) Repeat input until total target size reached, bytes.").Int()
	)

	kingpin.Version(version.Info().Version)

	f := Flags{}

	f.Register()

	f.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
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
		kingpin.Usage()

		return
	}

	if *loopInputSize > 0 {
		i, err := LoopReaderFromFile(inputs[0].FileName, *loopInputSize)
		if err != nil {
			log.Fatalf("failed to init loop reader: %v", err)
		}

		inputs[0].Reader = i
		inputs[0].FileName = ""
	}

	var cfg Config

	if f.Config != "" {
		b, err := os.ReadFile(f.Config)
		if err != nil {
			log.Fatalf("failed to read config file: %v", err)
		}

		yerr := yaml.Unmarshal(b, &cfg)
		if yerr != nil {
			err = json5.Unmarshal(b, &cfg)
			if err != nil {
				log.Fatalf("failed to decode config file: json5: %v, yaml: %v", err, yerr)
			}
		}
	}

	proc := NewProcessor(f, cfg, inputs...)

	if err := proc.Process(); err != nil {
		log.Fatal(err)
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
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
