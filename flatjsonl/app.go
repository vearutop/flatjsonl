package flatjsonl

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"github.com/bool64/dev/version"
)

// Main is the entry point for flatjsonl CLI tool.
func Main() {
	var (
		showVersion   bool
		cpuProfile    string
		memProfile    string
		loopInputSize int
	)

	f := Flags{}

	f.Register()
	flag.BoolVar(&showVersion, "version", false, "Show version and exit.")
	flag.StringVar(&cpuProfile, "dbg-cpu-prof", "", "Write CPU profile to file.")
	flag.StringVar(&memProfile, "dbg-mem-prof", "", "Write mem profile to file.")
	flag.IntVar(&loopInputSize, "dbg-loop-input-size", 0,
		"(benchmark) Repeat input until total target size reached, bytes.")

	f.Parse()

	if showVersion {
		fmt.Println(version.Module("github.com/vearutop/flatjsonl").Version)

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

	if loopInputSize > 0 {
		i, err := LoopReaderFromFile(inputs[0].FileName, loopInputSize)
		if err != nil {
			log.Fatalf("failed to init loop reader: %v", err)
		}

		inputs[0].Reader = i
		inputs[0].FileName = ""
	}

	proc, err := New(f)
	if err != nil {
		log.Fatal(err)
	}

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
