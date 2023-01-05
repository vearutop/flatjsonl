// Package main implements utility to check equality of two CSV files.
package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/vearutop/flatjsonl/flatjsonl"
)

func main() { //nolint
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: csvdiff <file1.csv> <file2.csv>")
		flag.PrintDefaults()

		return
	}

	fn1 := flag.Arg(0)
	fn2 := flag.Arg(1)

	f1, err := os.Open(fn1)
	if err != nil {
		log.Fatal(err)
	}

	f1s, err := os.Stat(fn1)
	if err != nil {
		log.Fatal(err)
	}

	f2, err := os.Open(fn2)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err1 := f1.Close()
		err2 := f2.Close()

		if err1 != nil || err2 != nil {
			log.Fatal(err1, err2)
		}
	}()

	f2s, err := os.Stat(fn1)
	if err != nil {
		log.Fatal(err)
	}

	cr1 := &flatjsonl.CountingReader{Reader: f1}
	c1 := csv.NewReader(cr1)
	c1.ReuseRecord = true

	cr2 := &flatjsonl.CountingReader{Reader: f2}
	c2 := csv.NewReader(cr2)
	c2.ReuseRecord = true

	var (
		keys1, keys2 []string
		m1           = map[string]string{}
		m2           = map[string]string{}
	)

	l := 0

	pr := flatjsonl.Progress{}

	pr.Start(f1s.Size()+f2s.Size(), func() int64 {
		return cr1.Bytes() + cr2.Bytes()
	}, "reading files")

	defer pr.Stop()

	for {
		l++

		r1, err := c1.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			log.Fatal("file1 reading: ", err)
		}

		r2, err := c2.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			log.Fatal("file2 reading: ", err)
		}

		if r1 == nil && r2 == nil {
			break
		}

		if r1 == nil {
			log.Fatal("too few lines in file1")
		}

		if r2 == nil {
			log.Fatal("too few lines in file2")
		}

		if keys1 == nil {
			keys1 = append(keys1, r1...)
			keys2 = append(keys2, r2...)

			continue
		}

		for i, v := range r1 {
			m1[keys1[i]] = v
		}

		for i, v := range r2 {
			m2[keys2[i]] = v
		}

		diff := mapDiff(m1, m2)

		if len(diff) > 0 {
			log.Fatalln("found diff in line ", l, ":\n"+strings.Join(diff, "\n"))
		}
	}

	println("files are equal")
}

func mapDiff(m1, m2 map[string]string) []string {
	var diff []string //nolint:prealloc

	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok {
			diff = append(diff, "missing "+k+":"+v1+" in file2")
		}

		if v1 != v2 {
			diff = append(diff, "changed "+k+" // "+v1+" // "+v2)
		}

		delete(m2, k)
	}

	for k, v2 := range m2 {
		diff = append(diff, "missing "+k+":"+v2+" in file1")
	}

	return diff
}
