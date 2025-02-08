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

	"github.com/bool64/progress"
)

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: csvequals <file1.csv> <file2.csv>")
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

	cr1 := progress.NewCountingReader(f1)
	c1 := csv.NewReader(cr1)
	c1.ReuseRecord = true

	cr2 := progress.NewCountingReader(f2)
	c2 := csv.NewReader(cr2)
	c2.ReuseRecord = true

	var (
		keys1, keys2 []string
		m1           = map[string]string{}
		m2           = map[string]string{}
	)

	l := 0

	pr := progress.Progress{}
	tot := f1s.Size() + f2s.Size()

	pr.Start(func(t *progress.Task) {
		t.Task = "reading files"
		t.TotalBytes = func() int64 {
			return tot
		}
		t.CurrentBytes = func() int64 {
			return cr1.Bytes() + cr2.Bytes()
		}
		t.CurrentLines = func() int64 {
			return cr1.Lines() + cr2.Lines()
		}
	})

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

	cr1.Close()
	cr2.Close()

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
