# flatjsonl

[![Build Status](https://github.com/vearutop/flatjsonl/workflows/test-unit/badge.svg)](https://github.com/vearutop/flatjsonl/actions?query=branch%3Amaster+workflow%3Atest-unit)
[![Coverage Status](https://codecov.io/gh/vearutop/flatjsonl/branch/master/graph/badge.svg)](https://codecov.io/gh/vearutop/flatjsonl)
[![GoDevDoc](https://img.shields.io/badge/dev-doc-00ADD8?logo=go)](https://pkg.go.dev/github.com/vearutop/flatjsonl)
[![Time Tracker](https://wakatime.com/badge/github/vearutop/flatjsonl.svg)](https://wakatime.com/badge/github/vearutop/flatjsonl)
![Code lines](https://sloc.xyz/github/vearutop/flatjsonl/?category=code)
![Comments](https://sloc.xyz/github/vearutop/flatjsonl/?category=comments)

`flatjsonl` renders structured logs as table.

## Why?

Logs, structured as [`JSON Lines`](https://jsonlines.org/) (and sometimes prefixed with non-JSON message), are very 
common source of information for ad-hoc analytics and investigations. 

They can be processed with `jq` and grepped for a variety of data checks, however there are much more powerful and 
convenient tools that operate on rows and columns, rather than hierarchical structures.

This tool converts structured logs into tabular data (`CSV`, `SQLite`, `PostgreSQL dump`) with flexible mapping options.

## Performance

Logs of busy systems tend to be large, so performance is important if you want the job done in reasonable time.

Thanks to [`github.com/valyala/fastjson`](https://github.com/valyala/fastjson),
[`github.com/puzpuzpuz/xsync`](https://github.com/puzpuzpuz/xsync) and concurrency-friendly design, 
`flatjsonl` can leverage multicore machines to a large extent and crunch data at high speed.

```
vearutop@bigassbox ~ $ time ~/flatjsonl -pg-dump ~/events.pg.sql.gz -input ~/events.log -sql-table events -progress-interval 1m
```
```
scanning keys...
scanning keys: 100.0% bytes read, 11396506 lines processed, 200806.2 l/s, 902.3 MB/s, elapsed 56.75s, remaining 0s, heap 44 MB
lines: 11396506 , keys: 310
flattening data...
flattening data: 20.7% bytes read, 2363192 lines processed, 39385.9 l/s, 177.0 MB/s, elapsed 1m0s, remaining 3m49s, heap 569 MB
flattening data: 41.7% bytes read, 4750006 lines processed, 39583.1 l/s, 177.9 MB/s, elapsed 2m0s, remaining 2m47s, heap 485 MB
flattening data: 62.7% bytes read, 7140289 lines processed, 39668.1 l/s, 178.3 MB/s, elapsed 3m0s, remaining 1m47s, heap 610 MB
flattening data: 83.6% bytes read, 9528709 lines processed, 39702.9 l/s, 178.4 MB/s, elapsed 4m0s, remaining 47s, heap 572 MB
flattening data: 100.0% bytes read, 11396506 lines processed, 39692.4 l/s, 178.4 MB/s, elapsed 4m47.12s, remaining 0s, heap 508 MB
lines: 11396506 , keys: 310

real    5m44.002s
user    53m24.841s
sys     1m1.772s
```
```
51G  events.log
3.6G events.pg.sql.gz
```



## How it works?

In simplest case this tool iterates log file two times, first pass to collect all available keys and 
second pass to actually fill the table with already known keys (columns).

During each pass, each line is decoded and traversed recursively.
Keys for nested elements are declared with dot-separated syntax (same as in `jq`), array indexes are enclosed in `[x]`, 
e.g. `.deeper.subProperty.[0].foo`.

String values are checked for JSON contents and are also traversed if JSON is found (with `-extract-strings` flag).

If `includeKeys` is not empty in [configuration file](#configuration-file), first pass is skipped.

## Install

```
go install github.com/vearutop/flatjsonl@latest
$(go env GOPATH)/bin/flatjsonl --help
```

Or download binary from [releases](https://github.com/vearutop/flatjsonl/releases).

### Linux AMD64

```
wget https://github.com/vearutop/flatjsonl/releases/latest/download/linux_amd64.tar.gz && tar xf linux_amd64.tar.gz && rm linux_amd64.tar.gz
./flatjsonl -version
```

### Macos Intel

```
wget https://github.com/vearutop/flatjsonl/releases/latest/download/darwin_amd64.tar.gz && tar xf darwin_amd64.tar.gz && rm darwin_amd64.tar.gz
codesign -s - ./flatjsonl
./flatjsonl -version
```

### Macos Apple Silicon (M1, etc...)

```
wget https://github.com/vearutop/flatjsonl/releases/latest/download/darwin_arm64.tar.gz && tar xf darwin_arm64.tar.gz && rm darwin_arm64.tar.gz
codesign -s - ./flatjsonl
./flatjsonl -version
```

## Usage

```
flatjsonl -help
```
```
Usage of flatjsonl:
  -add-sequence
        Add auto incremented sequence number.
  -buf-size int
        Buffer size (max length of file line) in bytes. (default 10000000)
  -case-sensitive-keys
        Use case-sensitive keys (can fail for SQLite).
  -concurrency int
        Number of concurrent routines in reader. (default 24)
  -config string
        Configuration JSON value, path to JSON5 or YAML file.
  -csv string
        Output to CSV file (gzip encoded if ends with .gz).
  -dbg-cpu-prof string
        Write CPU profile to file.
  -dbg-loop-input-size int
        (benchmark) Repeat input until total target size reached, bytes.
  -dbg-mem-prof string
        Write mem profile to file.
  -extract-strings
        Check string values for JSON content and extract when available.
  -field-limit int
        Max length of field value, exceeding tail is truncated, 0 for unlimited.
  -get-key string
        Add a single key to list of included keys.
  -input string
        Input from JSONL files, comma-separated.
  -key-limit int
        Max length of key, exceeding tail is truncated, 0 for unlimited.
  -match-line-prefix string
        Regular expression to capture parts of line prefix (preceding JSON).
  -max-lines int
        Max number of lines to process.
  -max-lines-keys int
        Max number of lines to process when scanning keys.
  -mem-limit int
        Heap in use soft limit, in MB. (default 1000)
  -offset-lines int
        Skip a number of first lines.
  -output string
        Output to a file (default <input>.csv).
  -pg-dump string
        Output to PostgreSQL dump file.
  -progress-interval duration
        Progress update interval. (default 5s)
  -raw string
        Output to RAW file (column values are written as is without escaping, gzip encoded if ends with .gz).
  -raw-delim string
        RAW file column delimiter.
  -replace-keys
        Use unique tail segment converted to snake_case as key.
  -show-keys-flat
        Show all available keys as flat list.
  -show-keys-hier
        Show all available keys as hierarchy.
  -show-keys-info
        Show keys, their replaces and types.
  -skip-zero-cols
        Skip columns with zero values.
  -sql-max-cols int
        Maximum columns in single SQL table (SQLite will fail with more than 2000). (default 500)
  -sql-table string
        Table name. (default "flatjsonl")
  -sqlite string
        Output to SQLite file.
  -verbosity int
        Show progress in STDERR, 0 disables status, 2 adds more metrics. (default 1)
  -version
        Show version and exit.
```

### Configuration file

```yaml
includeKeys:
  - ".key1"
  - ".key2"
  - "const:my-value"
  - ".keyGroup.[0].key3"
includeKeysRegex:
  - ".keyGroup.[1].*"
excludeKeys:
  - ".keyGroup.[1].notNeeded"
replaceKeys:
  ".key1": key1
  ".key2": created_at
parseTime:
  "._prefix.[1]": 2006/01/02 15:04:05.99999
outputTimeFormat: '2006-01-02 15:04:05'
outputTZ: UTC
concatDelimiter: "::"
extractValuesRegex:
  ".foo.link": "URL"
  ".*.nested": "JSON"
```

Parse time is a map of original key to time pattern. See https://pkg.go.dev/time#pkg-constants for pattern rules.

Output time format is used to write parsed timestamps.

List of `includeKeys` can also declare columns with constant values in form of `"const:<value>"`, `<value>` would
be used as column value.

Configuration file can also have [regexp replaces](https://pkg.go.dev/regexp#Regexp.ReplaceAllString) as a map of 
regular expression as keys and replace patterns as values.

It is also possible to use simplified syntax with `*`, where `*` means key segment (can not start with a digit) between two dots.

```json
{
  "replaceKeysRegex": {
    "^\\.foo\\.([^.]+)$": "f00_${1} VARCHAR(255)",
    ".foo.*.*": "f00_${2}_${1} VARCHAR(255)"
  }
}
```
This example would produce such transformation.
```
.foo.bar => f00_bar VARCHAR(255)
.foo.baz.qux => f00_qux_baz VARCHAR(255)
```

Regular expression replaces are applied to keys that have no matches in `replaceKeys`.

Regular expressions are checked in no particular order, when replaced key is different from original checks are 
stopped and replaced key is used.

Multiple regular expression could match and replace a key, this can lead to undefined behavior, to avoid it is 
recommended to use mutually exclusive expressions and match against full key by having `^` and `$` at the edges of exp.

If multiple keys are replaced into similar key, coalesce function is used for resulting column value, or if 
`concatDelimiter` is defined those values would be concatenated.

### Transposing data

In cases of dynamic arrays or objects, you may want to transpose the values as rows of separate tables instead of
columns of main table.

This is possible with `transpose` configuration file field ([example](./flatjsonl/testdata/transpose_cfg.json)), 
it accepts a map of key prefixes to transposed table name. During processing, values found in the prefixed keys would
be moved as multiple rows in transposed table.

### Extracting data from strings

With `extractValuesRegex` config parameter, you can set a map of `regexp` matching key name to value format. 
Currently `URL` and `JSON` are supported as formats. The string values in the matching keys would be decoded 
and exposed as JSON.

## Examples

Import data from `events.jsonl` as columns described in `events.json` config file to 
SQLite table `events` in file `report.sqlite`.

```
flatjsonl -sqlite report.sqlite -sql-table events -config events.json events.jsonl
```

Show flat list of keys found in first 100 (or less) lines of `events.jsonl`.
```
flatjsonl -max-lines 100 -show-keys-flat events.jsonl
```

Import data from `part1.log`, `part2.log`, `part3.log` into `part1.log.csv` with keys converted to snake_case 
unique tails and with columns matched from line prefix (for lines formatted as `<prefix> {<json>}`).
```
flatjsonl -match-line-prefix '([\w\d-]+) [\w\d]+ ([\d/]+\s[\d:\.]+)' -replace-keys part1.log part2.log part3.log
```

Extract a single column from JSONL log (equivalent to `cat huge.log | jq .foo.bar.baz > entries.log`), `flatjsonl` is optimized for multi-core processors, so it can bring perfromance improvement compared to single-threaded `jq`.
```
flatjsonl -input huge.log -raw entries.log -get-key ".foo.bar.baz"
```
