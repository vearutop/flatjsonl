package flatjsonl

import (
	"fmt"
	"html"
	"log"
	"net/http"
	httppprof "net/http/pprof"
	"runtime"
	"time"
)

func startHTTPStatusServer(addr string, proc *Processor) {
	mux := http.NewServeMux()
	startedAt := time.Now()

	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		m := runtime.MemStats{}
		runtime.ReadMemStats(&m)
		progressStatus := "idle"
		progressUpdated := ""

		if proc != nil {
			if s, updatedAt := proc.ProgressStatus(); s != "" {
				progressStatus = s
				progressUpdated = updatedAt.Format(time.RFC3339)
			}
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprintf(w, `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>flatjsonl status</title>
<style>
body { font-family: monospace; margin: 2rem; max-width: 72rem; }
table { border-collapse: collapse; }
td { padding: 0.2rem 0.8rem 0.2rem 0; vertical-align: top; }
</style>
</head>
<body>
<h1>flatjsonl status</h1>
<p>uptime: %s</p>
<p><a href="/debug/pprof/">/debug/pprof/</a></p>
<table>
<tr><td>goroutines</td><td>%d</td></tr>
<tr><td>heap alloc</td><td>%s</td></tr>
<tr><td>heap in use</td><td>%s</td></tr>
<tr><td>heap sys</td><td>%s</td></tr>
<tr><td>heap objects</td><td>%d</td></tr>
<tr><td>stack in use</td><td>%s</td></tr>
<tr><td>next gc</td><td>%s</td></tr>
<tr><td>gc cycles</td><td>%d</td></tr>
</table>
<h2>progress</h2>
<p>updated: %s</p>
<pre>%s</pre>
</body>
</html>`,
			time.Since(startedAt).Truncate(time.Second),
			runtime.NumGoroutine(),
			formatBytes(m.HeapAlloc),
			formatBytes(m.HeapInuse),
			formatBytes(m.HeapSys),
			m.HeapObjects,
			formatBytes(m.StackInuse),
			formatBytes(m.NextGC),
			m.NumGC,
			html.EscapeString(progressUpdated),
			html.EscapeString(progressStatus),
		)
	})

	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	for _, name := range []string{"allocs", "block", "goroutine", "heap", "mutex", "threadcreate"} {
		mux.Handle("/debug/pprof/"+name, httppprof.Handler(name))
	}

	go func() {
		log.Printf("http status listening on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("http status server stopped: %v", err)
		}
	}()
}

func formatBytes(v uint64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%d B", v)
	}

	div, exp := uint64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(v)/float64(div), "KMGTPE"[exp])
}
