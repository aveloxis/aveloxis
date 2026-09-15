// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

const sccStreamWorkDir = "/tmp/aveloxis-analysis/repo_144636_1789431201538900741"

// collectStream runs streamSCCLabor over r and gathers the emitted rows.
func collectStream(t *testing.T, r io.Reader, now time.Time) ([]*db.RepoLaborRow, error) {
	t.Helper()
	var rows []*db.RepoLaborRow
	err := streamSCCLabor(json.NewDecoder(r), sccStreamWorkDir, now, func(row *db.RepoLaborRow) {
		rows = append(rows, row)
	})
	return rows, err
}

// unmarshalReference is the pre-v0.29.14 implementation: read everything,
// json.Unmarshal, then convert. It is kept HERE (and only here) as the
// parity oracle — streamSCCLabor must produce byte-identical rows. If the
// two ever diverge the streaming rewrite changed observable behavior,
// which it must not.
func unmarshalReference(t *testing.T, doc []byte, now time.Time) []*db.RepoLaborRow {
	t.Helper()
	var languages []sccLanguage
	if err := json.Unmarshal(doc, &languages); err != nil {
		t.Fatalf("reference unmarshal: %v", err)
	}
	var rows []*db.RepoLaborRow
	for _, lang := range languages {
		for _, file := range lang.Files {
			relPath, relErr := filepath.Rel(sccStreamWorkDir, file.Location)
			if relErr != nil || relPath == "" {
				relPath = file.Location
			}
			rows = append(rows, &db.RepoLaborRow{
				CloneDate:    now,
				AnalysisDate: now,
				Language:     lang.Name,
				FilePath:     relPath,
				FileName:     filepath.Base(file.Location),
				TotalLines:   file.Lines,
				CodeLines:    file.Code,
				CommentLines: file.Comment,
				BlankLines:   file.Blank,
				Complexity:   file.Complexity,
			})
		}
	}
	return rows
}

// sccDoc builds a document shaped like `scc -f json --by-file`, including
// the fields our struct ignores, so the decoder is exercised against the
// real key set rather than a trimmed fixture.
func sccDoc(langs ...string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, name := range langs {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"Name":%q,"Bytes":123,"CodeBytes":0,"Lines":10,"Code":8,"Comment":1,"Blank":1,"Complexity":2,"Count":2,"WeightedComplexity":0,"Files":[`, name)
		for j := range 2 {
			if j > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"Language":%q,"PossibleLanguages":[%q],"Filename":"f%d.go","Extension":"go","Location":%q,"Symlocation":"","Bytes":50,"Lines":%d,"Code":%d,"Comment":%d,"Blank":%d,"Complexity":%d,"WeightedComplexity":0,"Hash":null,"Binary":false,"Minified":false,"Generated":false,"EndPoint":0,"Uloc":0}`,
				name, name, j, sccStreamWorkDir+fmt.Sprintf("/pkg/%s/f%d.go", name, j),
				100+j, 80+j, 10+j, 5+j, 3+j)
		}
		b.WriteString(`],"LineLength":null,"ULOC":0}`)
	}
	b.WriteByte(']')
	return b.String()
}

func TestStreamSCCLaborMatchesUnmarshalReference(t *testing.T) {
	doc := sccDoc("Go", "YAML", "Markdown")
	now := time.Now()

	want := unmarshalReference(t, []byte(doc), now)
	got, err := collectStream(t, strings.NewReader(doc), now)
	if err != nil {
		t.Fatalf("streamSCCLabor: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("row count: stream=%d reference=%d", len(got), len(want))
	}
	for i := range want {
		if *got[i] != *want[i] {
			t.Fatalf("row %d diverged from the pre-v0.29.14 behavior:\n stream    = %+v\n reference = %+v", i, *got[i], *want[i])
		}
	}
	if len(got) == 0 {
		t.Fatal("fixture produced no rows — the parity assertion would be vacuous")
	}
}

// TestStreamSCCLaborIsIncremental is the behavioral pin for the fix: rows
// must be produced while scc is still writing. A reader that hands over
// the first language object and then blocks (no EOF) must still yield
// that language's rows. Reintroducing "buffer the whole document, then
// parse" — under any name — hangs this test instead of quietly restoring
// the allocation that killed the scheduler on 2026-09-15.
func TestStreamSCCLaborIsIncremental(t *testing.T) {
	doc := sccDoc("Go", "YAML")
	split := strings.Index(doc, `,{"Name":"YAML"`)
	if split < 0 {
		t.Fatal("fixture shape changed — cannot split between language objects")
	}

	release := make(chan struct{})
	r := &blockingReader{head: []byte(doc[:split]), tail: []byte(doc[split:]), release: release}

	var emitted atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- streamSCCLabor(json.NewDecoder(r), sccStreamWorkDir, time.Now(), func(*db.RepoLaborRow) {
			emitted.Add(1)
		})
	}()

	deadline := time.After(5 * time.Second)
	for emitted.Load() < 2 {
		select {
		case err := <-done:
			t.Fatalf("decoder finished before the reader was released (err=%v) — it consumed the whole stream before emitting", err)
		case <-deadline:
			t.Fatalf("no rows emitted while scc was still writing: got %d, want >= 2 — the decoder is buffering the whole document", emitted.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("streamSCCLabor after release: %v", err)
	}
	if n := emitted.Load(); n != 4 {
		t.Fatalf("emitted %d rows, want 4", n)
	}
}

// blockingReader serves head, then blocks until release is closed, then
// serves tail and EOF. It models scc still running.
type blockingReader struct {
	head, tail []byte
	release    chan struct{}
	blocked    bool
}

func (b *blockingReader) Read(p []byte) (int, error) {
	if len(b.head) > 0 {
		n := copy(p, b.head)
		b.head = b.head[n:]
		return n, nil
	}
	if !b.blocked {
		b.blocked = true
		<-b.release
	}
	if len(b.tail) > 0 {
		n := copy(p, b.tail)
		b.tail = b.tail[n:]
		return n, nil
	}
	return 0, io.EOF
}

// TestStreamSCCLaborKeyOrderIndependent: scc 3.7.0 emits "Name" before
// "Files", and the streaming path depends on that to emit rows as it
// goes. If a future scc reverses the order the rows must still carry the
// right language — buffered for that one object and stamped at close.
func TestStreamSCCLaborKeyOrderIndependent(t *testing.T) {
	doc := `[{"Files":[
		{"Location":"` + sccStreamWorkDir + `/a.go","Lines":10,"Code":8,"Comment":1,"Blank":1,"Complexity":2},
		{"Location":"` + sccStreamWorkDir + `/b.go","Lines":20,"Code":18,"Comment":1,"Blank":1,"Complexity":4}
	],"Bytes":99,"Name":"Go"}]`

	rows, err := collectStream(t, strings.NewReader(doc), time.Now())
	if err != nil {
		t.Fatalf("streamSCCLabor: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for i, row := range rows {
		if row.Language != "Go" {
			t.Errorf("row %d: Language = %q, want %q — a language name arriving after its Files was dropped", i, row.Language, "Go")
		}
	}
}

func TestStreamSCCLaborEmptyAndSparse(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want int
	}{
		{"empty document", `[]`, 0},
		{"language with no files", `[{"Name":"Go","Files":[]}]`, 0},
		{"null files", `[{"Name":"Go","Files":null}]`, 0},
		{"file missing optional keys", `[{"Name":"Go","Files":[{"Location":"` + sccStreamWorkDir + `/x.go"}]}]`, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := collectStream(t, strings.NewReader(tc.doc), time.Now())
			if err != nil {
				t.Fatalf("streamSCCLabor(%s): %v", tc.doc, err)
			}
			if len(rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rows), tc.want)
			}
		})
	}
}

// TestStreamSCCLaborRejectsMalformed: a truncated or non-array stream
// must surface an error rather than silently yielding a short snapshot.
// A short snapshot would be written as the current truth and rotate the
// real one into history (the v0.27.7 replace contract), so "parsed what
// it could" is data loss, not resilience.
func TestStreamSCCLaborRejectsMalformed(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{"not an array", `{"Name":"Go"}`},
		{"truncated mid-file", `[{"Name":"Go","Files":[{"Location":"/tmp/a.go","Lines":1`},
		{"truncated after language", `[{"Name":"Go","Files":[]}`},
		{"garbage", `not json at all`},
		{"wrong type for a numeric field", `[{"Name":"Go","Files":[{"Location":"/w/a.go","Lines":"not-a-number"}]}]`},
		{"file entry is not an object", `[{"Name":"Go","Files":["a.go"]}]`},
		{"Files is not an array", `[{"Name":"Go","Files":42}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := collectStream(t, strings.NewReader(tc.doc), time.Now()); err == nil {
				t.Fatalf("streamSCCLabor(%q) returned nil error — a malformed scan must not be written as a fresh snapshot", tc.doc)
			}
		})
	}
}

// TestStreamSCCLaborRelPathFallback pins the path handling the pre-fix
// code had: a Location outside workDir keeps its absolute path rather
// than becoming a ../../.. walk.
func TestStreamSCCLaborRelPathFallback(t *testing.T) {
	doc := `[{"Name":"Go","Files":[{"Location":"relative/elsewhere.go","Lines":1,"Code":1}]}]`
	rows, err := collectStream(t, strings.NewReader(doc), time.Now())
	if err != nil {
		t.Fatalf("streamSCCLabor: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	want := unmarshalReference(t, []byte(doc), rows[0].CloneDate)
	if rows[0].FilePath != want[0].FilePath {
		t.Errorf("FilePath = %q, want %q (must match the pre-v0.29.14 filepath.Rel fallback)", rows[0].FilePath, want[0].FilePath)
	}
}

// TestStreamSCCLaborDoesNotLeakFieldsBetweenFiles pins the reset of the
// reused sccFile. streamSCCFiles decodes every entry into ONE struct to
// avoid a per-file allocation; without the `f = sccFile{}` reset, a key
// absent from entry N silently inherits entry N-1's value, which would
// write another file's line counts into this file's labor row.
func TestStreamSCCLaborDoesNotLeakFieldsBetweenFiles(t *testing.T) {
	doc := `[{"Name":"Go","Files":[
		{"Location":"` + sccStreamWorkDir + `/first.go","Lines":999,"Code":888,"Comment":777,"Blank":666,"Complexity":555},
		{"Location":"` + sccStreamWorkDir + `/second.go"}
	]}]`

	rows, err := collectStream(t, strings.NewReader(doc), time.Now())
	if err != nil {
		t.Fatalf("streamSCCLabor: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	second := rows[1]
	if second.FilePath != "second.go" {
		t.Fatalf("row 1 FilePath = %q, want %q", second.FilePath, "second.go")
	}
	for _, f := range []struct {
		name string
		got  int
	}{
		{"TotalLines", second.TotalLines}, {"CodeLines", second.CodeLines},
		{"CommentLines", second.CommentLines}, {"BlankLines", second.BlankLines},
		{"Complexity", second.Complexity},
	} {
		if f.got != 0 {
			t.Errorf("second.go %s = %d, want 0 — the reused sccFile leaked first.go's value", f.name, f.got)
		}
	}
}

// scanSCCBody returns scanSCC's source with comments stripped, so the
// positional pins below cannot be satisfied by prose. srctest.FuncBody is
// THE extractor (SR-12) — it is AST-based, so it cannot anchor inside a
// doc comment that mentions the signature.
func scanSCCBody(t *testing.T) string {
	t.Helper()
	src := srctest.Read(t, "internal/collector/analysis.go")
	return srctest.StripGoComments(srctest.FuncBody(t, src, "func (ac *AnalysisCollector) scanSCC("))
}

// TestScanSCCFailsClosedOnSccError pins the ordering that keeps a failed
// scan from destroying a good snapshot. ReplaceRepoLaborSnapshot rotates
// the previous snapshot into history and inserts the new one in one
// transaction (v0.27.7), so calling it with the rows of a scan that DIED
// would replace real data with a partial one. Both failure checks —
// the decode error and scc's exit status — must therefore return before
// the store is ever reached.
//
// Positional pin, same shape as TestReplaceRepoLaborSnapshotRotatesFirst
// in internal/db: it asserts the control flow, not the presence of a
// token, so deleting a check fails it rather than renaming one.
func TestScanSCCFailsClosedOnSccError(t *testing.T) {
	body := scanSCCBody(t)

	replace := strings.Index(body, "ReplaceRepoLaborSnapshot(ctx, repoID, laborRows)")
	if replace < 0 {
		t.Fatal("scanSCC no longer calls ReplaceRepoLaborSnapshot with the accumulated rows")
	}

	// The drain must precede the reap. streamSCCLabor stops at the
	// top-level "]", so scc can still hold unread bytes; cmd.Wait() on an
	// undrained pipe blocks FOREVER once they exceed the 64 KiB pipe
	// buffer and the collection worker is lost. Measured: 60,000 trailing
	// bytes returned, 100,000 wedged. TestScanSCCDrainsTrailingOutput is
	// the runtime proof; this pins the ordering that makes it work.
	drain := strings.Index(body, "io.Copy(trailing, io.MultiReader(dec.Buffered(), counted))")
	reap := strings.Index(body, "waitErr := cmd.Wait()")
	if drain < 0 {
		t.Fatal("scanSCC must drain scc's pipe — without it any trailing output past 64 KiB wedges cmd.Wait() forever")
	}
	if reap < 0 {
		t.Fatal("scanSCC must reap scc via cmd.Wait()")
	}
	if drain > reap {
		t.Error("scanSCC reaps scc BEFORE draining the pipe — cmd.Wait() then blocks forever on trailing output and the worker is lost")
	}

	// A decode failure must ABANDON the scan, not read it to the end: the
	// report may have minutes left, and none of it can help.
	killBlock := strings.Index(body, "if decodeErr != nil {")
	if killBlock < 0 {
		t.Fatal("scanSCC must handle the decode error explicitly")
	}
	if killBlock > drain || !strings.Contains(body[killBlock:drain], "cancelSCC()") {
		t.Error("the decode-error path must kill scc BEFORE the drain — otherwise a doomed scan is read to completion first")
	}

	// Each guard must sit before the snapshot write AND actually leave the
	// function. A guard that keeps its position but loses its `return`
	// still reads correctly and still destroys the snapshot, so position
	// alone is not the property worth pinning.
	//
	// "if decodeErr != nil {" appears TWICE — the kill above and the
	// classify-and-return below — so the guard check takes the LAST one.
	for _, g := range []struct {
		name, open, whatBreaks string
		last                   bool
	}{
		{name: "scc exit status", open: "if waitErr != nil {",
			whatBreaks: "a failed scan would rotate the good snapshot into history and install a partial one"},
		{name: "decode error", open: "if decodeErr != nil {", last: true,
			whatBreaks: "a truncated scan would overwrite the good snapshot with whatever parsed"},
		{name: "trailing data", open: "if trailing.n > 0 {",
			whatBreaks: "garbage after the top-level array would be accepted as a complete report, which json.Unmarshal rejected before the streaming rewrite"},
	} {
		at := strings.Index(body, g.open)
		if g.last {
			at = strings.LastIndex(body, g.open)
		}
		if at < 0 {
			t.Fatalf("scanSCC no longer guards on %s (%q)", g.name, g.open)
		}
		if at > replace {
			t.Errorf("scanSCC checks %s AFTER replacing the snapshot — %s", g.name, g.whatBreaks)
			continue
		}
		guard := body[at:]
		if end := strings.Index(guard, "\n\t}"); end >= 0 {
			guard = guard[:end]
		}
		if !strings.Contains(guard, "return") {
			t.Errorf("the %s guard does not return — execution falls through to the snapshot write and %s", g.name, g.whatBreaks)
		}
	}
}

// TestScanSCCDoesNotBufferWholeReport is the negative half of the
// streaming contract. TestStreamSCCLaborIsIncremental proves the decoder
// streams; this proves scanSCC actually hands it the live pipe rather
// than a filled buffer, which is the wiring the incident turned on.
func TestScanSCCDoesNotBufferWholeReport(t *testing.T) {
	body := scanSCCBody(t)
	if !strings.Contains(body, "cmd.StdoutPipe()") {
		t.Error("scanSCC must read scc's report from a pipe — see the 2026-09-15 OOM")
	}
	for _, banned := range []string{"cmd.Stdout =", "cmd.Output()", "cmd.CombinedOutput()", "io.ReadAll"} {
		if strings.Contains(body, banned) {
			t.Errorf("scanSCC uses %q — that buffers scc's whole report, the allocation shape that OOM-killed the scheduler on 2026-09-15 (repo 144636, 1 GiB buffer doubling to 2 GiB under vm.overcommit_memory=2)", banned)
		}
	}
}

// benchSCCDoc builds a report with n files spread over 10 languages, with
// production-length Location paths (the analysis clone's temp prefix
// dominates the per-entry size, so a short fixture understates the JSON
// by ~2x and would flatter both implementations equally but unrealistically).
func benchSCCDoc(n int) []byte {
	langs := []string{"YAML", "Go", "Markdown", "Python", "Shell", "JSON", "C", "Rust", "Java", "HTML"}
	var b strings.Builder
	b.WriteByte('[')
	per := n / len(langs)
	for li, name := range langs {
		if li > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"Name":%q,"Bytes":1,"CodeBytes":0,"Lines":1,"Code":1,"Comment":0,"Blank":0,"Complexity":1,"Count":%d,"WeightedComplexity":0,"Files":[`, name, per)
		for i := range per {
			if i > 0 {
				b.WriteByte(',')
			}
			loc := fmt.Sprintf("%s/components/odh-operator/%064d/charts/dashboard-operator/templates/file%d.yaml", sccStreamWorkDir, i, i)
			fmt.Fprintf(&b, `{"Language":%q,"PossibleLanguages":[%q],"Filename":"file%d.yaml","Extension":"yaml","Location":%q,"Symlocation":"","Bytes":900,"Lines":%d,"Code":%d,"Comment":1,"Blank":1,"Complexity":1,"WeightedComplexity":0,"Hash":null,"Binary":false,"Minified":false,"Generated":false,"EndPoint":0,"Uloc":0}`,
				name, name, i, loc, 100+i, 80+i)
		}
		b.WriteString(`],"LineLength":null,"ULOC":0}`)
	}
	b.WriteByte(']')
	return []byte(b.String())
}

// BenchmarkStreamSCCLabor measures the decode path that replaced the
// buffered one in v0.29.14. The number that matters is B/op: the pre-fix
// implementation allocated ~9.7x this for the same rows (3,848 MB vs
// 397 MB at 1M files) because it held the raw JSON, the decoded structs
// and the rows simultaneously, and grew the raw buffer by doubling — a
// 2 GiB contiguous request that the kernel refused. If B/op ever climbs
// back toward the document size, the buffering is back.
func BenchmarkStreamSCCLabor(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		doc := benchSCCDoc(n)
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			now := time.Now()
			b.SetBytes(int64(len(doc)))
			b.ReportAllocs()
			for b.Loop() {
				var rows []*db.RepoLaborRow
				err := streamSCCLabor(json.NewDecoder(bytes.NewReader(doc)), sccStreamWorkDir, now, func(r *db.RepoLaborRow) {
					rows = append(rows, r)
				})
				if err != nil {
					b.Fatal(err)
				}
				if len(rows) == 0 {
					b.Fatal("no rows")
				}
			}
		})
	}
}
