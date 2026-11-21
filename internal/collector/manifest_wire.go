// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
)

// The wire between an analysis and the child that reads its clone
// (0.29.213): the manifests, and the lockfiles. Frames, each a four-byte big-endian length and that many
// bytes of JSON, on the child's standard input (parent to child) and
// standard output (child to parent). The child's standard error carries
// nothing but a crash.
//
//	parent -> child   request      once, first
//	child  -> parent  log          a log record of the pass, as it happens
//	child  -> parent  pom          a parent or BOM pom is wanted
//	parent -> child   pom_answer   the answer to the pom just asked for
//	child  -> parent  part         a piece of a pass's result, any number
//	child  -> parent  done         once for each pass asked for, in the order
//	                               asked: that pass's result is complete
//
// The exchange is strictly in turn: the child reads only after it has asked
// for a pom, and one goroutine on each side reads and writes.

// manifestFrameLimit bounds one frame, each way: twice the largest build
// file (manifest.MaxBuildFileBytes). A frame carries a piece of a result,
// cut by the child at manifestPartTarget, or one fetched pom's extract
// (whose document was at most mavenPomBodyLimit, the same eight mebibytes);
// the factor of two is for the encoding. No item of a result is too large
// for a frame: one longer than manifestItemLimit is not sent, and the file
// it stands in is recorded as one that could not be read, by name
// (0.29.214; this comment said so before, and the child instead exited with
// the protocol status and said nothing).
const manifestFrameLimit = 2 * manifest.MaxBuildFileBytes

// manifestPartTarget is the size at which the child cuts a piece of the
// result: a sixteenth of the frame limit, so that a piece and the item that
// carried it over the target fit a frame many times over.
const manifestPartTarget = manifestFrameLimit / 16

var (
	errManifestFrameTooLarge = fmt.Errorf("a frame is larger than the limit of %d bytes", manifestFrameLimit)
	errManifestFrameShort    = errors.New("the stream ended inside a frame")
)

// manifestFrame is one frame. T says which fields are meant.
type manifestFrame struct {
	T string `json:"t"`

	Request   *manifestRequest   `json:"request,omitempty"`
	Log       *manifestLog       `json:"log,omitempty"`
	Pom       *manifestPomAsk    `json:"pom,omitempty"`
	PomAnswer *manifestPomAnswer `json:"pom_answer,omitempty"`
	Part      *manifestPart      `json:"part,omitempty"`
	Done      *manifestDone      `json:"done,omitempty"`
}

const (
	frameRequest   = "request"
	frameLog       = "log"
	framePom       = "pom"
	framePomAnswer = "pom_answer"
	framePart      = "part"
	frameDone      = "done"
)

// manifestRequest is what the parent asks of the child.
type manifestRequest struct {
	// Version is the version of the program that asks (db.ToolVersion). The
	// child serves only its own version (ManifestChildMain).
	Version string `json:"version"`
	// Build is the file the asking process is (programBuild), "" when it
	// could not tell: a child that is another file answers as it does to
	// another version.
	Build string `json:"build,omitempty"`
	// Passes are the readings wanted, done in this order by one child: the
	// names, the declarations, the lockfiles (manifestPass*). Each ends with
	// its own done frame, so that a child stopped in a later pass leaves the
	// earlier ones whole.
	Passes []string `json:"passes"`
	Dir    string   `json:"dir"`
	RepoID int64    `json:"repo_id"`

	DevBuildDeps      bool `json:"dev_build_deps"`
	GitHubActionsDeps bool `json:"github_actions_deps"`
	// PomSource: the parent answers pom frames. RemotePomLimit is how many
	// coordinates the walk may ask for.
	PomSource      bool `json:"pom_source"`
	RemotePomLimit int  `json:"remote_pom_limit"`
	// The child's limits on itself (the watchdog).
	CPULimitNanos  int64 `json:"cpu_limit_nanos"`
	HeapLimitBytes int64 `json:"heap_limit_bytes"`
	// ParentPID is the process that started the child: the child ends
	// itself when that is no longer its parent. Told, not asked of the
	// system when the child starts: a parent that died before the child
	// looked would never be seen to go. 0: not watched (a child run inside
	// another process, in a test).
	ParentPID int `json:"parent_pid"`
}

// manifestLog is one log record of the pass. An attribute's value travels
// as text, with its kind, so the parent logs an int as an int.
type manifestLog struct {
	Level int               `json:"level"`
	Msg   string            `json:"msg"`
	Attrs []manifestLogAttr `json:"attrs,omitempty"`
}

type manifestLogAttr struct {
	Key   string `json:"k"`
	Value string `json:"v"`
	Kind  string `json:"kind,omitempty"` // "i": an integer; "b": a bool; "": text
}

type manifestPomAsk struct {
	Group    string   `json:"group"`
	Artifact string   `json:"artifact"`
	Version  string   `json:"version"`
	Declared []string `json:"declared,omitempty"`
}

type manifestPomAnswer struct {
	Outcome  int                 `json:"outcome"`
	Pom      *manifest.RemotePom `json:"pom,omitempty"`
	ServedBy string              `json:"served_by,omitempty"` // RemotePom.ServedBy is not in its own JSON
}

// manifestPart is a piece of a result: what it holds is appended to what
// came before.
type manifestPart struct {
	Deps         []model.LibyearDep            `json:"deps,omitempty"`
	Unused       []model.LibyearDep            `json:"unused,omitempty"`
	Failed       []manifestWireFailure         `json:"failed,omitempty"`
	TreeHosts    []model.MavenTreeHosts        `json:"tree_hosts,omitempty"`
	NotWhole     []model.MavenTreeGap          `json:"not_whole,omitempty"`
	Repositories []manifest.DeclaredRepository `json:"repositories,omitempty"`
	// Names are name counts of the name pass: language, name, count.
	Names []manifestNameCount `json:"names,omitempty"`
	// Present are the lockfiles the clone holds, listed by the name pass
	// without reading one (0.29.214).
	Present []manifestWirePresent `json:"present,omitempty"`
	// The lockfile pass: each lockfile that parsed is one head, then its
	// entries and edges, which name it by its place among the heads.
	Lockfiles []manifestWireLockfile `json:"lockfiles,omitempty"`
	Entries   []manifestWireEntry    `json:"entries,omitempty"`
	Edges     []manifestWireEdge     `json:"edges,omitempty"`
}

// manifestWireLockfile is a parsed lockfile without its entries and edges,
// and how many of each follow.
type manifestWireLockfile struct {
	Path        string `json:"path"`
	Kind        string `json:"kind"`
	Ecosystem   string `json:"ecosystem"`
	DirectKnown bool   `json:"direct_known,omitempty"`
	Aliased     int    `json:"aliased,omitempty"`
	NotRegistry int    `json:"not_registry,omitempty"`
	NotAVersion int    `json:"not_a_version,omitempty"`
	Entries     int    `json:"entries"`
	Edges       int    `json:"edges"`
}

// manifestWirePresent is a lockfile that is in the clone: its path and its
// roster kind (its file name).
type manifestWirePresent struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
}

// manifestWireEntry and manifestWireEdge are a LockfileEntry and a
// LockfileEdge of lockfile number L. Short names: a lockfile has as many as
// a million of them.
type manifestWireEntry struct {
	L         int    `json:"l"`
	Name      string `json:"n"`
	Version   string `json:"v"`
	Direct    bool   `json:"d,omitempty"`
	Namespace string `json:"ns,omitempty"`
	Ecosystem string `json:"e,omitempty"`
	Scope     string `json:"s,omitempty"`
}

type manifestWireEdge struct {
	L             int    `json:"l"`
	ParentName    string `json:"p"`
	ParentVersion string `json:"pv,omitempty"`
	ChildName     string `json:"c"`
	Constraint    string `json:"k,omitempty"`
}

// manifestWireFailure is a model.ManifestFailure with the field its own JSON
// leaves out (NoCarry is an instruction to the writer, not part of the
// stored record).
type manifestWireFailure struct {
	model.ManifestFailure
	NoCarry  bool `json:"no_carry,omitempty"`
	TooLarge bool `json:"too_large,omitempty"`
}

type manifestNameCount struct {
	Language string `json:"l"`
	Name     string `json:"n"`
	Count    int    `json:"c"`
}

// manifestDone ends one pass's result. RootError: the clone's root could
// not be read, and that pass has no result.
type manifestDone struct {
	Pass           string `json:"pass"`
	Found          int    `json:"found"`
	Parsed         int    `json:"parsed"`
	RemoteNotAsked int    `json:"remote_not_asked"`
	// Lockfiles is how many lockfiles the lockfile pass parsed and sent.
	Lockfiles int `json:"lockfiles"`
	// LockfilesListed: the name pass listed the clone's lockfiles (its
	// Present items are all of them).
	LockfilesListed bool   `json:"lockfiles_listed,omitempty"`
	RootError       string `json:"root_error,omitempty"`
	// What the pass cost the child, as its watchdog measured it: the CPU
	// time used and the largest heap seen.
	CPUNanos      int64 `json:"cpu_nanos"`
	HeapPeakBytes int64 `json:"heap_peak_bytes"`
	// MaxRSSBytes is the most memory the child process has had resident, as
	// the system counted it: exact, where the heap peak is the largest of
	// the watchdog's samples.
	MaxRSSBytes int64 `json:"max_rss_bytes"`
}

// writeManifestFrame writes one frame. Every string in it is first made
// exact (wireEscape): JSON cannot carry a string that is not UTF-8, and a
// manifest's bytes are whatever its author wrote.
func writeManifestFrame(w io.Writer, f *manifestFrame) error {
	escaped := *f
	wireWalk(reflect.ValueOf(&escaped).Elem(), wireEscape, true)
	body, err := json.Marshal(&escaped)
	if err != nil {
		return err
	}
	if int64(len(body)) > manifestFrameLimit {
		return errManifestFrameTooLarge
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(body)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// readManifestFrame reads one frame. io.EOF: the stream ended between
// frames.
func readManifestFrame(r io.Reader) (*manifestFrame, int, error) {
	body, err := readManifestFrameBytes(r)
	if err != nil {
		return nil, 0, err
	}
	return decodeManifestFrame(body)
}

// readManifestFrameBytes reads one frame's bytes, undecoded.
func readManifestFrameBytes(r io.Reader) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errManifestFrameShort
		}
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if int64(n) > manifestFrameLimit {
		return nil, errManifestFrameTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, errManifestFrameShort
	}
	return body, nil
}

func decodeManifestFrame(body []byte) (*manifestFrame, int, error) {
	n := len(body)
	var f manifestFrame
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, 0, fmt.Errorf("a frame is not what this protocol sends: %w", err)
	}
	if dec.More() {
		return nil, 0, errors.New("a frame holds more than one value")
	}
	bad := false
	wireWalk(reflect.ValueOf(&f).Elem(), func(s string) string {
		out, ok := wireUnescape(s)
		if !ok {
			bad = true
		}
		return out
	}, false)
	if bad {
		return nil, 0, errors.New("a frame holds a string that is not one this protocol wrote")
	}
	return &f, int(n), nil
}

// wireMark begins a string that travels as base64: one that is not UTF-8,
// or that itself begins with the mark. NUL is the mark because no manifest
// field that is ever stored may hold one, so text that is sent as it is
// stays readable in a dump.
const wireMark = "\x00"

func wireEscape(s string) string {
	if utf8.ValidString(s) && !strings.HasPrefix(s, wireMark) {
		return s
	}
	return wireMark + base64.StdEncoding.EncodeToString([]byte(s))
}

func wireUnescape(s string) (string, bool) {
	rest, marked := strings.CutPrefix(s, wireMark)
	if !marked {
		return s, true
	}
	b, err := base64.StdEncoding.DecodeString(rest)
	if err != nil {
		return "", false
	}
	return string(b), true
}

// wireWalk applies fn to every string a frame holds: fields, slice
// elements, map keys and values. copyFirst: what is reached through a
// pointer, slice or map is copied before it is changed, so the caller's
// values are untouched (writing); a frame just decoded is changed in place.
func wireWalk(v reflect.Value, fn func(string) string, copyFirst bool) {
	switch v.Kind() {
	case reflect.String:
		if v.CanSet() {
			v.SetString(fn(v.String()))
		}
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		if copyFirst {
			c := reflect.New(v.Type().Elem())
			c.Elem().Set(v.Elem())
			v.Set(c)
		}
		wireWalk(v.Elem(), fn, copyFirst)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				wireWalk(v.Field(i), fn, copyFirst)
			}
		}
	case reflect.Slice:
		if v.IsNil() {
			return
		}
		if copyFirst {
			c := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
			reflect.Copy(c, v)
			v.Set(c)
		}
		for i := 0; i < v.Len(); i++ {
			wireWalk(v.Index(i), fn, copyFirst)
		}
	case reflect.Map:
		if v.IsNil() {
			return
		}
		c := reflect.MakeMapWithSize(v.Type(), v.Len())
		for _, key := range v.MapKeys() {
			k := reflect.New(v.Type().Key()).Elem()
			k.Set(key)
			e := reflect.New(v.Type().Elem()).Elem()
			e.Set(v.MapIndex(key))
			wireWalk(k, fn, copyFirst)
			wireWalk(e, fn, copyFirst)
			c.SetMapIndex(k, e)
		}
		v.Set(c)
	}
}
