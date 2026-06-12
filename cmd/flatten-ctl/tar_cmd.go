package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandbox-builder/pkg/remote"
	btar "github.com/kuasar-sandbox/sandbox-builder/pkg/tar"
)

// cmdTar implements the tar tooling, all pure Go:
//
//	flatten-ctl tar extract — pull files out of any tar stream
//	  (single-pass streaming; sparse members get their holes back)
//	flatten-ctl tar stream  — package one file (sparseness
//	  auto-detected) as a tarstream (sandbox-accelerator pkg/tarstream)
func cmdTar(args []string) {
	if len(args) < 1 || args[0] == "-h" || args[0] == "--help" {
		tarUsage()
		if len(args) < 1 {
			os.Exit(1)
		}
		return
	}
	sub := args[0]
	switch sub {
	case "extract":
		cmdTarExtract(args[1:])
	case "stream":
		cmdTarStream(args[1:])
	default:
		fatal("tar: unknown subcommand %q (want extract or stream)", sub)
	}
}

func cmdTarExtract(args []string) {
	fs := flag.NewFlagSet("tar extract", flag.ExitOnError)
	var file string
	fs.StringVar(&file, "file", "-", "archive file (- = stdin)")
	fs.StringVar(&file, "f", "-", "shorthand for --file")
	chown := fs.String("chown", "", "numeric uid:gid applied to every entry")
	chmod := fs.String("chmod", "", "octal permission bits applied to every entry")
	fs.Usage = tarUsage
	fs.Parse(args)

	rules, err := btar.ParseRules(fs.Args())
	if err != nil {
		fatal("tar: %v", err)
	}
	opts := btar.Options{
		Warnf: func(format string, a ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", a...)
		},
	}
	if *chown != "" {
		o, err := btar.ParseOwner(*chown)
		if err != nil {
			fatal("tar: %v", err)
		}
		opts.Chown = &o
	}
	if *chmod != "" {
		m, err := btar.ParseMode(*chmod)
		if err != nil {
			fatal("tar: %v", err)
		}
		opts.Chmod = &m
	}

	var in io.Reader = os.Stdin
	if file != "-" {
		f, err := os.Open(file)
		if err != nil {
			fatal("tar extract: %v", err)
		}
		defer f.Close()
		in = f
	}
	if err := btar.Extract(in, rules, opts); err != nil {
		fatal("%v", err)
	}
}

func cmdTarStream(args []string) {
	fs := flag.NewFlagSet("tar stream", flag.ExitOnError)
	var file string
	fs.StringVar(&file, "file", "-", "tarstream output (- = stdout)")
	fs.StringVar(&file, "f", "-", "shorthand for --file")
	configPath := fs.String("config", "", "flatten config YAML (overrides FLATTEN_CONFIG env): tmpdir for the stdin spool")
	fs.Usage = tarUsage
	fs.Parse(args)

	if fs.NArg() > 1 {
		fatal("tar stream: packages exactly one file (got %d rules)", fs.NArg())
	}
	name, src, err := parseStreamRule(fs.Arg(0))
	if err != nil {
		fatal("tar stream: %v", err)
	}

	var out io.Writer
	if file == "-" {
		if st, err := os.Stdout.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
			fatal("tar stream: refusing to write a tar stream to a terminal (use -f FILE or redirect stdout)")
		}
		out = os.Stdout
	} else {
		f, err := os.Create(file)
		if err != nil {
			fatal("tar stream: %v", err)
		}
		defer f.Close()
		out = f
	}

	var data *os.File
	if src == "-" {
		// A one-shot stream has no length, and the tar header needs it
		// up front: spool stdin, punching zero runs into holes so the
		// probe below finds them (tmpdir on tmpfs ≈ memory buffering).
		cfg, err := remote.LoadConfig(*configPath, flattenConfigEnv)
		if err != nil {
			fatal("%v", err)
		}
		tmp, err := os.CreateTemp(cfg.TmpDir, "tarstream-stdin-*")
		if err != nil {
			fatal("tar stream: %v", err)
		}
		defer func() {
			tmp.Close()
			os.Remove(tmp.Name())
		}()
		if err := spoolPunched(tmp, os.Stdin); err != nil {
			fatal("tar stream: spool stdin: %v", err)
		}
		data = tmp
	} else {
		f, err := os.Open(src)
		if err != nil {
			fatal("tar stream: %v", err)
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			fatal("tar stream: %v", err)
		}
		if !st.Mode().IsRegular() {
			fatal("tar stream: %s is not a regular file", src)
		}
		data = f
	}

	holes, err := tarstream.ProbeHoles(data)
	if err != nil {
		fatal("tar stream: probe holes: %v", err)
	}
	if err := tarstream.WriteTo(out, name, data, holes); err != nil {
		fatal("%v", err)
	}
}

// parseStreamRule normalizes the stream argument "in-tar[:source]":
//
//	(none)        ≡ -:-
//	-             ≡ -:-
//	path/to/file  ≡ file:path/to/file   (named after its base)
//	:source       ≡ -:source
//	name:-        content from stdin
//	name:source   explicit
func parseStreamRule(arg string) (name, src string, err error) {
	switch arg {
	case "", "-":
		return "-", "-", nil
	}
	if i := strings.Index(arg, ":"); i >= 0 {
		name, src = arg[:i], arg[i+1:]
		if name == "" {
			name = "-"
		}
		if src == "" {
			return "", "", fmt.Errorf("rule %q: missing source (use %s:- for stdin)", arg, name)
		}
		return name, src, nil
	}
	return path.Base(arg), arg, nil
}

// spoolPunched copies r into f, seeking over zero runs (4 KiB
// granularity) so they become holes ProbeHoles can find, and sets the
// final size.
func spoolPunched(f *os.File, r io.Reader) error {
	const blk = 4096
	zero := make([]byte, blk)
	buf := make([]byte, 32*blk)
	var off int64
	for {
		n, err := io.ReadFull(r, buf)
		chunk := buf[:n]
		for len(chunk) > 0 {
			c := min(blk, len(chunk))
			if bytes.Equal(chunk[:c], zero[:c]) {
				off += int64(c)
				chunk = chunk[c:]
				continue
			}
			end := c
			for end < len(chunk) {
				next := min(blk, len(chunk)-end)
				if bytes.Equal(chunk[end:end+next], zero[:next]) {
					break
				}
				end += next
			}
			if _, werr := f.WriteAt(chunk[:end], off); werr != nil {
				return werr
			}
			off += int64(end)
			chunk = chunk[end:]
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return f.Truncate(off)
		}
		if err != nil {
			return err
		}
	}
}

func tarUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  flatten-ctl tar extract [-f tarfile] [--chown u:g] [--chmod 755] [rule...]
  flatten-ctl tar stream  [-f tarfile] [in-tar[:source]]

Both directions are pure Go; no tar binary is involved.

extract pulls files out of any tar stream (-f defaults to stdin), one
single pass — pipes need no spooling, sparse members and dense zero
runs land as holes. Rules are "in-tar-path[:outside-path]":

  path              same name inside and outside
  in:out            rename (write the entry "in" to the path "out")
  in:-              stream the entry's content to stdout (one rule at most)
  dir/              directory rule: dir and everything under it
  dir/:out[/]       directory prefix rename
  dir/:             extract under the current directory
  :dir/             the whole archive root mapped to dir/

No rules takes everything. --chown/--chmod override ownership and
permissions on every extracted entry.

stream packages exactly one file as a tarstream (a single-file sparse
tar; see sandbox-accelerator/pkg/tarstream), holes auto-detected — from
the filesystem via SEEK_HOLE, or by zero-run detection when the source
is stdin (spooled first: a tar header carries the size up front). The
argument is "in-tar[:source]":

  (none) or -       ≡ -:-  (entry named "-", content from stdin)
  path/to/file      ≡ file:path/to/file  (named after its base)
  :source           ≡ -:source
  name:-            entry "name", content from stdin
  name:source       explicit

The output (-f, default stdout) is itself a valid tar: extract, GNU tar
and archive/tar all read it back.
`)
}
