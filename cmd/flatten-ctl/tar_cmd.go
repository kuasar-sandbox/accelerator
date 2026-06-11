package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/kuasar-sandbox/sandbox-builder/pkg/remote"
	btar "github.com/kuasar-sandbox/sandbox-builder/pkg/tar"
)

// cmdTar implements `flatten-ctl tar create|extract`: assemble files
// into / extract files from a tar stream via a GNU tar engine
// (pkg/tar). Rules read `tar内路径[:外部路径]`; "-" on the right side
// is the process stdio, a trailing "/" marks a directory prefix rule.
func cmdTar(args []string) {
	if len(args) < 1 || args[0] == "-h" || args[0] == "--help" {
		tarUsage()
		if len(args) < 1 {
			os.Exit(1)
		}
		return
	}
	sub := args[0]
	if sub != "create" && sub != "extract" {
		fatal("tar: unknown subcommand %q (want create or extract)", sub)
	}

	fs := flag.NewFlagSet("tar "+sub, flag.ExitOnError)
	var file string
	fs.StringVar(&file, "file", "-", "archive file (- = stdin/stdout)")
	fs.StringVar(&file, "f", "-", "shorthand for --file")
	chown := fs.String("chown", "", "numeric uid:gid applied to every entry")
	chmod := fs.String("chmod", "", "octal permission bits applied to every entry")
	configPath := fs.String("config", "", "flatten config YAML (overrides FLATTEN_CONFIG env): tmpdir for spool files")
	fs.Usage = tarUsage
	fs.Parse(args[1:])

	rules, perr := btar.ParseRules(fs.Args())
	if perr != nil {
		fatal("tar: %v", perr)
	}

	opts := btar.Options{
		Warnf: func(format string, a ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", a...)
		},
	}
	cfg, err := remote.LoadConfig(*configPath, flattenConfigEnv)
	if err != nil {
		fatal("%v", err)
	}
	opts.TmpDir = cfg.TmpDir
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

	switch sub {
	case "create":
		if len(rules) == 0 {
			fatal("tar create: file list is required (tar内路径[:外部路径] ...)")
		}
		var out io.Writer
		if file == "-" {
			if st, err := os.Stdout.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
				fatal("tar create: refusing to write an archive to a terminal (use -f FILE or redirect stdout)")
			}
			out = os.Stdout
		} else {
			f, err := os.Create(file)
			if err != nil {
				fatal("tar create: %v", err)
			}
			defer f.Close()
			out = f
		}
		if err := btar.Create(out, rules, opts); err != nil {
			fatal("%v", err)
		}
	case "extract":
		var in io.Reader
		if file == "-" {
			in = os.Stdin
		} else {
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
}

func tarUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  flatten-ctl tar create  [-f tarfile] [--chown u:g] [--chmod 755] rule...
  flatten-ctl tar extract [-f tarfile] [--chown u:g] [--chmod 755] [rule...]

-f defaults to "-": create writes the archive to stdout, extract reads
it from stdin. Rules are "in-tar-path[:outside-path]":

  path              same name inside and outside
  in:out            rename (create: read out, store as in; extract: the reverse)
  in:-              the outside is this process's stdio (one rule at most)
  dir/              directory rule: dir and everything under it
  dir/:out[/]       directory prefix rename
  dir/:             extract under / pack from the current directory
  :dir/             the whole archive root mapped to dir/

Sparse files keep their holes (GNU --sparse, PAX 1.0) on create and
get them back on extract. create requires a rule list; extract without
rules takes everything. Needs GNU tar >= 1.28 ($TAR_PATH, next to this
binary, or PATH).
`)
}
