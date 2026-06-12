// Package tar extracts files from tar streams: pure Go, single pass
// over any io.Reader (the stdlib reader decodes every sparse encoding
// into logical bytes), entries matched and materialized on the fly —
// pipes need no spooling and no tar binary is involved. Zero runs land
// as holes whether the archive encoded them or not. Selection and
// renaming use Rules: left side = in-tar path, right side = filesystem
// path, or "-" for the process stdout.
//
// Producing tar streams is out of scope here: single sparse files
// travel via sandbox-accelerator/pkg/tarstream (WriteTo/ReadFrom).
package tar

import (
	"fmt"
	"path"
	"strings"
)

// Rule maps between an in-tar path and an outside path. The same rule
// vocabulary drives both directions: Create reads the outside and
// stores it under Tar; Extract reads Tar and writes it outside.
//
//	Tar    in-tar path; "" selects the archive root (every entry)
//	FS     outside path; "-" means process stdio (file rules only)
//	Prefix true for directory rules (written with a trailing "/"):
//	       Tar is a name prefix and FS a directory root
type Rule struct {
	Tar    string
	FS     string
	Prefix bool
}

// ParseRule parses one rule argument:
//
//	p             ≡ p:p
//	t:f           file rule (rename)
//	t:-           file rule, outside = stdio
//	d/            ≡ d/:d/
//	d/:o[/]       directory prefix rule
//	d/:           ≡ d/:.   (current directory)
//	:o/           ≡ /:o/   (archive root)
//	t:            file rule, outside = base name of t
//
// Leading "/" and "./" on the tar side are stripped before matching;
// ".." segments on the tar side are rejected.
func ParseRule(arg string) (Rule, error) {
	if arg == "" {
		return Rule{}, fmt.Errorf("empty rule")
	}
	tarSide, fsSide := arg, ""
	hasColon := false
	if i := strings.Index(arg, ":"); i >= 0 {
		tarSide, fsSide, hasColon = arg[:i], arg[i+1:], true
	}

	prefix := strings.HasSuffix(tarSide, "/")
	t, err := normalizeTarPath(tarSide)
	if err != nil {
		return Rule{}, fmt.Errorf("rule %q: %w", arg, err)
	}
	if t == "" { // archive root
		if !hasColon {
			return Rule{}, fmt.Errorf("rule %q: bare archive root is meaningless; use :dir/", arg)
		}
		if !prefix && fsSide != "" && !strings.HasSuffix(fsSide, "/") {
			return Rule{}, fmt.Errorf("rule %q: archive-root rule needs a directory target (:dir/)", arg)
		}
		prefix = true
	}

	r := Rule{Tar: t, Prefix: prefix}
	switch {
	case !hasColon:
		r.FS = tarSide // verbatim, including any leading "/" or "./"
		if prefix {
			r.FS = strings.TrimSuffix(r.FS, "/")
			if r.FS == "" {
				r.FS = "/"
			}
		}
	case fsSide == "":
		if prefix {
			r.FS = "." // d/:  → current directory
		} else {
			r.FS = path.Base(t) // t:  → base name in current directory
		}
	case fsSide == "-":
		if prefix {
			return Rule{}, fmt.Errorf("rule %q: stdio (-) is only valid for file rules", arg)
		}
		r.FS = "-"
	default:
		r.FS = strings.TrimSuffix(fsSide, "/")
		if r.FS == "" {
			r.FS = "/"
		}
		if !prefix && strings.HasSuffix(fsSide, "/") {
			return Rule{}, fmt.Errorf("rule %q: file rule with directory target; use %s/:%s or name the file", arg, tarSide, fsSide)
		}
	}
	if r.Tar == "" && !r.Prefix {
		return Rule{}, fmt.Errorf("rule %q: empty tar side is only valid for directory rules (:dir/)", arg)
	}
	return r, nil
}

// ParseRules parses a rule list and rejects duplicate stdio rules.
func ParseRules(args []string) ([]Rule, error) {
	rules := make([]Rule, 0, len(args))
	stdio := 0
	for _, a := range args {
		r, err := ParseRule(a)
		if err != nil {
			return nil, err
		}
		if r.FS == "-" {
			stdio++
		}
		rules = append(rules, r)
	}
	if stdio > 1 {
		return nil, fmt.Errorf("at most one stdio (-) rule is allowed")
	}
	return rules, nil
}

// normalizeTarPath canonicalizes an in-tar path for matching: strips
// leading "/" and "./", cleans the path, rejects "..". Returns "" for
// the archive root.
func normalizeTarPath(p string) (string, error) {
	p = strings.TrimSuffix(p, "/")
	for {
		switch {
		case strings.HasPrefix(p, "/"):
			p = strings.TrimPrefix(p, "/")
		case strings.HasPrefix(p, "./"):
			p = strings.TrimPrefix(p, "./")
		default:
			goto done
		}
	}
done:
	if p == "" || p == "." {
		return "", nil
	}
	c := path.Clean(p)
	if c == ".." || strings.HasPrefix(c, "../") {
		return "", fmt.Errorf("tar path escapes the archive root: %q", p)
	}
	return c, nil
}

// match resolves an entry name against the rules: the most specific
// match wins (exact file rule first, then the longest directory
// prefix). ok=false means the entry is not selected.
func match(rules []Rule, name string) (best Rule, rel string, ok bool) {
	bestLen := -1
	for _, r := range rules {
		if !r.Prefix {
			if r.Tar == name {
				return r, "", true // exact file rule: most specific possible
			}
			continue
		}
		switch {
		case r.Tar == "": // archive root
			if bestLen < 0 {
				best, bestLen, ok, rel = r, 0, true, name
			}
		case name == r.Tar:
			if len(r.Tar) > bestLen {
				best, bestLen, ok, rel = r, len(r.Tar), true, ""
			}
		case strings.HasPrefix(name, r.Tar+"/"):
			if len(r.Tar) > bestLen {
				best, bestLen, ok, rel = r, len(r.Tar), true, name[len(r.Tar)+1:]
			}
		}
	}
	if !ok {
		return Rule{}, "", false
	}
	return best, rel, true
}
