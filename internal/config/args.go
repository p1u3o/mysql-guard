package config

import (
	"flag"
	"fmt"
	"strings"
)

// SplitFlagsAndPositionals separates flag tokens from positional arguments so flags
// may appear before or after the subcommand target.
func SplitFlagsAndPositionals(args []string) (flagArgs []string, positionals []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-") {
			flagArgs = append(flagArgs, a)
			if needsFlagValue(a) && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				flagArgs = append(flagArgs, args[i+1])
				i++
			}
			continue
		}
		positionals = append(positionals, a)
	}
	return flagArgs, positionals
}

func needsFlagValue(name string) bool {
	if name == "-" || name == "--" {
		return false
	}
	if strings.Contains(name, "=") {
		return false
	}
	switch name {
	case "-h", "-help", "--help":
		return false
	}
	n := strings.TrimLeft(name, "-")
	return !isBoolFlag(n)
}

// isBoolFlag reports whether name is a boolean guard flag. Boolean flags never
// consume the following argument (-mysql-tls rejoin db-a), matching the flag
// package; give them an explicit value with -flag=false.
func isBoolFlag(name string) bool {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	defineFlags(fs, &Config{}, &rawFlags{})
	f := fs.Lookup(name)
	if f == nil {
		return false
	}
	bf, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok && bf.IsBoolFlag()
}

// LoadSubcommand parses flags in any order and returns optional positional target.
func LoadSubcommand(args []string) (*Config, string, error) {
	flagArgs, pos := SplitFlagsAndPositionals(args)
	cfg, err := Load(flagArgs)
	if err != nil {
		return nil, "", err
	}
	if len(pos) > 1 {
		return nil, "", fmt.Errorf("unexpected positional arguments: %s", strings.Join(pos[1:], " "))
	}
	target := ""
	if len(pos) == 1 {
		target = pos[0]
	}
	return cfg, target, nil
}
