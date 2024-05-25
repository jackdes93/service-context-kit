package flagenv

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var Prefix = ""

func contains(list []*flag.Flag, f *flag.Flag) bool {
	for _, i := range list {
		if i == f {
			return true
		}
	}
	return false
}

func ParseSet(prefix string, set *flag.FlagSet) error {
	var explicit []*flag.Flag
	var all []*flag.Flag
	set.Visit(func(f *flag.Flag) {
		explicit = append(explicit, f)
	})

	var err error
	set.VisitAll(func(f *flag.Flag) {
		if err != nil {
			return
		}
		all = append(all, f)
		if !contains(explicit, f) {
			name := strings.Replace(f.Name, ".", "_", -1)
			name = strings.Replace(name, "-", "_", -1)

			if prefix != "" {
				name = prefix + name
			}
			name = strings.ToUpper(name)
			val := os.Getenv(name)
			if val != "" {
				if ferr := f.Value.Set(val); ferr != nil {
					err = fmt.Errorf("failed to set flag %q with value %q", f.Name, val)
				}
			}
		}
	})
	return err
}

func Parse() {
	if err := ParseSet(Prefix, flag.CommandLine); err != nil {
		log.Fatalln(err)
	}
}
