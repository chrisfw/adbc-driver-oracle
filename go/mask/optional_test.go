package mask

import (
	"go/parser"
	gotoken "go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The hook is optional by import: a build that never imports this package
// carries none of it. That holds only while nothing else in the module
// imports it, so the driver package and the C shared library stay free of it.
func TestNothingElseInTheModuleImportsMask(t *testing.T) {
	const self = "github.com/chrisfw/adbc-driver-oracle/go/mask"
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	here, _ := filepath.Abs(".")
	fset := gotoken.NewFileSet()
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == here || strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			if p, _ := strconv.Unquote(imp.Path.Value); p == self {
				t.Errorf("%s imports the mask package; the hook must stay opt-in by import", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
