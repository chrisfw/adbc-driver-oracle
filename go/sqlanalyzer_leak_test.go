package oracle

import (
	"context"
	"runtime"
	"strings"
	"testing"
)

// Every parse runs in its own WebAssembly instance; an instance left open
// keeps its linear memory, so a long export leaked gigabytes. After many
// parses of a wide statement the heap must come back down.
func TestAnalyzerInstancesAreReleased(t *testing.T) {
	ctx := context.Background()
	a, err := sharedAnalyzer(ctx)
	if err != nil {
		t.Skip(err)
	}
	cols := make([]string, 200)
	for i := range cols {
		cols[i] = "COLUMN_NUMBER_" + strings.Repeat("X", 20) + string(rune('A'+i%26))
	}
	sql := "SELECT " + strings.Join(cols, ", ") + " FROM CIS.SOME_WIDE_TABLE WHERE ROWNUM < 10"
	for i := 0; i < 60; i++ {
		if _, err := a.parseStatements(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.HeapInuse > 512<<20 {
		t.Fatalf("after 60 parses %d MB are still in use: instances are not released", m.HeapInuse>>20)
	}
	t.Logf("heap in use after 60 parses: %d MB", m.HeapInuse>>20)
}
