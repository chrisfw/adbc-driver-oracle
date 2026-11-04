package mask

import "testing"

func BenchmarkIdentifierFF1(b *testing.B) {
	spec := IdentifierSpec{Family: "account", Unit: 1000, Width: 9, Frontiers: []uint64{400_000_000_000}}
	id, _ := NewIdentifier(testKey, "amo", spec, []uint64{1000})
	b.ResetTimer()
	for i := range b.N {
		id.Mask(uint64(i%399_999_998+1) * 1000)
	}
}
