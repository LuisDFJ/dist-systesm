package types

import (
	"slices"
	"iter"
	"cmp"
	"mr/shared"
)

type Collection []shared.KeyValue

func (l Collection) Sort() {
	f := func ( a,b shared.KeyValue ) int {
		return cmp.Compare(a.Key, b.Key)
	}
	slices.SortFunc(l,f)
}

func (l Collection) Suffle() iter.Seq[shared.KeyValues] {
	l.Sort()
	return func( yield func( shared.KeyValues ) bool ) {
		i := 0
		for i < len(l) {
			j :=  i + 1
			for j < len(l) && l[j].Key == l[i].Key { j++ }
			values := []string{}
			for k := i; k < j; k++ { values = append(values,l[k].Value) }
			kvs := shared.KeyValues{Key: l[i].Key, Values:values}
			if !yield( kvs ) { break }
			i = j
		}
	}
}

