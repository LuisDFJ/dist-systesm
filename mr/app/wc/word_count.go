package wc

import (
	"strconv"
	"strings"
	"unicode"
	"mr/shared"
)

func Map( filename string, contents string ) []shared.KeyValue {
	f := func (r rune) bool { return !unicode.IsLetter(r) }

	kv := []shared.KeyValue{}
	for w := range strings.FieldsFuncSeq(contents, f) {
		kv = append(kv, shared.KeyValue{Key:w,Value:"1"})
	}
	return kv
}

func Reduce( kvs shared.KeyValues ) string {
	return strconv.Itoa(len(kvs.Values))
}
