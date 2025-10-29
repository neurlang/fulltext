// package fulltext implements a Fulltext Index data structure for Golang
package fulltext

import quaternary "github.com/neurlang/quaternary/v1"
import "fmt"

type BagOfWords = map[string]struct{}

type Index struct {
	private struct {
		Version byte            `json:"version"`
		Pk []byte               `json:"pk"`
		Buckets [][]byte        `json:"buckets"`
		Pkbits uint64           `json:"pkbits"`
		Rows uint64             `json:"rows"`
		Logrows byte            `json:"logrows"`
		Maxword int             `json:"maxword"`
	}
}

type NewOpts struct {
	// use this to tune the false positive rate of the underlying filters. Default = 10.
	// Higher values (10+) consume more memory, but cause less false positive problems.
	// lower values (0-9) increase the false positive rate, cause more false positive problems.
	FalsePositiveFunctions byte
}

var ErrNonuniform = fmt.Errorf("nonuniform_key_size")
var ErrNilGetter = fmt.Errorf("nil_getter")

// New creates new full text index based on primary keys with common size of every string primary key.
// Getter iterates the storage based on primary keys and returns the words in the row with primaryKey. Opts can be nil.
func New(opts *NewOpts, primaryKeys BagOfWords, getter func(primaryKey string) BagOfWords) (i *Index, err error) {
	if getter == nil {
		return nil, ErrNilGetter
	}
	if opts == nil {
		// defaults
		opts = &NewOpts{
			FalsePositiveFunctions: 10,
		}
	}
	i = new(Index)
	i.private.Version = 1
	var ikeys = make(map[int]string, len(primaryKeys))
	var keys_len int
	countBag := make(map[[3]byte]uint64)
	initialBag := make(map[string]uint64)
	for k := range primaryKeys {
		if keys_len == 0 {
			keys_len = len(k)
		} else if keys_len != len(k) {
			return nil, ErrNonuniform
		}
		size := len(ikeys)+1
		ikeys[size] = k
		bag := getter(k)
		for word := range bag {
			if len(word) > i.private.Maxword {
				i.private.Maxword = len(word)
			}
			if len(word) < 3 {
				continue
			}
			for len(word) - 3 >= len(i.private.Buckets) {
				i.private.Buckets = append(i.private.Buckets, nil)
			}
			wrd := [3]byte{word[0], word[1], word[2]}
			countBag[wrd]++
			cnt := countBag[wrd]
			initialBag[word[:3]+fmt.Sprint(cnt)] = uint64(size)

		}
	}
	for k, v := range countBag {
		//println("countBag:", string(k[:])+"0", v)
		initialBag[string(k[:])+"0"] = v
	}
	primaryKeys = nil
	i.private.Rows = uint64(len(ikeys))
	for j := i.private.Rows; j > 0; j >>= 1 {
		i.private.Logrows++
	}
	if i.private.Rows > 0 {
		i.private.Pkbits = uint64(len(ikeys[1])) * 8
	}
	if i.private.Pkbits <= 255 {
		i.private.Pk = quaternary.Make(ikeys, byte(i.private.Pkbits))
	} else {
		i.private.Pk = quaternary.Make(ikeys, 0)
	}
	ikeys = nil
	if len(i.private.Buckets) > 0 {
		i.private.Buckets[0] = quaternary.New(initialBag, i.private.Logrows, opts.FalsePositiveFunctions)
	} else {
		return
	}
	for q := 0; q + 3 < i.private.Maxword; q++ {
		countBag := make(map[[3]byte]uint64)
		initialBag := make(map[string]uint64)
		for j := uint64(1); j <= i.private.Rows; j++ {
			var k = string(quaternary.Get(i.private.Pk, i.private.Pkbits, j))	
			bag := getter(k)
			for word := range bag {
				if len(word) <= 3 + q {
					continue
				}
				wrd := [3]byte{word[1+q], word[2+q], word[3+q]}
				countBag[wrd]++
				cnt := countBag[wrd]
				initialBag[word[1+q:4+q]+fmt.Sprint(cnt)] = j
			}
		}
		for k, v := range countBag {
			//println("countBag:", string(k[:])+"0", v)
			initialBag[string(k[:])+"0"] = v
		}
		i.private.Buckets[1+q] = quaternary.New(initialBag, i.private.Logrows, opts.FalsePositiveFunctions)
	}
	return
}


// Lookup iterates the fulltext search index based on a specific word with length of 3 characters or more.
// Exact finds exact word matches (faster). Dedup hits each primary key exactly once (slower, but can be worth it if db is slow).
// Iterator can (in rare cases) have false positives.
func (i *Index) Lookup(word string, exact, dedup bool) func(yield func(primaryKey string) bool) {
	return func(yield func(string) bool) {
		if len(word) < 3 {
			return
		}
		if i.private.Rows == 0 {
			return
		}
		var uniq map[uint64]int
		if dedup {
			uniq = make(map[uint64]int)
		}
		for t := len(word)-3; t >= 0; t-- {
			term := [3]byte{word[t], word[t+1], word[t+2]}
			var bucket int
			if exact {
				bucket = len(word)-3			
			} else {
				bucket = i.private.Maxword-3
			}
			for ; bucket >= 0; bucket-- {
				if bucket >= len(i.private.Buckets) {
					continue
				}
				if len(i.private.Buckets[bucket]) < 2 {
					continue
				}
				count := quaternary.GetNum(i.private.Buckets[bucket], uint64(i.private.Logrows), string(term[:]) + "0")
				//println("Lookup:", string(term[:]) + "0", count)
				if count == 0 {
					continue
				}
				if count > i.private.Rows {
					continue
				}
				//println(word, count, "results")
				for c := uint64(1); c <= count; c++ {
					pos := quaternary.GetNum(i.private.Buckets[bucket], uint64(i.private.Logrows), string(term[:]) + fmt.Sprint(c))
					//println("Lookup:", string(term[:]) + fmt.Sprint(c), pos)
					if pos == 0 {
						continue
					}
					if pos > i.private.Rows {
						continue
					}
					if dedup {
						uniq[pos]++
					} else {
						//println(word, pos, "result")
						var k = string(quaternary.Get(i.private.Pk, i.private.Pkbits, pos))
						//println(string(term[:]), k, "yielded")
						if !yield(k) {
							return
						}
					}
				}
				if exact {
					break
				}
			}
		}
		if dedup {
			for pos, v := range uniq {
				if v + 3 >= len(word) {
					var k = string(quaternary.Get(i.private.Pk, i.private.Pkbits, pos))
					//println(string(term[:]), k, "yielded")
					if !yield(k) {
						return
					}
				}
			}
		}
	}
}
