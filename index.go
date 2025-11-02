// package fulltext implements a Fulltext Index data structure for Golang
package fulltext

import quaternary "github.com/neurlang/quaternary/v1"
import "fmt"

type BagOfWords = map[string]struct{}

type index struct {
	Version byte     `json:"version"`
	Pk      []byte   `json:"pk"`
	Buckets [][]byte `json:"buckets"`
	Pkbits  uint64   `json:"pkbits"`
	Rows    uint64   `json:"rows"`
	Logrows byte     `json:"logrows"`
	Maxword int      `json:"maxword"`
}

type Index struct {
	private []index
}

type NewOpts struct {
	// FalsePositiveFunctions tunes the false positive rate of the underlying filters. Default = 10.
	// Higher values (10+) consume more memory, but cause less false positive problems.
	// Lower values (0-9) increase the false positive rate, cause more false positive problems.
	FalsePositiveFunctions byte

	// BucketingExponent affects speed for large-scale indexes.
	// Higher values are slower to generate the index. Lower values are slower to search.
	BucketingExponent byte
}

var ErrNonuniform = fmt.Errorf("nonuniform_key_size")
var ErrNilGetter = fmt.Errorf("nil_getter")


// Append is O(1) but NOT a thread safe operation. Use external synchronization to protect mutation of the index.
func (i *Index) Append(j *Index) *Index {
	i.private = append(i.private, j.private...)
	return i
}

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
			BucketingExponent:      13,
		}
	}
	i = new(Index)
	i.private = make([]index, (len(primaryKeys)>>opts.BucketingExponent)+1, (len(primaryKeys)>>opts.BucketingExponent)+1)
	for current := range i.private {
		i.private[current].Version = 1
	}
	var ikeys = make(map[int]string, 1<<opts.BucketingExponent)
	var keys_len int
	var current int
	countBag := make(map[[3]byte]uint64)
	initialBag := make(map[string]uint64)
	for k := range primaryKeys {
		if keys_len == 0 {
			keys_len = len(k)
		} else if keys_len != len(k) {
			return nil, ErrNonuniform
		}
		size := len(ikeys) + 1
		ikeys[size] = k
		bag := getter(k)
		for word := range bag {
			if len(word) > i.private[current].Maxword {
				i.private[current].Maxword = len(word)
			}
			if len(word) < 3 {
				continue
			}
			for len(word)-3 >= len(i.private[current].Buckets) {
				i.private[current].Buckets = append(i.private[current].Buckets, nil)
			}
			wrd := [3]byte{word[0], word[1], word[2]}
			countBag[wrd]++
			cnt := countBag[wrd]
			initialBag[word[:3]+fmt.Sprint(cnt)] = uint64(size)
		}
		if (size >> opts.BucketingExponent) != 0 {
			//println("flush", current)
			for k, v := range countBag {
				//println("countBag:", string(k[:])+"0", v)
				initialBag[string(k[:])+"0"] = v
			}
			i.private[current].Rows = uint64(len(ikeys))
			for j := i.private[current].Rows; j > 0; j >>= 1 {
				i.private[current].Logrows++
			}
			if i.private[current].Rows > 0 {
				i.private[current].Pkbits = uint64(len(ikeys[1])) * 8
			}
			//println(i.private[current].Pkbits)
			if i.private[current].Pkbits <= 255 {
				i.private[current].Pk = quaternary.Make(ikeys, byte(i.private[current].Pkbits))
			} else {
				i.private[current].Pk = quaternary.Make(ikeys, 0)
			}
			i.private[current].Buckets[0] = quaternary.New(initialBag, i.private[current].Logrows, opts.FalsePositiveFunctions)
			ikeys = make(map[int]string, 1<<opts.BucketingExponent)
			countBag = make(map[[3]byte]uint64)
			initialBag = make(map[string]uint64)
			current++
		}
	}
	for k, v := range countBag {
		//println("countBag:", string(k[:])+"0", v)
		initialBag[string(k[:])+"0"] = v
	}
	countBag = nil
	primaryKeys = nil
	last := current
	//println("flush", last, "last")
	i.private[last].Rows = uint64(len(ikeys))
	for j := i.private[last].Rows; j > 0; j >>= 1 {
		i.private[last].Logrows++
	}
	if i.private[last].Rows > 0 {
		i.private[last].Pkbits = uint64(len(ikeys[1])) * 8
	}
	if i.private[last].Pkbits <= 255 {
		i.private[last].Pk = quaternary.Make(ikeys, byte(i.private[last].Pkbits))
	} else {
		i.private[last].Pk = quaternary.Make(ikeys, 0)
	}
	ikeys = nil
	if len(i.private[last].Buckets) > 0 {
		i.private[last].Buckets[0] = quaternary.New(initialBag, i.private[last].Logrows, opts.FalsePositiveFunctions)
	}
	i.private = i.private[:last+1]
	initialBag = nil
	var more bool
	for curr := range i.private {
		if len(i.private[curr].Buckets) >= 0 {
			more = true
			break
		}
	}
	if !more {
		return
	}
	//println("Length", len(i.private))
	for curr := range i.private {
		//println("Maxword", i.private[curr].Maxword)
		for q := 0; q+3 < i.private[curr].Maxword; q++ {
			countBag := make(map[[3]byte]uint64)
			initialBag := make(map[string]uint64)
			for j := uint64(1); j <= i.private[curr].Rows; j++ {
				var k = string(quaternary.Get(i.private[curr].Pk, i.private[curr].Pkbits, j))
				bag := getter(k)
				for word := range bag {
					//println("key:",k, word)
					if len(word) <= 3+q {
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
			i.private[curr].Buckets[1+q] = quaternary.New(initialBag, i.private[curr].Logrows, opts.FalsePositiveFunctions)
		}
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
		for current := range i.private {
			if i.private[current].Rows == 0 {
				continue
			}
			var uniq map[uint64]int
			if dedup {
				uniq = make(map[uint64]int)
			}
			for t := len(word) - 3; t >= 0; t-- {
				term := [3]byte{word[t], word[t+1], word[t+2]}
				var bucket int
				if exact {
					bucket = len(word) - 3
				} else {
					bucket = i.private[current].Maxword - 3
				}
				for ; bucket >= 0; bucket-- {
					if bucket >= len(i.private[current].Buckets) {
						continue
					}
					if len(i.private[current].Buckets[bucket]) < 2 {
						continue
					}
					count := quaternary.GetNum(i.private[current].Buckets[bucket], uint64(i.private[current].Logrows), string(term[:])+"0")
					//println("Lookup:", string(term[:]) + "0", count)
					if count == 0 {
						continue
					}
					if count > i.private[current].Rows {
						continue
					}
					//println(word, count, "results")
					for c := uint64(1); c <= count; c++ {
						pos := quaternary.GetNum(i.private[current].Buckets[bucket], uint64(i.private[current].Logrows), string(term[:])+fmt.Sprint(c))
						//println("Lookup:", string(term[:]) + fmt.Sprint(c), pos)
						if pos == 0 {
							//println("pos == 0")
							continue
						}
						if pos > i.private[current].Rows {
							//println("pos > rows")
							continue
						}
						if dedup {
							uniq[pos]++
						} else {
							//println(word, pos, "result")
							var k = string(quaternary.Get(i.private[current].Pk, i.private[current].Pkbits, pos))
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
					if v+3 >= len(word) {
						var k = string(quaternary.Get(i.private[current].Pk, i.private[current].Pkbits, pos))
						//println(string(term[:]), k, "yielded")
						if !yield(k) {
							return
						}
					}
				}
			}
		}
	}
}
