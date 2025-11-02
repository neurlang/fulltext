// package fulltext implements a Fulltext Index data structure for Golang
package fulltext

import quaternary "github.com/neurlang/quaternary/v1"
import "fmt"
import "reflect"
import "sync"

type BagOfWords = map[string]struct{}

type index struct {
	Version byte     `json:"version"`
	Pk      []byte   `json:"pk"`
	Buckets [][]byte `json:"buckets"`
	Counts  [][]byte `json:"counts"`
	Pkbits  uint64   `json:"pkbits"`
	Rows    uint64   `json:"rows"`
	Logrows byte     `json:"logrows"`
	Maxword int      `json:"maxword"`
	MinWord byte     `json:"minword"`
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

	// Shortest length of word that can be searched
	MinWordLength byte
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
func New[V struct{} | BagOfWords | []string](opts *NewOpts, data map[string]V, getter func(primaryKey string) BagOfWords) (i *Index, err error) {
	if getter == nil {
		vType := reflect.TypeOf(data[""])
		if vType.Kind() == reflect.Struct {
			return nil, ErrNilGetter
		} else if vType.Kind() == reflect.Slice {
			dataClone := data
			getter = func(pk string) BagOfWords {
				var slice = (interface{}(dataClone[pk])).([]string)
				var bag = make(BagOfWords, len(slice))
				for _, v := range slice {
					bag[v] = struct{}{}
				}
				return bag
			}
		} else {
			dataClone := data
			getter = func(pk string) BagOfWords {
				return (interface{}(dataClone[pk])).(BagOfWords)
			}
		}
	}
	if opts == nil {
		// defaults
		opts = &NewOpts{
			FalsePositiveFunctions: 10,
			BucketingExponent:      13,
			MinWordLength:          3,
		}
	}
	var wg sync.WaitGroup
	i = new(Index)
	i.private = make([]index, (len(data)>>opts.BucketingExponent)+1, (len(data)>>opts.BucketingExponent)+1)
	for current := range i.private {
		i.private[current].Version = 2
		i.private[current].MinWord = opts.MinWordLength
	}
	var ikeys = make(map[int]string, 1<<opts.BucketingExponent)
	var keys_len int
	var current int
	countBag := make(map[string]uint64)
	initialBag := make(map[string]uint64)
	for k := range data {
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
			if len(word) < int(opts.MinWordLength) {
				continue
			}
			for len(word)-int(opts.MinWordLength) >= len(i.private[current].Buckets) {
				i.private[current].Buckets = append(i.private[current].Buckets, nil)
				i.private[current].Counts = append(i.private[current].Counts, nil)
			}
			wrd := word[0:int(opts.MinWordLength)]
			countBag[wrd]++
			cnt := countBag[wrd]
			initialBag[wrd+fmt.Sprint(cnt)] = uint64(size)
		}
		if (size >> opts.BucketingExponent) != 0 {
			wg.Add(1)
			go func(ikeys map[int]string, countBag map[string]uint64, initialBag map[string]uint64, current int) {
				//println("flush", current)
				i.private[current].Rows = uint64(len(ikeys))
				for j := i.private[current].Rows; j > 0; j >>= 1 {
					i.private[current].Logrows++
				}
				if i.private[current].Rows > 0 {
					i.private[current].Pkbits = uint64(len(ikeys[1])) * 8
				}
				//println(i.private[current].Pkbits)
				if i.private[current].Pkbits <= 255 {
					i.private[current].Pk = quaternary.New(ikeys, byte(i.private[current].Pkbits), 0)
				} else {
					i.private[current].Pk = quaternary.New(ikeys, 0, 0)
				}
				i.private[current].Buckets[0] = quaternary.New(initialBag, i.private[current].Logrows, 0)
				i.private[current].Counts[0] = quaternary.New(countBag, i.private[current].Logrows, opts.FalsePositiveFunctions)
				wg.Done()
			}(ikeys, countBag, initialBag, current)
			ikeys = make(map[int]string, 1<<opts.BucketingExponent)
			countBag = make(map[string]uint64)
			initialBag = make(map[string]uint64)
			current++
		}
	}
	data = nil
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
		i.private[last].Pk = quaternary.New(ikeys, byte(i.private[last].Pkbits), 0)
	} else {
		i.private[last].Pk = quaternary.New(ikeys, 0, 0)
	}
	ikeys = nil
	if len(i.private[last].Buckets) > 0 {
		i.private[last].Buckets[0] = quaternary.New(initialBag, i.private[last].Logrows, 0)
		i.private[last].Counts[0] = quaternary.New(countBag, i.private[last].Logrows, opts.FalsePositiveFunctions)
	}
	i.private = i.private[:last+1]
	countBag = nil
	initialBag = nil
	wg.Wait()
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
	wg = sync.WaitGroup{}
	//println("Length", len(i.private))
	for curr := range i.private {
		//println("Maxword", i.private[curr].Maxword)
		for q := 0; q+int(opts.MinWordLength) < i.private[curr].Maxword; q++ {
			wg.Add(1)
			go func(curr, q int) {
				countBag := make(map[string]uint64)
				initialBag := make(map[string]uint64)
				for j := uint64(1); j <= i.private[curr].Rows; j++ {
					var k = string(quaternary.Get(i.private[curr].Pk, i.private[curr].Pkbits, j))
					bag := getter(k)
					for word := range bag {
						//println("key:",k, word)
						if len(word) <= int(opts.MinWordLength)+q {
							continue
						}
						wrd := word[1+q : 1+int(opts.MinWordLength)+q]
						countBag[wrd]++
						cnt := countBag[wrd]
						initialBag[wrd+fmt.Sprint(cnt)] = j
					}
				}
				i.private[curr].Buckets[1+q] = quaternary.New(initialBag, i.private[curr].Logrows, 0)
				i.private[curr].Counts[1+q] = quaternary.New(countBag, i.private[curr].Logrows, opts.FalsePositiveFunctions)
				wg.Done()
			}(curr, q)
		}
	}
	wg.Wait()
	return
}

// Lookup iterates the fulltext search index based on a specific word with length of opts.MinWordLength characters or more.
// Exact finds exact word matches (faster). Dedup hits each primary key exactly once (slower, but can be worth it if db is slow).
// Iterator can (in rare cases) have false positives.
func (i *Index) Lookup(word string, exact, dedup bool) func(yield func(primaryKey string) bool) {
	return func(yield func(string) bool) {
		for current := range i.private {
			var minWord int
			if i.private[current].Version <= 1 {
				minWord = 3
			} else {
				minWord = int(i.private[current].MinWord)
			}
			if len(word) < minWord {
				continue
			}
			if i.private[current].Rows == 0 {
				continue
			}
			var uniq map[uint64]int
			if dedup {
				uniq = make(map[uint64]int)
			}
			for t := len(word) - minWord; t >= 0; t-- {
				term := word[t : t+minWord]
				var bucket int
				if exact {
					bucket = len(word) - minWord
				} else {
					bucket = i.private[current].Maxword - minWord
				}
				for ; bucket >= 0; bucket-- {
					if bucket >= len(i.private[current].Buckets) {
						continue
					}
					var count uint64
					if i.private[current].Version <= 1 {
						if len(i.private[current].Buckets[bucket]) < 2 {
							continue
						}
						count = quaternary.GetNum(i.private[current].Buckets[bucket], uint64(i.private[current].Logrows), term+"0")
					} else {
						if len(i.private[current].Counts[bucket]) < 2 {
							continue
						}
						count = quaternary.GetNum(i.private[current].Counts[bucket], uint64(i.private[current].Logrows), term)
					}
					//println("Lookup:", string(term[:]) + "0", count)
					if count == 0 {
						continue
					}
					if count > i.private[current].Rows {
						continue
					}
					//println(word, count, "results")
					for c := uint64(1); c <= count; c++ {
						pos := quaternary.GetNum(i.private[current].Buckets[bucket], uint64(i.private[current].Logrows), term+fmt.Sprint(c))
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
					if v+minWord >= len(word) {
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
