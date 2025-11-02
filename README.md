# fulltext

`package fulltext` implements a **Fulltext Index** data structure in Go, designed for efficient lookup and retrieval of records by tokenized words.
It’s optimized for compact storage, fast lookups, and tunable tradeoffs between accuracy (false positives) and memory usage.

---

## 📖 Overview

The `fulltext` package provides a probabilistic full-text indexing mechanism over a set of primary keys and their corresponding word sets (“bag of words”).
It supports serialization/deserialization to JSON for persistence and efficient in-memory lookup of records by words or prefixes.

---

## ✨ Features

* 🔍 **Fast subword-based lookup** with optional exact word matching
* 🧮 **Compact index** representation using probabilistic filters
* 🧱 **Serializable** — easy to store and reload from JSON
* ⚙️ **Configurable false positive rate** for memory/speed trade-offs
* 🧩 **Results deduplication option** for slow or expensive data stores

---

## 📦 Installation

```bash
go get github.com/neurlang/fulltext
```

## 🧠 Concepts

### `BagOfWords`

A `BagOfWords` represents a set of unique words extracted from a document or record:

```go
type BagOfWords = map[string]struct{}
```

---

### `Index`

The `Index` structure holds the fulltext index data:

```go
type Index struct {
    // internal fields not exported
}
```

You can build a new index, serialize/deserialize it, append it, and perform lookups.

---

## 🛠 Usage

### Creating an Index

Use `New()` to build an index from a map of primary keys and their corresponding `BagOfWords`:

```go
package main

import "fmt"
import "log"
import "github.com/neurlang/fulltext"

func main() {
	dataMaps := map[string]fulltext.BagOfWords{
	    "doc1": {"golang": {}, "index": {}, "data": {}},
	    "doc2": {"search": {}, "engine": {}, "golang": {}},
	    "doc3": {"text": {}, "filter": {}, "query": {}},
	}

	idx, err := fulltext.New(nil, dataMaps, nil)
	if err != nil {
		log.Fatal(err)
	}

	iter := idx.Lookup("golang", true, true)

	for pk := range iter {
		fmt.Println("Found in:", pk)
	}
}
```

### 🧩 Example Output

```
Found in: doc1
Found in: doc2
```

---

### Looking Up Words

To look up all primary keys containing a given word:

```go
iter := idx.Lookup("golang", true, true)
```

Parameters:

* `exact` — if `true`, only exact word matches are considered. If `false`, a subword matches for the word may be found.
* `dedup` — if `true`, ensures each primary key is only yielded once (slower, but useful if your backing store is expensive to query).


---

### Serialization / Deserialization

You can serialize an index to JSON for persistence:

```go
data, err := idx.Serialize()
if err != nil {
    log.Fatal(err)
}

err = os.WriteFile("index.json", data, 0644)
if err != nil {
    log.Fatal(err)
}
```

To reload the index later:

```go
var idx fulltext.Index
data, _ := os.ReadFile("index.json")

if err := idx.Deserialize(data); err != nil {
    log.Fatal(err)
}
```

---

## ⚙️ Configuration Options

### `NewOpts`

```go
type NewOpts struct {
	// FalsePositiveFunctions tunes the false positive rate of the underlying filters. Default = 3.
	// Higher values (4+) consume more memory, but cause less false positive problems.
	// Lower values (0-3) increase the false positive rate, cause more false positive problems.
	FalsePositiveFunctions byte

	// BucketingExponent affects speed for large-scale indexes.
	// Higher values are slower to generate the index. Lower values are slower to search.
	BucketingExponent byte

	// MinShards affects speed of lookup for small tables
	MinShards byte

	// Shortest length of word that can be searched
	MinWordLength byte

	// Sync calls getter from one thread only
	Sync bool
}
```

---

## ⚠️ Errors

| Variable                   | Description                                      |
| -------------------------- | ------------------------------------------------ |
| `ErrFormatVersionMismatch` | Indicates an incompatible index format version   |
| `ErrNilGetter`             | Raised when `getter` function is `nil`           |
| `ErrNonuniform`            | Raised when primary keys are not of uniform size |

---

## 🧪 Notes

* Lookup supports words **with length ≥ 3**.
* Iteration may produce **false positives** depending on `FalsePositiveFunctions`.
* Deduplication mode can reduce redundant hits but adds overhead.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

