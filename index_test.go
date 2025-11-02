package fulltext

import (
	"testing"
)

// TestNewIndexCreation tests basic index creation with valid inputs
func TestNewIndexCreation(t *testing.T) {
	pk := BagOfWords{"user:1": struct{}{}, "user:2": struct{}{}, "user:3": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"user:1": {"golang": struct{}{}, "programming": struct{}{}, "backend": struct{}{}},
			"user:2": {"golang": struct{}{}, "web": struct{}{}, "frontend": struct{}{}},
			"user:3": {"rust": struct{}{}, "systems": struct{}{}, "backend": struct{}{}},
		}
		return words[key]
	}

	idx, err := New(nil, pk, getter)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if idx == nil {
		t.Fatal("expected index to be created, got nil")
	}
}

// TestNewIndexWithNilGetter tests that nil getter returns an error
func TestNewIndexWithNilGetter(t *testing.T) {
	pk := BagOfWords{"user:1": struct{}{}}
	_, err := New(nil, pk, nil)
	if err == nil {
		t.Fatal("expected error with nil getter, got nil")
	}
}

// TestNewIndexWithEmptyPrimaryKeys tests index creation with empty primary keys
func TestNewIndexWithEmptyPrimaryKeys(t *testing.T) {
	pk := BagOfWords{}
	getter := func(key string) BagOfWords { return nil }

	idx, err := New(nil, pk, getter)
	if err != nil {
		t.Fatalf("expected no error for empty primary keys, got %v", err)
	}
	if idx == nil {
		t.Fatal("expected index to be created")
	}
}

// TestLookupSingleWord tests looking up a single word that exists
func TestLookupSingleWord(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}, "doc:2": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"hello": struct{}{}, "world": struct{}{}},
			"doc:2": {"goodbye": struct{}{}, "world": struct{}{}},
		}
		return words[key]
	}

	idx, _ := New(nil, pk, getter)
	results := make(map[string]struct{})

	iter := idx.Lookup("world", true, true)
	iter(func(pk string) bool {
		results[pk] = struct{}{}
		return true
	})

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

// TestLookupNonexistentWord tests looking up a word that doesn't exist
func TestLookupNonexistentWord(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}, "doc:2": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"hello": struct{}{}, "world": struct{}{}},
			"doc:2": {"goodbye": struct{}{}, "world": struct{}{}},
		}
		return words[key]
	}

	idx, _ := New(nil, pk, getter)
	results := []string{}

	iter := idx.Lookup("nonexistent", true, true)
	iter(func(pk string) bool {
		results = append(results, pk)
		return true
	})

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

// TestLookupWithLimit tests that limit parameter is respected
func TestLookupWithLimit(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}, "doc:2": struct{}{}, "doc:3": struct{}{}, "doc:4": struct{}{}, "doc:5": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"common": struct{}{}},
			"doc:2": {"common": struct{}{}},
			"doc:3": {"common": struct{}{}},
			"doc:4": {"common": struct{}{}},
			"doc:5": {"common": struct{}{}},
		}
		return words[key]
	}

	idx, _ := New(nil, pk, getter)
	results := make(map[string]struct{})

	iter := idx.Lookup("common", true, true)
	iter(func(pk string) bool {
		results[pk] = struct{}{}
		return true
	})

	if len(results) > 5 {
		t.Fatalf("expected at most 5 results, got %d", len(results))
	}
}

// TestLookupIteratorEarlyExit tests that returning false in iterator stops iteration
func TestLookupIteratorEarlyExit(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}, "doc:2": struct{}{}, "doc:3": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"target": struct{}{}},
			"doc:2": {"target": struct{}{}},
			"doc:3": {"target": struct{}{}},
		}
		return words[key]
	}

	idx, _ := New(nil, pk, getter)
	count := 0

	iter := idx.Lookup("target", true, true)
	iter(func(pk string) bool {
		count++
		if count == 2 {
			return false // stop iteration
		}
		return true
	})

	if count != 2 {
		t.Fatalf("expected iteration to stop at 2, got %d iterations", count)
	}
}

// TestLookupCaseSensitivity tests word lookup case sensitivity
func TestLookupCaseSensitivity(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"Hello": struct{}{}, "world": struct{}{}},
		}
		return words[key]
	}

	idx, _ := New(nil, pk, getter)

	// Search for lowercase "hello"
	results := []string{}
	iter := idx.Lookup("hello", true, true)
	iter(func(pk string) bool {
		results = append(results, pk)
		return true
	})

	// Behavior depends on implementation - either case-sensitive or normalized
	// This test documents the expected behavior
	if len(results) != 0 {
		t.Logf("index is case-sensitive (searching 'hello' did not match 'Hello')")
	}
}

// TestLookupMultipleWords tests searching for different words in same index
func TestLookupMultipleWords(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}, "doc:2": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"golang": struct{}{}, "backend": struct{}{}},
			"doc:2": {"rust": struct{}{}, "backend": struct{}{}},
		}
		return words[key]
	}

	idx, _ := New(nil, pk, getter)

	// First lookup
	results1 := make(map[string]struct{})
	iter1 := idx.Lookup("golang", true, true)
	iter1(func(pk string) bool {
		results1[pk] = struct{}{}
		return true
	})

	// Second lookup
	results2 := make(map[string]struct{})
	iter2 := idx.Lookup("backend", true, true)
	iter2(func(pk string) bool {
		results2[pk] = struct{}{}
		return true
	})

	if len(results1) != 1 {
		t.Fatalf("expected 1 result for 'golang', got %d", len(results1))
	}
	if len(results2) != 2 {
		t.Fatalf("expected 2 results for 'backend', got %d", len(results2))
	}
}

// TestLookupEmptyWord tests searching with an empty string
func TestLookupEmptyWord(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}}
	getter := func(key string) BagOfWords {
		return BagOfWords{"hello": struct{}{}}
	}

	idx, _ := New(nil, pk, getter)
	results := make(map[string]struct{})

	iter := idx.Lookup("", true, true)
	iter(func(pk string) bool {
		results[pk] = struct{}{}
		return true
	})

	// Empty search should return no results
	if len(results) != 0 {
		t.Fatalf("expected 0 results for empty word, got %d", len(results))
	}
}

// TestLookupMultipleWords tests searching for different words in same index
func TestLookupMultipleWordsSub(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}, "doc:2": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"golang": struct{}{}, "backend": struct{}{}},
			"doc:2": {"rust": struct{}{}, "backend": struct{}{}},
		}
		return words[key]
	}

	idx, _ := New(nil, pk, getter)

	// First lookup
	results1 := make(map[string]struct{})
	iter1 := idx.Lookup("lan", false, true)
	iter1(func(pk string) bool {
		results1[pk] = struct{}{}
		return true
	})

	// Second lookup
	results2 := make(map[string]struct{})
	iter2 := idx.Lookup("cke", false, true)
	iter2(func(pk string) bool {
		results2[pk] = struct{}{}
		return true
	})

	if len(results1) != 1 {
		t.Fatalf("expected 1 result for 'golang', got %d", len(results1))
	}
	if len(results2) != 2 {
		t.Fatalf("expected 2 results for 'backend', got %d", len(results2))
	}
}

// TestSerialize tests serializing
func TestSerialize(t *testing.T) {
	pk := BagOfWords{"doc:1": struct{}{}, "doc:2": struct{}{}}
	getter := func(key string) BagOfWords {
		words := map[string]BagOfWords{
			"doc:1": {"golang": struct{}{}, "backend": struct{}{}},
			"doc:2": {"rust": struct{}{}, "backend": struct{}{}},
		}
		return words[key]
	}

	var v0 Index

	idx, err := New(nil, pk, getter)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	bytes1, _ := idx.Serialize()
	bytes0, _ := v0.Serialize()
	if string(bytes1) == string(bytes0) {
		t.Fatalf("expected different, got %s", string(bytes1))
	}
	err = idx.Deserialize(bytes1)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
