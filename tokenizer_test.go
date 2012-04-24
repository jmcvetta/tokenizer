// Copyright 2012 Jason McVetta.  This is Free Software, released under
// an MIT-style license.  See README.md for details.

package tokenizer

import (
	"fmt"
	"github.com/jmcvetta/goutil"
	"launchpad.net/mgo"
	"log"
	"testing"
)

// Tests tokenization 
func TestRoundTrip(t *testing.T) {
	log.SetFlags(log.Ltime | log.Lshortfile)
	session, err := mgo.Dial("localhost")
	if err != nil {
		t.Fatal(err)
	}
	db := session.DB("test_gokenizer_tokenizer")
	err = db.DropDatabase()
	if err != nil {
		t.Fatal(err)
	}
	tokenizer := NewMongoTokenizer(db)
	orig := goutil.RandAlphanumeric(8, 8)
	token := tokenizer.Tokenize(orig)
	var detok string // Result of detokenization - should be same as orig
	detok, err = tokenizer.Detokenize(token)
	if err != nil {
		t.Error(err)
	}
	if detok != orig {
		msg := "Detokenization failed: '%s' != '%s'."
		msg = fmt.Sprintf(msg, orig, detok)
		t.Error(msg)
	}
}

// Tests tokenization 
func BenchmarkRoundTrip(b *testing.B) {
	b.StopTimer()
	log.SetFlags(log.Ltime | log.Lshortfile)
	session, err := mgo.Dial("localhost")
	if err != nil {
		b.Error(err)
	}
	db := session.DB("test_gokenizer_tokenizer")
	err = db.DropDatabase()
	if err != nil {
		b.Error(err)
	}
	tokenizer := NewMongoTokenizer(db)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		orig := goutil.RandAlphanumeric(8, 8)
		token := tokenizer.Tokenize(orig)
		var detok string // Result of detokenization - should be same as orig
		detok, err = tokenizer.Detokenize(token)
		if err != nil {
			b.Error(err)
		}
		if detok != orig {
			msg := "Detokenization failed: '%s' != '%s'."
			msg = fmt.Sprintf(msg, orig, detok)
			b.Error(msg)
		}
	}
}
