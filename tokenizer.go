// Copyright 2012 Jason McVetta.  This is Free Software, released under
// an MIT-style license.  See README.md for details.

// Package tokenizer implements a data tokenization service.
package tokenizer

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/jmcvetta/guid"
	"launchpad.net/mgo"
	"launchpad.net/mgo/bson"
	"log"
)

// A TokenNotFound error is returned by GetOriginal if the supplied token 
// string cannot be found in the database.
var TokenNotFound = errors.New("Token Not Found")

// Tokenizer generates tokens that represent, but are not programmatically 
// derived from, original text.
type Tokenizer interface {
	Tokenize(string) (string, error)   // Get a token
	Detokenize(string) (string, error) // Get the original text
}

// tokenRecord represents a token in the database.
type tokenRecord struct {
	Text  string
	Token string
}

// MongoTokenizer allows you to tokenize and detokenize strings.
type mongoTokenizer struct {
	db *mgo.Database
}

// Get the MongoDB collection object containing our tokens.
func (t mongoTokenizer) collection() *mgo.Collection {
	// lightweight operation, involves no network communication
	col := t.db.C("tokens")
	return col
}

// Fetches the token for string s from the database.
func (t mongoTokenizer) fetchToken(s string) (string, error) {
	log.Println("fetchToken:", s)
	var token string
	col := t.collection()
	result := tokenRecord{}
	err := col.Find(bson.M{"original": s}).One(&result)
	if err == nil {
		token = result.Token
	}
	return token, err
}

func (t mongoTokenizer) Tokenize(s string) (string, error) {
	log.Println("Tokenize:", s)
	var result string
	var err error
	col := t.collection()
	for {
		// 
		// First check for an existing token
		//
		var token string
		token, err = t.fetchToken(s)
		if err == nil {
			log.Println("Existing token:", token)
			result = token
			break
		}
		if err != mgo.NotFound {
			// NotFound is harmless - anything else is WTF
			break // Will return a nil result and a non-nil error
		}
		log.Println("No existing token.")
		//
		// No existing token found, so generate a new token
		//
		// TODO: Instead of using top-level NextId(), each Tokenizer should 
		// have its own guid.Generator, which can be configurable with 
		// datacenter & worker IDs.  Once that is in place we should be 
		// guaranteed against guid collision even when running multiple
		// uncoordinated tokenizers.
		//
		guid, err := guid.NextId()
		// We return MongoDB errors because the caller might reasonably want
		// to deal with them.  However the caller almost certainly can't deal
		// with an error caused by guid.NextId().
		if err != nil {
			log.Panic(err)
		}
		guidstr := fmt.Sprintf("%v", guid)
		token = base64.StdEncoding.EncodeToString([]byte(guidstr))
		trec := tokenRecord{
			Text:  s,
			Token: token,
		}
		log.Println(trec)
		err = col.Insert(&trec)
		if err == nil {
			// Success!
			log.Println("New token:", token)
			result = token
			break
		}
		// MongoDB error code 11000 = duplicate key error Either the token or
		// the original are already in the DB, possibly put there by a
		// different tokenizer process.  The original may have already been 
		// tokenized by another process, or (less likely) there may have been a 
		// guid collision.  Either way, let's try again.
		if e, ok := err.(*mgo.LastError); ok && e.Code == 11000 {
			log.Println("Duplicate key")
			log.Println(e)
			continue
		}
		break // Will return a nil result and a non-nil error
	}
	return result, err
}

func (t mongoTokenizer) Detokenize(s string) (string, error) {
	log.Println("Detokenize:", s)
	log.Println("  Token:      " + s)
	var orig string
	var err error
	col := t.collection()
	result := tokenRecord{}
	query := col.Find(bson.M{"token": s})
	switch db_err := query.One(&result); true {
	case db_err == mgo.NotFound:
		log.Println("Token not found in DB")
		err = TokenNotFound
		return orig, err
	case db_err != nil:
		log.Panic(err)
	}
	log.Println(result)
	orig = result.Text
	log.Println("Found original for token: " + orig)
	return orig, err
}

// NewMongoTokenizer returns a Tokenizer backed by a MongDB database
func NewMongoTokenizer(db *mgo.Database) Tokenizer {
	//
	// Setup database.  If DB is already setup, this is a noop.
	//
	col := db.C("tokens")
	col.EnsureIndex(mgo.Index{
		Key:      []string{"original"},
		Unique:   true,
		DropDups: false,
		Sparse:   true,
	})
	col.EnsureIndex(mgo.Index{
		Key:      []string{"token"},
		Unique:   true,
		DropDups: false,
		Sparse:   true,
	})
	//
	// Initialize tokenizer
	//
	t := mongoTokenizer{
		db: db,
	}
	return t
}
