// Package bolter is a boltdb helper package and extends functionality provided by boltdb.
// Extends functionality of cursor to include `Pagination`
package bolter

import (
	"errors"

	"github.com/boltdb/bolt"
)

// Order type is set of record retreival orders
type Order int

const (
	// Desc is descending `Order`
	Desc Order = iota
	// Asc is ascending `Order`
	Asc
)

var (
	// ErrPaginationExlucde should be returned in foreachFn (which is passed to Pagination.ForEach)
	// if the corresponding record shouldn't decrement Pagination.Limit
	ErrPaginationExlucde = errors.New("Don't decrement Pagination.Limit for this record")
)

// Pagination is serializable pagination type
type Pagination struct {
	key          []byte
	LastKey      []byte
	Limit        int
	ExcludeFirst bool
	Order        `json:"omit"`
}

// NewPagination creates Pagination type with defaults, other public
// fields can be overridden using ".Notion". forEachFn has type
// `func (key []byte, value []byte) error`
func NewPagination(key []byte, count int, order Order) *Pagination {
	return &Pagination{
		key:          key,
		Limit:        count,
		ExcludeFirst: true,
		Order:        order,
	}
}

type forEachFn func([]byte, []byte) error

// ForEach iterates through the cursor starting from first element
func (p *Pagination) ForEach(cursor *bolt.Cursor, fn forEachFn) error {
	var (
		err  error
		l    int
		k, v []byte
	)

	for k, v = p.seekToFirst(cursor); k != nil && l < p.Limit; k, v = p.next(cursor) {
		nk := make([]byte, len(k))
		nv := make([]byte, len(v))
		err = fn(
			append(nk, k...),
			append(nv, v...),
		)
		if err == nil {
			l++
		} else if err != ErrPaginationExlucde {
			return err
		}
	}

	p.LastKey = k

	return nil
}

// NextPage returns pagination starting from LastKey
func (p *Pagination) NextPage(count int, order Order) *Pagination {
	return &Pagination{
		key:          p.LastKey,
		ExcludeFirst: true,
		Limit:        count,
		Order:        order,
	}
}

func (p *Pagination) seekToFirst(cursor *bolt.Cursor) ([]byte, []byte) {
	if p.key == nil {
		if p.Order == Asc {
			return cursor.First()
		}
		return cursor.Last()
	}

	k, v := cursor.Seek(p.key)
	if p.ExcludeFirst {
		return p.next(cursor)
	}
	return k, v
}

func (p *Pagination) next(cursor *bolt.Cursor) ([]byte, []byte) {
	if p.Order == Asc {
		return cursor.Next()
	}
	return cursor.Prev()
}
