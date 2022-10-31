package dataloader

import (
	"context"
	"sync"
	"time"
)

// Config for a generic dataloader.
type Config[T any] struct {
	// Fetch sets the function for fetching data.
	Fetch func(ctx context.Context, keys []string) ([]T, error)
	// Wait sets the duration to wait before fetching data.
	Wait time.Duration
	// MaxBatch sets the max batch size when fetching data.
	MaxBatch int
}

// Dataloader is a generic dataloader.
type Dataloader[T any] struct {
	ctx    context.Context
	config Config[T]
	mu     sync.Mutex // protects mutable state below
	cache  map[string]T
	batch  *dataloaderBatch[T]
}

// New creates a new dataloader.
func New[T any](
	ctx context.Context,
	config Config[T],
) *Dataloader[T] {
	return &Dataloader[T]{
		ctx:    ctx,
		config: config,
	}
}

type dataloaderBatch[T any] struct {
	ctx     context.Context
	keys    []string
	data    []T
	err     error
	closing bool
	done    chan struct{}
}

// Load a result by key, batching and caching will be applied automatically.
func (l *Dataloader[T]) Load(key string) (T, error) {
	return l.LoadThunk(key)()
}

// LoadThunk returns a function that when called will block waiting for a result.
// This method should be used if you want one goroutine to make requests to
// different data loaders without blocking until the thunk is called.
func (l *Dataloader[T]) LoadThunk(key string) func() (T, error) {
	l.mu.Lock()
	if it, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return func() (T, error) {
			return it, nil
		}
	}
	if l.batch == nil {
		l.batch = &dataloaderBatch[T]{ctx: l.ctx, done: make(chan struct{})}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)
	l.mu.Unlock()
	return func() (T, error) {
		<-batch.done
		var data T
		if pos < len(batch.data) {
			data = batch.data[pos]
		}
		if batch.err == nil {
			l.mu.Lock()
			l.unsafeSet(key, data)
			l.mu.Unlock()
		}
		return data, batch.err
	}
}

// LoadAll fetches many keys at once.
// It will be broken into appropriately sized sub-batches based on how the dataloader is configured.
func (l *Dataloader[T]) LoadAll(keys []string) ([]T, error) {
	results := make([]func() (T, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	values := make([]T, len(keys))
	var err error
	for i, thunk := range results {
		values[i], err = thunk()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

// LoadAllThunk returns a function that when called will block waiting for results.
// This method should be used if you want one goroutine to make requests to many
// different data loaders without blocking until the thunk is called.
func (l *Dataloader[T]) LoadAllThunk(keys []string) func() ([]T, error) {
	results := make([]func() (T, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	return func() ([]T, error) {
		values := make([]T, len(keys))
		var err error
		for i, thunk := range results {
			values[i], err = thunk()
			if err != nil {
				return nil, err
			}
		}
		return values, nil
	}
}

func (l *Dataloader[T]) unsafeSet(key string, value T) {
	if l.cache == nil {
		l.cache = map[string]T{}
	}
	l.cache[key] = value
}

// keyIndex will return the location of the key in the batch, if its not found
// it will add the key to the batch.
func (b *dataloaderBatch[T]) keyIndex(l *Dataloader[T], key string) int {
	for i, existingKey := range b.keys {
		if key == existingKey {
			return i
		}
	}
	pos := len(b.keys)
	b.keys = append(b.keys, key)
	if pos == 0 {
		go b.startTimer(l)
	}
	if l.config.MaxBatch != 0 && pos >= l.config.MaxBatch-1 {
		if !b.closing {
			b.closing = true
			l.batch = nil
			go b.end(l)
		}
	}
	return pos
}

func (b *dataloaderBatch[T]) startTimer(l *Dataloader[T]) {
	// TODO: Respect context.
	time.Sleep(l.config.Wait)
	l.mu.Lock()
	// we must have hit a batch limit and are already finalizing this batch
	if b.closing {
		l.mu.Unlock()
		return
	}
	l.batch = nil
	l.mu.Unlock()
	b.end(l)
}

func (b *dataloaderBatch[T]) end(l *Dataloader[T]) {
	b.data, b.err = l.config.Fetch(b.ctx, b.keys)
	close(b.done)
}
