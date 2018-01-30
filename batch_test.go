package batch_test

import (
	"sync"
	"testing"
	"time"

	"github.com/hourglassdesign/batch"
	"github.com/stretchr/testify/assert"
)

func BenchmarkBatch_Add(b *testing.B) {
	b.StopTimer()
	batch := batch.New(b.N, time.Duration(b.N))
	defer batch.Close()

	go func() {
		for range <-batch.Ready() {
		}
	}()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		batch.Add(b.N)
	}
}

func TestBatch_MaxWait(t *testing.T) {
	tt := []struct {
		MaxSize  int
		MaxWait  time.Duration
		AddItems int
	}{
		{10, time.Second * 2, 1},
		{10, time.Second * 2, 2},
		{10, time.Second * 2, 3},
		{10, time.Second * 2, 4},
		{10, time.Second * 2, 5},
	}

	for _, tc := range tt {
		b := batch.New(tc.MaxSize, tc.MaxWait)

		for i := 0; i < tc.AddItems; i++ {
			b.Add(i)
		}

		items := <-b.Ready()

		assert.Equal(t, tc.AddItems, len(items))
		b.Close()
	}
}

func TestBatch_MaxSize(t *testing.T) {
	tt := []struct {
		MaxSize  int
		MaxWait  time.Duration
		AddItems int
	}{
		{10, time.Second * 100, 10},
		{10, time.Second * 100, 20},
		{10, time.Second * 100, 30},
	}

	for _, tc := range tt {
		wg := &sync.WaitGroup{}
		b := batch.New(tc.MaxSize, tc.MaxWait)

		wg.Add(1)

		go func(t *testing.T, wg *sync.WaitGroup, batch *batch.Batch) {
			defer wg.Done()

			batches := tc.AddItems / tc.MaxSize

			for items := range batch.Ready() {
				assert.Equal(t, tc.MaxSize, len(items))
				batches--

				if batches == 0 {
					return
				}
			}
		}(t, wg, b)

		for i := 0; i < tc.AddItems; i++ {
			b.Add(i)
		}

		wg.Wait()
		b.Close()
	}
}
