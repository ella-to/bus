package sqlite_test

import (
	"context"
	"sync"
	"testing"

	"ella.to/bus.go/internal/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestSqliteWorker(t *testing.T) {
	const poolSize = 10
	const queueSize = 100
	const workerSize = poolSize
	const c = 200
	const total = 2000

	db, err := sqlite.New(sqlite.WithMemory(), sqlite.WithPoolSize(poolSize))
	assert.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	err = sqlite.RunScript(context.Background(), db, `CREATE TABLE IF NOT EXISTS mytests (name TEXT);`)
	assert.NoError(t, err)

	w := sqlite.NewWorker(db, queueSize, workerSize)

	var wg sync.WaitGroup

	wg.Add(c)

	for range c {
		go func() {
			defer wg.Done()
			for range total {
				w.Submit(func(conn *sqlite.Conn) {
					ctx := context.Background()
					stmt, err := conn.Prepare(ctx, `INSERT INTO mytests (name) VALUES (?);`, "hello world")
					if !assert.NoError(t, err) {
						return
					}

					defer stmt.Finalize()

					_, err = stmt.Step()
					assert.NoError(t, err)
				})
			}
		}()
	}

	wg.Wait()
}
