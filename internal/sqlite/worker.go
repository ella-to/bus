package sqlite

import "context"

type task struct {
	fn   func(conn *Conn)
	done chan struct{}
}

type Worker struct {
	db    *Database
	queue chan *task
}

func (s *Worker) Submit(fn func(conn *Conn)) {
	task := &task{
		fn:   fn,
		done: make(chan struct{}, 1),
	}

	s.queue <- task
	<-task.done
}

func (s *Worker) Close() {
	close(s.queue)
}

func NewWorker(db *Database, queueSize int64, workerSize int64) *Worker {
	w := &Worker{
		db:    db,
		queue: make(chan *task, queueSize),
	}

	for i := 0; i < int(workerSize); i++ {
		go func() {
			for task := range w.queue {
				func() {
					ctx := context.Background()
					conn, err := w.db.Conn(ctx)
					if err != nil {
						return
					}
					defer conn.Close()

					task.fn(conn)
					task.done <- struct{}{}
				}()
			}
		}()
	}

	return w
}
