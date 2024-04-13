package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var (
	ErrNotFound    = errors.New("database row not found")
	ErrPrepareSQL  = errors.New("database failed to prepare sql")
	ErrExecSQL     = errors.New("database failed to exec sql")
	ErrUnknownType = errors.New("database failed to prepare sql because of unknown type")
)

// Reason behind this is that I don't want to import two packages that
// starts with sqlite into my project. I can use the type alias in my project
type Stmt = sqlite.Stmt
type FunctionImpl = sqlite.FunctionImpl
type Context = sqlite.Context
type Value = sqlite.Value
type AggregateFunction = sqlite.AggregateFunction

var IntegerValue = sqlite.IntegerValue

func Sql(sql string, values ...any) string {
	return fmt.Sprintf(sql, values...)
}

type Conn struct {
	id   int
	conn *sqlite.Conn
	db   *Database
}

// When your trie to use transaction in a nice way, you can use the following
// at the beginning of your code:
//
// defer conn.Save(&err)()
func (c *Conn) Save(err *error) func() {
	fn := sqlitex.Save(c.conn)
	return func() {
		fn(err)
	}
}

func (c *Conn) Function(name string, impl *FunctionImpl) error {
	return c.conn.CreateFunction(name, impl)
}

func (c *Conn) ExecScript(sql string) error {
	return sqlitex.ExecScript(c.conn, strings.TrimSpace(sql))
}

func (c *Conn) Close() {
	c.db.pool.Put(c.conn)
	poolSize := atomic.AddInt64(&c.db.remaining, 1)
	slog.Debug("put connection back to pool", "conn_id", c.id, "pool_size", poolSize)
}

func (c *Conn) Prepare(ctx context.Context, sql string, values ...any) (*Stmt, error) {
	slog.Debug("prepare sql", "sql", ShowSql(sql, values...))

	stmt, err := c.conn.Prepare(strings.TrimSpace(sql))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPrepareSQL, err)
	}

	for i, value := range values {
		i++ // bind starts from 1

		if value == nil {
			stmt.BindNull(i)
			continue
		}

		valueType := reflect.TypeOf(value)

		switch valueType.Kind() {
		case reflect.Slice:
			if valueType.Elem().Kind() == reflect.Uint8 {
				blob, ok := value.([]byte)
				if !ok {
					blob = value.(json.RawMessage)
				}
				stmt.BindZeroBlob(i, int64(len(blob)))
				stmt.BindBytes(i, blob)
				continue
			}
			fallthrough
		case reflect.Map:
			var buffer bytes.Buffer
			err = json.NewEncoder(&buffer).Encode(value)
			if err != nil {
				return nil, err
			}
			stmt.BindText(i, buffer.String())
			continue
		case reflect.String:
			stmt.BindText(i, reflect.ValueOf(value).String())
			continue
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			stmt.BindInt64(i, reflect.ValueOf(value).Int())
			continue
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			stmt.BindInt64(i, int64(reflect.ValueOf(value).Uint()))
			continue
		case reflect.Float32, reflect.Float64:
			stmt.BindFloat(i, reflect.ValueOf(value).Float())
			continue
		case reflect.Bool:
			stmt.BindBool(i, reflect.ValueOf(value).Bool())
			continue
		}

		switch v := value.(type) {
		case time.Time:
			stmt.BindInt64(i, v.Unix())
		case fmt.Stringer:
			stmt.BindText(i, v.String())
		default:
			return nil, ErrUnknownType
		}
	}

	return stmt, nil
}

func (c *Conn) Exec(ctx context.Context, sql string, values ...any) error {
	stmt, err := c.Prepare(ctx, sql, values...)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPrepareSQL, err)
	}
	defer stmt.Finalize()

	_, err = stmt.Step()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrExecSQL, err)
	}
	return nil
}

// Database struct which holds pool of connection
type Database struct {
	pool       *sqlitex.Pool
	stringConn string
	size       int
	remaining  int64
	fns        map[string]*FunctionImpl
}

func (db *Database) PoolSize() int {
	return db.size
}

// Conn returns one connection from connection pool
// NOTE: make sure to call Close function to put the connection back to the pool
func (db *Database) Conn(ctx context.Context) (conn *Conn, err error) {
	sqlConn, err := db.pool.Take(ctx)
	if err != nil {
		return nil, err
	}

	conn = &Conn{
		conn: sqlConn,
		db:   db,
	}

	poolSize := atomic.AddInt64(&db.remaining, -1)

	slog.Debug("get connection from pool", "conn_id", conn.id, "pool_size", poolSize)
	return conn, nil
}

func (db *Database) Close() error {
	if db.remaining != int64(db.size) {
		slog.Warn("database has some connections that are not closed", "still_open", int64(db.size)-db.remaining)
	}
	return db.pool.Close()
}

type OptionFunc func(*Database) error

func WithMemory() OptionFunc {
	return WithStringConn("file::memory:?mode=memory&cache=shared")
}

func WithFile(path string) OptionFunc {
	return func(db *Database) error {
		err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if err != nil {
			return err
		}

		return WithStringConn("file:" + path + "?cache=shared")(db)
	}
}

func WithStringConn(stringConn string) OptionFunc {
	return func(db *Database) error {
		if db.stringConn != "" {
			return errors.New("stringConn already set")
		}
		db.stringConn = stringConn
		return nil
	}
}

func WithPoolSize(size int) OptionFunc {
	return func(db *Database) error {
		if db.size != 0 {
			return errors.New("pool size already set")
		}
		db.size = size
		db.remaining = int64(size)
		return nil
	}
}

func WithFunctions(fns map[string]*FunctionImpl) OptionFunc {
	return func(db *Database) error {
		db.fns = fns
		return nil
	}
}

// New creates a sqlite database
func New(opts ...OptionFunc) (*Database, error) {
	const pragma = `PRAGMA foreign_keys = ON;`

	var err error

	db := &Database{}
	for _, opt := range opts {
		err := opt(db)
		if err != nil {
			return nil, err
		}
	}

	db.pool, err = sqlitex.Open(db.stringConn, 0, db.size)
	if err != nil {
		return nil, err
	}

	// the following loop makes sure that all pool connections have
	// forgen_key enabled by default
	connections := make([]*Conn, 0, db.size)
	for i := 0; i < db.size; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			return nil, err
		}

		err = conn.Exec(context.Background(), pragma)
		if err != nil {
			return nil, err
		}

		for name, fn := range db.fns {
			err = conn.Function(name, fn)
			if err != nil {
				return nil, err
			}
		}

		conn.id = i
		connections = append(connections, conn)
	}

	for _, conn := range connections {
		conn.Close()
	}

	return db, nil
}

func RunScript(ctx context.Context, db *Database, sql string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.ExecScript(strings.TrimSpace(sql))
}

func RunScriptFiles(ctx context.Context, db *Database, path string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	sqlFiles := make([]string, 0, len(files))

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if filepath.Ext(file.Name()) != ".sql" {
			continue
		}

		sqlFile := filepath.Join(path, file.Name())
		sqlFiles = append(sqlFiles, sqlFile)
	}

	sort.Slice(sqlFiles, func(i, j int) bool {
		return sqlFiles[i] < sqlFiles[j]
	})

	for _, sqlFile := range sqlFiles {
		sql, err := os.ReadFile(sqlFile)
		if err != nil {
			return err
		}

		err = conn.ExecScript(string(sql))
		if err != nil {
			return err
		}
	}

	return nil
}
