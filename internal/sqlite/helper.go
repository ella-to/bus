package sqlite

import (
	"strings"
	"time"
)

func LoadTime(stmt *Stmt, key string) time.Time {
	value := stmt.GetInt64(key)
	return time.Unix(value, 0)
}

func Placeholders(count int) string {
	var sb strings.Builder
	for i := 0; i < count; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
	}
	return sb.String()
}

// (?, ?), (?, ?), (?, ?)
func MultiplePlaceholders(numRows, numCols int) string {
	var sb strings.Builder
	for i := 0; i < numRows; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		sb.WriteString(Placeholders(numCols))
		sb.WriteString(")")
	}
	return sb.String()
}
