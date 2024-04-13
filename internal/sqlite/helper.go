package sqlite

import (
	"encoding/json"
	"fmt"
	"reflect"
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

func ShowSql(sql string, args ...any) string {
	var temp2 []string

	temp := strings.FieldsFunc(sql, func(r rune) bool {
		switch r {
		case '\t', '\n', ' ':
			return true
		default:
			return false
		}
	})
	for _, tmp := range temp {
		if tmp != "" {
			temp2 = append(temp2, tmp)
		}
	}

	newArgs := []any{}
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			arg = fmt.Sprintf("'%s'", v)
		case time.Time:
			arg = fmt.Sprintf("%d", v.Unix())
		default:
			if v == nil {
				arg = "NULL"
				break
			}
			kind := reflect.TypeOf(v).Kind()
			if kind == reflect.Slice || kind == reflect.Map {
				b, _ := json.Marshal(v)
				arg = fmt.Sprintf("'%v'", string(b))
				break
			}
			arg = fmt.Sprintf("%v", v)
		}
		newArgs = append(newArgs, arg)
	}

	format := strings.ReplaceAll(strings.Join(temp2, " "), "?", "%v")
	return fmt.Sprintf(format, newArgs...)
}
