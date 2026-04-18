package engine

import (
	"fmt"
	"strings"
)

var MagicBytes = [5]byte{'E', 'V', 'O', 'D', 'B'}

const Version = 4

type DataType uint8

const (
	TypeInt    DataType = 1
	TypeFloat  DataType = 2
	TypeString DataType = 3
	TypeBool   DataType = 4
	TypeNull   DataType = 5
)

func (d DataType) String() string {
	switch d {
	case TypeInt:
		return "INT"
	case TypeFloat:
		return "FLOAT"
	case TypeString:
		return "STRING"
	case TypeBool:
		return "BOOL"
	case TypeNull:
		return "NULL"
	default:
		return "UNKNOWN"
	}
}

func ParseDataType(s string) (DataType, error) {
	switch strings.ToUpper(s) {
	case "INT":
		return TypeInt, nil
	case "FLOAT":
		return TypeFloat, nil
	case "STRING":
		return TypeString, nil
	case "BOOL":
		return TypeBool, nil
	case "NULL":
		return TypeNull, nil
	// Keep JSON as alias for STRING for backwards compat
	case "JSON":
		return TypeString, nil
	default:
		return 0, fmt.Errorf("unknown type: %s", s)
	}
}

type Column struct {
	Name    string
	Type    DataType
	Indexed bool
}

type Table struct {
	Name    string
	Columns []Column
	Rows    []Row
	indexes map[string]map[string][]int
}

type Row []Value

type Value struct {
	Type    DataType
	IntVal  int64
	FltVal  float64
	StrVal  string
	BoolVal bool
}

func NullValue() Value { return Value{Type: TypeNull} }

func (v Value) String() string {
	switch v.Type {
	case TypeInt:
		return fmt.Sprintf("%d", v.IntVal)
	case TypeFloat:
		return fmt.Sprintf("%g", v.FltVal)
	case TypeString:
		return v.StrVal
	case TypeBool:
		if v.BoolVal {
			return "true"
		}
		return "false"
	case TypeNull:
		return "null"
	default:
		return ""
	}
}

func (v Value) Equals(other Value) bool {
	if v.Type == TypeNull && other.Type == TypeNull {
		return true
	}
	if v.Type != other.Type {
		return false
	}
	switch v.Type {
	case TypeInt:
		return v.IntVal == other.IntVal
	case TypeFloat:
		return v.FltVal == other.FltVal
	case TypeString:
		return v.StrVal == other.StrVal
	case TypeBool:
		return v.BoolVal == other.BoolVal
	}
	return false
}

func (v Value) Compare(other Value) int {
	switch v.Type {
	case TypeInt:
		if v.IntVal < other.IntVal {
			return -1
		} else if v.IntVal > other.IntVal {
			return 1
		}
		return 0
	case TypeFloat:
		if v.FltVal < other.FltVal {
			return -1
		} else if v.FltVal > other.FltVal {
			return 1
		}
		return 0
	case TypeString:
		return strings.Compare(v.StrVal, other.StrVal)
	}
	return 0
}

type Operator int

const (
	OpEq Operator = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
)

func ParseOperator(s string) (Operator, error) {
	switch s {
	case "=":
		return OpEq, nil
	case "!=", "<>":
		return OpNe, nil
	case "<":
		return OpLt, nil
	case "<=":
		return OpLe, nil
	case ">":
		return OpGt, nil
	case ">=":
		return OpGe, nil
	default:
		return 0, fmt.Errorf("unknown operator: %s", s)
	}
}

func (op Operator) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNe:
		return "!="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	}
	return "?"
}

type Condition struct {
	Col string
	Op  Operator
	Val Value
}

func (c Condition) Matches(row Row, cols []Column) bool {
	colIdx := -1
	for i, col := range cols {
		if col.Name == c.Col {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return false
	}
	v := row[colIdx]
	if v.Type != c.Val.Type && c.Val.Type != TypeNull {
		return false
	}
	cmp := v.Compare(c.Val)
	switch c.Op {
	case OpEq:
		return v.Equals(c.Val)
	case OpNe:
		return !v.Equals(c.Val)
	case OpLt:
		return cmp < 0
	case OpLe:
		return cmp <= 0
	case OpGt:
		return cmp > 0
	case OpGe:
		return cmp >= 0
	}
	return false
}

type SortOrder int

const (
	SortAsc  SortOrder = iota
	SortDesc
)

type OrderBy struct {
	Col   string
	Order SortOrder
}
