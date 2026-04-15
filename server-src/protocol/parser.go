package protocol

import (
	"fmt"
	"strconv"
	"strings"

	"evodb/engine"
)

type CommandType int

const (
	CmdForge   CommandType = iota
	CmdPush
	CmdUpsert
	CmdPull
	CmdCount
	CmdBurn
	CmdReforge
	CmdDrop
	CmdTables
	CmdSchema
	CmdIndex
)

type Command struct {
	Type       CommandType
	Table      string
	KeyCol     string
	Columns    []engine.Column
	Values     []engine.Value
	Conditions []engine.Condition
	WhereCol string
	WhereVal engine.Value
	SetCol   string
	SetVal   engine.Value
	OrderBy  *engine.OrderBy
	Limit    int
}

func Parse(raw string) (*Command, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty command")
	}

	upper := strings.ToUpper(raw)

	switch {
	case strings.HasPrefix(upper, "FORGE "):
		return parseForge(raw)
	case strings.HasPrefix(upper, "PUSH "):
		return parsePush(raw)
	case strings.HasPrefix(upper, "UPSERT "):
		return parseUpsert(raw)
	case strings.HasPrefix(upper, "PULL "):
		return parsePull(raw)
	case strings.HasPrefix(upper, "COUNT "):
		return parseCount(raw)
	case strings.HasPrefix(upper, "BURN "):
		return parseBurn(raw)
	case strings.HasPrefix(upper, "REFORGE "):
		return parseReforge(raw)
	case strings.HasPrefix(upper, "DROP "):
		return parseDrop(raw)
	case upper == "TABLES":
		return &Command{Type: CmdTables}, nil
	case strings.HasPrefix(upper, "SCHEMA "):
		parts := strings.Fields(raw)
		if len(parts) != 2 {
			return nil, fmt.Errorf("usage: SCHEMA <table>")
		}
		return &Command{Type: CmdSchema, Table: parts[1]}, nil
	case strings.HasPrefix(upper, "INDEX "):
		return parseIndex(raw)
	default:
		return nil, fmt.Errorf("unknown command: %s", strings.Fields(raw)[0])
	}
}

func parseForge(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("FORGE "):])
	pOpen := strings.Index(rest, "(")
	pClose := strings.LastIndex(rest, ")")
	if pOpen < 0 || pClose < 0 {
		return nil, fmt.Errorf("FORGE syntax: FORGE <table> (col TYPE [INDEX], ...)")
	}

	tableName := strings.TrimSpace(rest[:pOpen])
	colStr := rest[pOpen+1 : pClose]

	colDefs := splitComma(colStr)
	cols := make([]engine.Column, 0, len(colDefs))
	for _, def := range colDefs {
		parts := strings.Fields(def)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column definition: %q", def)
		}
		dt, err := engine.ParseDataType(strings.ToUpper(parts[1]))
		if err != nil {
			return nil, err
		}
		indexed := len(parts) >= 3 && strings.ToUpper(parts[2]) == "INDEX"
		cols = append(cols, engine.Column{Name: parts[0], Type: dt, Indexed: indexed})
	}
	return &Command{Type: CmdForge, Table: tableName, Columns: cols}, nil
}

func parsePush(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("PUSH "):])
	pOpen := strings.Index(rest, "(")
	pClose := strings.LastIndex(rest, ")")
	if pOpen < 0 || pClose < 0 {
		return nil, fmt.Errorf("PUSH syntax: PUSH <table> (val, ...)")
	}
	tableName := strings.TrimSpace(rest[:pOpen])
	vals, err := parseValueList(rest[pOpen+1 : pClose])
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdPush, Table: tableName, Values: vals}, nil
}

func parseUpsert(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("UPSERT "):])
	upper := strings.ToUpper(rest)

	keyIdx := strings.Index(upper, " KEY ")
	if keyIdx < 0 {
		return nil, fmt.Errorf("UPSERT syntax: UPSERT <table> KEY <col> (val, ...)")
	}
	tableName := strings.TrimSpace(rest[:keyIdx])
	afterKey := strings.TrimSpace(rest[keyIdx+5:])

	pOpen := strings.Index(afterKey, "(")
	pClose := strings.LastIndex(afterKey, ")")
	if pOpen < 0 || pClose < 0 {
		return nil, fmt.Errorf("UPSERT syntax: UPSERT <table> KEY <col> (val, ...)")
	}
	keyCol := strings.TrimSpace(afterKey[:pOpen])
	vals, err := parseValueList(afterKey[pOpen+1 : pClose])
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdUpsert, Table: tableName, KeyCol: keyCol, Values: vals}, nil
}

func parsePull(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("PULL "):])
	upper := strings.ToUpper(rest)

	var orderBy *engine.OrderBy
	if oi := strings.Index(upper, " ORDER BY "); oi >= 0 {
		orderStr := strings.TrimSpace(rest[oi+10:])
		rest = strings.TrimSpace(rest[:oi])
		upper = strings.ToUpper(rest)

		parts := strings.Fields(orderStr)
		ob := &engine.OrderBy{Col: parts[0], Order: engine.SortAsc}
		if len(parts) >= 2 && strings.ToUpper(parts[1]) == "DESC" {
			ob.Order = engine.SortDesc
		}
		orderBy = ob
	}

	limit := 0
	if li := strings.Index(upper, " LIMIT "); li >= 0 {
		limitStr := strings.TrimSpace(rest[li+7:])
		limitParts := strings.Fields(limitStr)
		if len(limitParts) > 0 {
			n, err := strconv.Atoi(limitParts[0])
			if err != nil {
				return nil, fmt.Errorf("LIMIT must be an integer")
			}
			limit = n
		}
		rest = strings.TrimSpace(rest[:li])
		upper = strings.ToUpper(rest)
	}

	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		return &Command{Type: CmdPull, Table: rest, Limit: limit, OrderBy: orderBy}, nil
	}

	tableName := strings.TrimSpace(rest[:whereIdx])
	whereClause := strings.TrimSpace(rest[whereIdx+7:])

	conds, err := parseConditions(whereClause)
	if err != nil {
		return nil, err
	}

	cmd := &Command{
		Type:       CmdPull,
		Table:      tableName,
		Conditions: conds,
		Limit:      limit,
		OrderBy:    orderBy,
	}
	if len(conds) == 1 && conds[0].Op == engine.OpEq {
		cmd.WhereCol = conds[0].Col
		cmd.WhereVal = conds[0].Val
	}
	return cmd, nil
}

func parseCount(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("COUNT "):])
	upper := strings.ToUpper(rest)

	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		return &Command{Type: CmdCount, Table: rest}, nil
	}
	tableName := strings.TrimSpace(rest[:whereIdx])
	whereClause := strings.TrimSpace(rest[whereIdx+7:])
	col, val, err := parseWhere(whereClause)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdCount, Table: tableName, WhereCol: col, WhereVal: val}, nil
}

func parseBurn(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("BURN "):])
	upper := strings.ToUpper(rest)

	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx < 0 {
		return nil, fmt.Errorf("BURN requires WHERE clause")
	}
	tableName := strings.TrimSpace(rest[:whereIdx])
	whereClause := strings.TrimSpace(rest[whereIdx+7:])
	col, val, err := parseWhere(whereClause)
	if err != nil {
		return nil, err
	}
	return &Command{Type: CmdBurn, Table: tableName, WhereCol: col, WhereVal: val}, nil
}

func parseReforge(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("REFORGE "):])
	upper := strings.ToUpper(rest)

	setIdx := strings.Index(upper, " SET ")
	whereIdx := strings.Index(upper, " WHERE ")

	if setIdx < 0 || whereIdx < 0 || setIdx > whereIdx {
		return nil, fmt.Errorf("REFORGE syntax: REFORGE <table> SET col = val WHERE col = val")
	}
	tableName := strings.TrimSpace(rest[:setIdx])
	setClause := strings.TrimSpace(rest[setIdx+5 : whereIdx])
	whereClause := strings.TrimSpace(rest[whereIdx+7:])

	setCol, setVal, err := parseAssignment(setClause)
	if err != nil {
		return nil, fmt.Errorf("SET clause: %w", err)
	}
	whereCol, whereVal, err := parseWhere(whereClause)
	if err != nil {
		return nil, fmt.Errorf("WHERE clause: %w", err)
	}
	return &Command{
		Type:     CmdReforge,
		Table:    tableName,
		SetCol:   setCol,
		SetVal:   setVal,
		WhereCol: whereCol,
		WhereVal: whereVal,
	}, nil
}

func parseDrop(raw string) (*Command, error) {
	parts := strings.Fields(raw)
	if len(parts) != 2 {
		return nil, fmt.Errorf("usage: DROP <table>")
	}
	return &Command{Type: CmdDrop, Table: parts[1]}, nil
}

func parseIndex(raw string) (*Command, error) {
	rest := strings.TrimSpace(raw[len("INDEX "):])
	upper := strings.ToUpper(rest)
	onIdx := strings.Index(upper, " ON ")
	if onIdx < 0 {
		return nil, fmt.Errorf("INDEX syntax: INDEX <table> ON <col>")
	}
	tableName := strings.TrimSpace(rest[:onIdx])
	colName := strings.TrimSpace(rest[onIdx+4:])
	return &Command{Type: CmdIndex, Table: tableName, KeyCol: colName}, nil
}

func parseConditions(s string) ([]engine.Condition, error) {
	parts := splitAND(s)
	conds := make([]engine.Condition, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		col, op, val, err := parseCondition(part)
		if err != nil {
			return nil, err
		}
		conds = append(conds, engine.Condition{Col: col, Op: op, Val: val})
	}
	return conds, nil
}

func splitAND(s string) []string {
	upper := strings.ToUpper(s)
	var parts []string
	start := 0
	for {
		idx := strings.Index(upper[start:], " AND ")
		if idx < 0 {
			parts = append(parts, s[start:])
			break
		}
		parts = append(parts, s[start:start+idx])
		start = start + idx + 5
	}
	return parts
}

func parseCondition(s string) (string, engine.Operator, engine.Value, error) {
	ops := []string{"!=", "<>", "<=", ">=", "<", ">", "="}
	for _, opStr := range ops {
		idx := strings.Index(s, opStr)
		if idx < 0 {
			continue
		}
		col := strings.TrimSpace(s[:idx])
		valStr := strings.TrimSpace(s[idx+len(opStr):])
		op, err := engine.ParseOperator(opStr)
		if err != nil {
			return "", 0, engine.Value{}, err
		}
		val, err := parseValue(valStr)
		if err != nil {
			return "", 0, engine.Value{}, err
		}
		return col, op, val, nil
	}
	return "", 0, engine.Value{}, fmt.Errorf("no operator found in condition: %q", s)
}

func parseValueList(s string) ([]engine.Value, error) {
	rawVals := splitComma(s)
	vals := make([]engine.Value, 0, len(rawVals))
	for _, rv := range rawVals {
		v, err := parseValue(strings.TrimSpace(rv))
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	return vals, nil
}

func parseWhere(clause string) (string, engine.Value, error) {
	col, _, val, err := parseCondition(clause)
	return col, val, err
}

func parseAssignment(clause string) (string, engine.Value, error) {
	eqIdx := strings.Index(clause, "=")
	if eqIdx < 0 {
		return "", engine.Value{}, fmt.Errorf("expected col = val, got: %q", clause)
	}
	col := strings.TrimSpace(clause[:eqIdx])
	valStr := strings.TrimSpace(clause[eqIdx+1:])
	val, err := parseValue(valStr)
	if err != nil {
		return "", engine.Value{}, err
	}
	return col, val, nil
}

func parseValue(s string) (engine.Value, error) {
	if strings.ToUpper(s) == "NULL" {
		return engine.NullValue(), nil
	}
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return engine.Value{Type: engine.TypeString, StrVal: s[1 : len(s)-1]}, nil
	}
	if (strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}")) ||
		(strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")) {
		return engine.Value{Type: engine.TypeJSON, StrVal: s}, nil
	}
	if strings.ToLower(s) == "true" {
		return engine.Value{Type: engine.TypeBool, BoolVal: true}, nil
	}
	if strings.ToLower(s) == "false" {
		return engine.Value{Type: engine.TypeBool, BoolVal: false}, nil
	}
	if strings.Contains(s, ".") {
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return engine.Value{Type: engine.TypeFloat, FltVal: f}, nil
		}
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return engine.Value{Type: engine.TypeInt, IntVal: i}, nil
	}
	return engine.Value{}, fmt.Errorf("cannot parse value: %q", s)
}

func splitComma(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	depth := 0

	for _, ch := range s {
		switch {
		case ch == '"':
			inQuote = !inQuote
			current.WriteRune(ch)
		case (ch == '{' || ch == '[') && !inQuote:
			depth++
			current.WriteRune(ch)
		case (ch == '}' || ch == ']') && !inQuote:
			depth--
			current.WriteRune(ch)
		case ch == ',' && !inQuote && depth == 0:
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	return parts
}
