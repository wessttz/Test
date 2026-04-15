package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"sync"
)

type DB struct {
	mu     sync.RWMutex
	path   string
	tables map[string]*Table
	wal    *WAL
}

func Open(path string) (*DB, error) {
	db := &DB{
		path:   path,
		tables: make(map[string]*Table),
	}

	wal, err := openWAL(path + ".wal")
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	db.wal = wal

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := db.flush(); err != nil {
			return nil, fmt.Errorf("failed to create database: %w", err)
		}
	} else {
		if err := db.load(); err != nil {
			return nil, fmt.Errorf("failed to load database: %w", err)
		}
	}

	if err := db.wal.replay(db); err != nil {
		return nil, fmt.Errorf("WAL replay failed: %w", err)
	}

	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.flush(); err != nil {
		return err
	}
	return db.wal.close()
}

func (db *DB) Checkpoint() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.flush(); err != nil {
		return err
	}
	return db.wal.truncate()
}

func (db *DB) ForgeTable(name string, columns []Column) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; exists {
		return fmt.Errorf("table %q already exists", name)
	}

	t := &Table{
		Name:    name,
		Columns: columns,
		Rows:    []Row{},
		indexes: make(map[string]map[string][]int),
	}
	for _, col := range columns {
		if col.Indexed {
			t.indexes[col.Name] = make(map[string][]int)
		}
	}
	db.tables[name] = t

	if err := db.wal.writeForge(name, columns); err != nil {
		return err
	}
	return db.checkpointIfNeeded()
}

func (db *DB) DropTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; !exists {
		return fmt.Errorf("table %q not found", name)
	}
	delete(db.tables, name)
	if err := db.wal.writeDrop(name); err != nil {
		return err
	}
	return db.checkpointIfNeeded()
}

func (db *DB) ListTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	names := make([]string, 0, len(db.tables))
	for name := range db.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (db *DB) GetSchema(tableName string) ([]Column, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	table, err := db.getTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.Columns, nil
}

func (db *DB) AddIndex(tableName, colName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.getTable(tableName)
	if err != nil {
		return err
	}
	colIdx, err := db.colIndex(table, colName)
	if err != nil {
		return err
	}
	table.Columns[colIdx].Indexed = true
	table.indexes[colName] = make(map[string][]int)
	for i, row := range table.Rows {
		key := row[colIdx].String()
		table.indexes[colName][key] = append(table.indexes[colName][key], i)
	}
	return db.flush()
}

func (db *DB) rebuildIndex(table *Table, colName string) {
	colIdx := -1
	for i, c := range table.Columns {
		if c.Name == colName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return
	}
	idx := make(map[string][]int, len(table.Rows))
	for i, row := range table.Rows {
		key := row[colIdx].String()
		idx[key] = append(idx[key], i)
	}
	table.indexes[colName] = idx
}

func (db *DB) PushRow(tableName string, values []Value) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.getTable(tableName)
	if err != nil {
		return err
	}
	if err := db.validateRow(table, values); err != nil {
		return err
	}

	newIdx := len(table.Rows)
	table.Rows = append(table.Rows, Row(values))
	for colName, idx := range table.indexes {
		colIdx, _ := db.colIndex(table, colName)
		key := values[colIdx].String()
		idx[key] = append(idx[key], newIdx)
	}

	if err := db.wal.writeUpsert(tableName, "", values); err != nil {
		return err
	}
	return db.checkpointIfNeeded()
}

func (db *DB) UpsertRow(tableName string, keyCol string, values []Value) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.getTable(tableName)
	if err != nil {
		return err
	}
	if err := db.validateRow(table, values); err != nil {
		return err
	}

	keyIdx, err := db.colIndex(table, keyCol)
	if err != nil {
		return err
	}
	keyVal := values[keyIdx]

	existingIdx := -1
	if idx, hasIndex := table.indexes[keyCol]; hasIndex {
		if rows, ok := idx[keyVal.String()]; ok && len(rows) > 0 {
			existingIdx = rows[0]
		}
	} else {
		for i, row := range table.Rows {
			if row[keyIdx].Equals(keyVal) {
				existingIdx = i
				break
			}
		}
	}

	if existingIdx >= 0 {
		table.Rows[existingIdx] = Row(values)
		for colName := range table.indexes {
			db.rebuildIndex(table, colName)
		}
	} else {
		newIdx := len(table.Rows)
		table.Rows = append(table.Rows, Row(values))
		for colName, idx := range table.indexes {
			colIdx, _ := db.colIndex(table, colName)
			key := values[colIdx].String()
			idx[key] = append(idx[key], newIdx)
		}
	}

	if err := db.wal.writeUpsert(tableName, keyCol, values); err != nil {
		return err
	}
	return db.checkpointIfNeeded()
}

func (db *DB) Query(tableName string, conditions []Condition, orderBy *OrderBy, limit int) ([]Row, []Column, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, err := db.getTable(tableName)
	if err != nil {
		return nil, nil, err
	}

	var result []Row
	for _, row := range table.Rows {
		match := true
		for _, cond := range conditions {
			if !cond.Matches(row, table.Columns) {
				match = false
				break
			}
		}
		if match {
			result = append(result, row)
		}
	}

	if orderBy != nil {
		colIdx := -1
		for i, c := range table.Columns {
			if c.Name == orderBy.Col {
				colIdx = i
				break
			}
		}
		if colIdx >= 0 {
			sort.SliceStable(result, func(i, j int) bool {
				cmp := result[i][colIdx].Compare(result[j][colIdx])
				if orderBy.Order == SortDesc {
					return cmp > 0
				}
				return cmp < 0
			})
		}
	}

	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}

	return result, table.Columns, nil
}

func (db *DB) PullRows(tableName string, colName string, filterVal *Value, limit int) ([]Row, []Column, error) {
	var conds []Condition
	if filterVal != nil {
		conds = []Condition{{Col: colName, Op: OpEq, Val: *filterVal}}
	}
	return db.Query(tableName, conds, nil, limit)
}

func (db *DB) CountRows(tableName string, colName string, filterVal *Value) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, err := db.getTable(tableName)
	if err != nil {
		return 0, err
	}

	if filterVal == nil {
		return len(table.Rows), nil
	}

	colIdx, err := db.colIndex(table, colName)
	if err != nil {
		return 0, err
	}

	if idx, hasIndex := table.indexes[colName]; hasIndex {
		return len(idx[filterVal.String()]), nil
	}

	count := 0
	for _, row := range table.Rows {
		if row[colIdx].Equals(*filterVal) {
			count++
		}
	}
	return count, nil
}

func (db *DB) BurnRows(tableName string, colName string, filterVal Value) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.getTable(tableName)
	if err != nil {
		return 0, err
	}
	colIdx, err := db.colIndex(table, colName)
	if err != nil {
		return 0, err
	}

	var remaining []Row
	deleted := 0
	for _, row := range table.Rows {
		if row[colIdx].Equals(filterVal) {
			deleted++
		} else {
			remaining = append(remaining, row)
		}
	}
	table.Rows = remaining
	for colName := range table.indexes {
		db.rebuildIndex(table, colName)
	}

	if err := db.wal.writeBurn(tableName, colName, filterVal); err != nil {
		return deleted, err
	}
	return deleted, db.checkpointIfNeeded()
}

func (db *DB) ReforgeRows(tableName string, whereCol string, whereVal Value, setCol string, setVal Value) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, err := db.getTable(tableName)
	if err != nil {
		return 0, err
	}
	whereIdx, err := db.colIndex(table, whereCol)
	if err != nil {
		return 0, err
	}
	setIdx, err := db.colIndex(table, setCol)
	if err != nil {
		return 0, err
	}
	if setVal.Type != table.Columns[setIdx].Type {
		return 0, fmt.Errorf("column %q expects %s", setCol, table.Columns[setIdx].Type)
	}

	updated := 0
	for i, row := range table.Rows {
		if row[whereIdx].Equals(whereVal) {
			table.Rows[i][setIdx] = setVal
			updated++
		}
	}
	if updated > 0 {
		for colName := range table.indexes {
			db.rebuildIndex(table, colName)
		}
	}

	if err := db.wal.writeReforge(tableName, whereCol, whereVal, setCol, setVal); err != nil {
		return updated, err
	}
	return updated, db.checkpointIfNeeded()
}

func (db *DB) validateRow(table *Table, values []Value) error {
	if len(values) != len(table.Columns) {
		return fmt.Errorf("expected %d values, got %d", len(table.Columns), len(values))
	}
	for i, val := range values {
		if val.Type == TypeNull {
			continue
		}
		if val.Type != table.Columns[i].Type {
			return fmt.Errorf("column %q expects %s, got %s",
				table.Columns[i].Name, table.Columns[i].Type, val.Type)
		}
	}
	return nil
}

func (db *DB) getTable(name string) (*Table, error) {
	t, ok := db.tables[name]
	if !ok {
		return nil, fmt.Errorf("table %q not found", name)
	}
	return t, nil
}

func (db *DB) colIndex(table *Table, colName string) (int, error) {
	for i, col := range table.Columns {
		if col.Name == colName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %q not found in table %q", colName, table.Name)
}

func (db *DB) checkpointIfNeeded() error {
	if db.wal.size() >= 1000 {
		if err := db.flush(); err != nil {
			return err
		}
		return db.wal.truncate()
	}
	return nil
}

func (db *DB) flush() error {
	var buf bytes.Buffer

	buf.Write(MagicBytes[:])
	buf.WriteByte(Version)
	writeUint32(&buf, uint32(len(db.tables)))

	for _, table := range db.tables {
		if err := writeTable(&buf, table); err != nil {
			return err
		}
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	writeUint32(&buf, checksum)

	tmp := db.path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, db.path)
}

func (db *DB) load() error {
	data, err := os.ReadFile(db.path)
	if err != nil {
		return err
	}
	if len(data) < 10 {
		return fmt.Errorf("file too small")
	}

	body := data[:len(data)-4]
	storedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	actualCRC := crc32.ChecksumIEEE(body)
	if storedCRC != actualCRC {
		return fmt.Errorf("checksum mismatch: file may be corrupted")
	}

	r := bytes.NewReader(body)

	magic := make([]byte, 5)
	if _, err := io.ReadFull(r, magic); err != nil {
		return fmt.Errorf("invalid file: %w", err)
	}
	if !bytes.Equal(magic, MagicBytes[:]) {
		return fmt.Errorf("not a valid .evodb file")
	}

	ver, err := r.ReadByte()
	if err != nil {
		return err
	}
	if ver != Version && ver != 2 {
		return fmt.Errorf("unsupported version: %d", ver)
	}

	numTables, err := readUint32(r)
	if err != nil {
		return err
	}

	for i := 0; i < int(numTables); i++ {
		table, err := readTable(r)
		if err != nil {
			return fmt.Errorf("error reading table %d: %w", i, err)
		}
		table.indexes = make(map[string]map[string][]int)
		for _, col := range table.Columns {
			if col.Indexed {
				db.rebuildIndex(table, col.Name)
			}
		}
		db.tables[table.Name] = table
	}

	return nil
}

func writeUint32(w io.Writer, v uint32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	w.Write(b)
}

func writeString(w *bytes.Buffer, s string) {
	b := []byte(s)
	writeUint32(w, uint32(len(b)))
	w.Write(b)
}

func writeTable(w *bytes.Buffer, t *Table) error {
	writeString(w, t.Name)
	writeUint32(w, uint32(len(t.Columns)))
	for _, col := range t.Columns {
		writeString(w, col.Name)
		w.WriteByte(byte(col.Type))
		if col.Indexed {
			w.WriteByte(1)
		} else {
			w.WriteByte(0)
		}
	}
	writeUint32(w, uint32(len(t.Rows)))
	for _, row := range t.Rows {
		for _, val := range row {
			if err := writeValue(w, val); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeValue(w *bytes.Buffer, v Value) error {
	w.WriteByte(byte(v.Type))
	switch v.Type {
	case TypeInt:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v.IntVal))
		w.Write(b)
	case TypeFloat:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, math.Float64bits(v.FltVal))
		w.Write(b)
	case TypeString, TypeJSON:
		writeString(w, v.StrVal)
	case TypeBool:
		if v.BoolVal {
			w.WriteByte(1)
		} else {
			w.WriteByte(0)
		}
	case TypeNull:
	default:
		return fmt.Errorf("unknown type: %d", v.Type)
	}
	return nil
}

func readUint32(r *bytes.Reader) (uint32, error) {
	b := make([]byte, 4)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func readString(r *bytes.Reader) (string, error) {
	length, err := readUint32(r)
	if err != nil {
		return "", err
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func readTable(r *bytes.Reader) (*Table, error) {
	name, err := readString(r)
	if err != nil {
		return nil, err
	}
	numCols, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	cols := make([]Column, numCols)
	for i := range cols {
		colName, err := readString(r)
		if err != nil {
			return nil, err
		}
		typeByte, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		indexedByte, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		cols[i] = Column{Name: colName, Type: DataType(typeByte), Indexed: indexedByte == 1}
	}
	numRows, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, numRows)
	for i := range rows {
		row := make(Row, numCols)
		for j := range row {
			val, err := readValue(r)
			if err != nil {
				return nil, err
			}
			row[j] = val
		}
		rows[i] = row
	}
	return &Table{Name: name, Columns: cols, Rows: rows, indexes: make(map[string]map[string][]int)}, nil
}

func readValue(r *bytes.Reader) (Value, error) {
	typeByte, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	v := Value{Type: DataType(typeByte)}
	switch v.Type {
	case TypeInt:
		b := make([]byte, 8)
		if _, err := io.ReadFull(r, b); err != nil {
			return Value{}, err
		}
		v.IntVal = int64(binary.BigEndian.Uint64(b))
	case TypeFloat:
		b := make([]byte, 8)
		if _, err := io.ReadFull(r, b); err != nil {
			return Value{}, err
		}
		v.FltVal = math.Float64frombits(binary.BigEndian.Uint64(b))
	case TypeString, TypeJSON:
		s, err := readString(r)
		if err != nil {
			return Value{}, err
		}
		v.StrVal = s
	case TypeBool:
		b, err := r.ReadByte()
		if err != nil {
			return Value{}, err
		}
		v.BoolVal = b == 1
	case TypeNull:
	default:
		return Value{}, fmt.Errorf("unknown type: %d", typeByte)
	}
	return v, nil
}
