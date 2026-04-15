package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type walOp uint8

const (
	walOpUpsert walOp = 1
	walOpBurn   walOp = 2
	walOpForge  walOp = 3
	walOpDrop   walOp = 4
	walOpReforge walOp = 5
)

type WAL struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	entries int
}

func openWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	w := &WAL{path: path, f: f}
	w.entries = w.countEntries()
	return w, nil
}

func (w *WAL) countEntries() int {
	f, err := os.Open(w.path)
	if err != nil {
		return 0
	}
	defer f.Close()
	count := 0
	r := bufio.NewReader(f)
	for {
		b, err := r.ReadByte()
		if err != nil {
			break
		}
		_ = b
		count++
		var lenBuf [4]byte
		if _, err := r.Read(lenBuf[:]); err != nil {
			break
		}
		size := binary.BigEndian.Uint32(lenBuf[:])
		skip := make([]byte, size)
		if _, err := r.Read(skip); err != nil {
			break
		}
	}
	return count
}

func (w *WAL) size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.entries
}

func (w *WAL) write(op walOp, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var buf bytes.Buffer
	buf.WriteByte(byte(op))
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(data)))
	buf.Write(b)
	buf.Write(data)

	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("WAL write error: %w", err)
	}
	w.entries++
	return nil
}

func (w *WAL) writeForge(name string, cols []Column) error {
	var buf bytes.Buffer
	writeString(&buf, name)
	writeUint32(&buf, uint32(len(cols)))
	for _, col := range cols {
		writeString(&buf, col.Name)
		buf.WriteByte(byte(col.Type))
		if col.Indexed {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}
	return w.write(walOpForge, buf.Bytes())
}

func (w *WAL) writeDrop(name string) error {
	var buf bytes.Buffer
	writeString(&buf, name)
	return w.write(walOpDrop, buf.Bytes())
}

func (w *WAL) writeUpsert(table string, keyCol string, values []Value) error {
	var buf bytes.Buffer
	writeString(&buf, table)
	writeString(&buf, keyCol)
	writeUint32(&buf, uint32(len(values)))
	for _, v := range values {
		if err := writeValue(&buf, v); err != nil {
			return err
		}
	}
	return w.write(walOpUpsert, buf.Bytes())
}

func (w *WAL) writeBurn(table, col string, val Value) error {
	var buf bytes.Buffer
	writeString(&buf, table)
	writeString(&buf, col)
	if err := writeValue(&buf, val); err != nil {
		return err
	}
	return w.write(walOpBurn, buf.Bytes())
}

func (w *WAL) writeReforge(table, whereCol string, whereVal Value, setCol string, setVal Value) error {
	var buf bytes.Buffer
	writeString(&buf, table)
	writeString(&buf, whereCol)
	if err := writeValue(&buf, whereVal); err != nil {
		return err
	}
	writeString(&buf, setCol)
	if err := writeValue(&buf, setVal); err != nil {
		return err
	}
	return w.write(walOpReforge, buf.Bytes())
}

func (w *WAL) replay(db *DB) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := os.ReadFile(w.path)
	if err != nil {
		return nil 
	}
	if len(data) == 0 {
		return nil
	}

	r := bytes.NewReader(data)
	for r.Len() > 0 {
		opByte, err := r.ReadByte()
		if err != nil {
			break
		}
		op := walOp(opByte)

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			break
		}
		size := binary.BigEndian.Uint32(lenBuf)
		entryData := make([]byte, size)
		if _, err := io.ReadFull(r, entryData); err != nil {
			break
		}

		er := bytes.NewReader(entryData)
		if err := replayEntry(db, op, er); err != nil {
			break
		}
	}
	return nil
}

func replayEntry(db *DB, op walOp, r *bytes.Reader) error {
	switch op {
	case walOpForge:
		name, err := readString(r)
		if err != nil {
			return err
		}
		numCols, err := readUint32(r)
		if err != nil {
			return err
		}
		cols := make([]Column, numCols)
		for i := range cols {
			colName, err := readString(r)
			if err != nil {
				return err
			}
			tb, err := r.ReadByte()
			if err != nil {
				return err
			}
			ib, err := r.ReadByte()
			if err != nil {
				return err
			}
			cols[i] = Column{Name: colName, Type: DataType(tb), Indexed: ib == 1}
		}
		if _, exists := db.tables[name]; !exists {
			t := &Table{Name: name, Columns: cols, Rows: []Row{}, indexes: make(map[string]map[string][]int)}
			for _, col := range cols {
				if col.Indexed {
					t.indexes[col.Name] = make(map[string][]int)
				}
			}
			db.tables[name] = t
		}

	case walOpDrop:
		name, err := readString(r)
		if err != nil {
			return err
		}
		delete(db.tables, name)

	case walOpUpsert:
		tableName, err := readString(r)
		if err != nil {
			return err
		}
		keyCol, err := readString(r)
		if err != nil {
			return err
		}
		numVals, err := readUint32(r)
		if err != nil {
			return err
		}
		values := make([]Value, numVals)
		for i := range values {
			v, err := readValue(r)
			if err != nil {
				return err
			}
			values[i] = v
		}
		table, ok := db.tables[tableName]
		if !ok {
			return nil
		}
		if keyCol == "" {
			newIdx := len(table.Rows)
			table.Rows = append(table.Rows, Row(values))
			for colName, idx := range table.indexes {
				colIdx, _ := db.colIndex(table, colName)
				key := values[colIdx].String()
				idx[key] = append(idx[key], newIdx)
			}
		} else {
			keyIdx, err := db.colIndex(table, keyCol)
			if err != nil {
				return nil
			}
			keyVal := values[keyIdx]
			existingIdx := -1
			for i, row := range table.Rows {
				if row[keyIdx].Equals(keyVal) {
					existingIdx = i
					break
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
		}

	case walOpBurn:
		tableName, err := readString(r)
		if err != nil {
			return err
		}
		colName, err := readString(r)
		if err != nil {
			return err
		}
		val, err := readValue(r)
		if err != nil {
			return err
		}
		table, ok := db.tables[tableName]
		if !ok {
			return nil
		}
		colIdx, err := db.colIndex(table, colName)
		if err != nil {
			return nil
		}
		var remaining []Row
		for _, row := range table.Rows {
			if !row[colIdx].Equals(val) {
				remaining = append(remaining, row)
			}
		}
		table.Rows = remaining
		for cn := range table.indexes {
			db.rebuildIndex(table, cn)
		}

	case walOpReforge:
		tableName, err := readString(r)
		if err != nil {
			return err
		}
		whereCol, err := readString(r)
		if err != nil {
			return err
		}
		whereVal, err := readValue(r)
		if err != nil {
			return err
		}
		setCol, err := readString(r)
		if err != nil {
			return err
		}
		setVal, err := readValue(r)
		if err != nil {
			return err
		}
		table, ok := db.tables[tableName]
		if !ok {
			return nil
		}
		whereIdx, err := db.colIndex(table, whereCol)
		if err != nil {
			return nil
		}
		setIdx, err := db.colIndex(table, setCol)
		if err != nil {
			return nil
		}
		for i, row := range table.Rows {
			if row[whereIdx].Equals(whereVal) {
				table.Rows[i][setIdx] = setVal
			}
		}
		for cn := range table.indexes {
			db.rebuildIndex(table, cn)
		}
	}
	return nil
}

func (w *WAL) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, 0); err != nil {
		return err
	}
	w.entries = 0
	return nil
}

func (w *WAL) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
