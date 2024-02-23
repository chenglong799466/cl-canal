package canal

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

type binlogHandler struct {
	canal.DummyEventHandler          // Dummy handler from external lib
	BinlogParser                     // Our custom helper
	SchemaName              string   // database name
	TableNameList           []string // table name
}

func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Panic: %v\n%s\n", r, debug.Stack())
		}
	}()

	// Determine the action type for the event
	var action string
	switch e.Action {
	case canal.UpdateAction:
		action = "Update"
	case canal.InsertAction:
		action = "Insert"
	case canal.DeleteAction:
		action = "Delete"
	default:
		action = "Unknown action"
	}

	// Find the corresponding table in the handler's TableNameList
	key := strings.ToLower(e.Table.Schema + "." + e.Table.Name)
	for _, tableName := range h.TableNameList {
		keyTable := strings.ToLower(h.SchemaName + "." + tableName)
		if key == keyTable {
			switch e.Action {
			case canal.UpdateAction:
				// Extract old and new rows for update action
				oldRow := e.Rows[0]
				newRow := e.Rows[1]

				// Marshal old row
				marshalOld, err := json.Marshal(oldRow)
				if err != nil {
					return fmt.Errorf("failed to marshal old row: %w", err)
				}
				fmt.Printf("Old Row: %s\n", marshalOld)

				// Marshal new row
				marshalNew, err := json.Marshal(newRow)
				if err != nil {
					return fmt.Errorf("failed to marshal new row: %w", err)
				}
				fmt.Printf("New Row: %s\n", marshalNew)

				fmt.Printf("Action: %s\n", action)
				fmt.Printf("%s is updated: %s\n", tableName, marshalNew)

			case canal.InsertAction:
				// Extract inserted row for insert action
				insertedRow := e.Rows[0]

				// Marshal inserted row
				marshal, err := json.Marshal(insertedRow)
				if err != nil {
					return fmt.Errorf("failed to marshal inserted row: %w", err)
				}
				fmt.Printf("Inserted Row: %s\n", marshal)

				fmt.Printf("Action: %s\n", action)
				fmt.Printf("%s is created: %s\n", tableName, marshal)

			case canal.DeleteAction:
				// Extract deleted row for delete action
				deletedRow := e.Rows[0]

				// Marshal deleted row
				marshal, err := json.Marshal(deletedRow)
				if err != nil {
					return fmt.Errorf("failed to marshal deleted row: %w", err)
				}
				fmt.Printf("Deleted Row: %s\n", marshal)

				fmt.Printf("Action: %s\n", action)
				fmt.Printf("%s is deleted: %s\n", tableName, marshal)
			}
			break
		}
	}

	return nil
}

func (h *binlogHandler) String() string {
	return "binlogHandler"
}

type BinlogParser struct {
}

func (m *BinlogParser) GetBinLogData(element interface{}, e *canal.RowsEvent, n int) error {
	s := reflect.Indirect(reflect.ValueOf(element))
	t := s.Type()

	num := t.NumField()
	for k := 0; k < num; k++ {
		parsedTag := parseTagSetting(t.Field(k).Tag)

		columnName, ok := parsedTag["COLUMN"]
		if !ok || columnName == "COLUMN" {
			continue
		}

		switch fieldType := s.Field(k).Type(); fieldType.Name() {
		case "bool":
			value, err := m.boolHelper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("boolHelper error: %v, columnName: %v", err, columnName)
			}
			s.Field(k).SetBool(value)
		case "int", "int8", "int16", "int32", "int64":
			value, err := m.intHelper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("intHelper error: %v, columnName: %v", err, columnName)
			}
			s.Field(k).SetInt(value)
		case "uint", "uint8", "uint16", "uint32", "uint64":
			value, err := m.uintHelper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("uintHelper error: %v, columnName: %v", err, columnName)
			}
			s.Field(k).SetUint(value)
		case "string":
			value, err := m.stringHelper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("stringHelper error: %v, columnName: %v", err, columnName)
			}
			s.Field(k).SetString(value)
		case "NullString":
			value, err := m.nullStringHelper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("nullStringHelper error: %v, columnName: %v", err, columnName)
			}
			nullStringValue := reflect.ValueOf(value)
			s.Field(k).Set(nullStringValue)
		case "NullInt64":
			value, err := m.nullInt64Helper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("nullInt64Helper error: %v, columnName: %v", err, columnName)
			}
			nullInt64Value := reflect.ValueOf(value)
			s.Field(k).Set(nullInt64Value)
		case "Time":
			timeVal, err := m.dateTimeHelper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("dateTimeHelper error: %v, columnName: %v", err, columnName)
			}
			s.Field(k).Set(reflect.ValueOf(timeVal))
		case "float64", "float32":
			floatHelper, err := m.floatHelper(e, n, columnName)
			if err != nil {
				return fmt.Errorf("floatHelper error: %v, columnName: %v", err, columnName)
			}
			s.Field(k).SetFloat(floatHelper)
		default:
			if _, ok := parsedTag["FROMJSON"]; ok {
				newObject := reflect.New(fieldType).Interface()
				jsonStr, err := m.stringHelper(e, n, columnName)
				if err != nil {
					return err
				}

				err = json.Unmarshal([]byte(jsonStr), &newObject)
				if err != nil {
					return err
				}

				s.Field(k).Set(reflect.ValueOf(newObject).Elem().Convert(fieldType))
			}
		}
	}
	return nil
}

func (m *BinlogParser) dateTimeHelper(e *canal.RowsEvent, n int, columnName string) (time.Time, error) {
	columnId, err := m.getBinlogIdByName(e, columnName)
	if err != nil {
		return time.Time{}, err
	}

	columnValue := e.Rows[n][columnId]
	fmt.Printf("Helper columnName %v, columnId %v, value %v, type %T\n", columnName, columnId, columnValue, columnValue)

	switch e.Table.Columns[columnId].Type {
	case schema.TYPE_TIMESTAMP, schema.TYPE_DATETIME:
		t, err := time.Parse("2006-01-02 15:04:05", columnValue.(string))
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to parse time value: %v", err)
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported column type: %v", e.Table.Columns[columnId].Type)
	}
}

func (m *BinlogParser) intHelper(e *canal.RowsEvent, n int, columnName string) (int64, error) {
	columnId, err := m.getBinlogIdByName(e, columnName)
	if err != nil {
		return 0, err
	}
	fmt.Println(fmt.Sprintf("Helper columnName %v,columnId %v ,type %T",
		columnName, columnId, e.Rows[n][columnId]))

	switch i := e.Rows[n][columnId].(type) {
	case int8:
		return int64(i), nil
	case int32:
		return int64(i), nil
	case int64:
		return i, nil
	case int:
		return int64(i), nil
	default:
		return 0, fmt.Errorf("unsupported column type: %v", columnName)
	}
}

func (m *BinlogParser) uintHelper(e *canal.RowsEvent, n int, columnName string) (uint64, error) {

	columnId, err := m.getBinlogIdByName(e, columnName)
	if err != nil {
		return 0, err
	}
	fmt.Println(fmt.Sprintf("Helper columnName %v,columnId %v ,type %T",
		columnName, columnId, e.Rows[n][columnId]))

	switch i := e.Rows[n][columnId].(type) {
	case uint8:
		return uint64(i), nil
	case uint16:
		return uint64(i), nil
	case uint32:
		return uint64(i), nil
	case uint64:
		return i, nil
	case uint:
		return uint64(i), nil
	default:
		return 0, fmt.Errorf("unsupported column type: %v", columnName)
	}
}

func (m *BinlogParser) floatHelper(e *canal.RowsEvent, n int, columnName string) (float64, error) {

	columnId, err := m.getBinlogIdByName(e, columnName)
	if err != nil {
		return 0, err
	}
	fmt.Println(fmt.Sprintf("Helper columnName %v,columnId %v ,type %T",
		columnName, columnId, e.Rows[n][columnId]))

	switch i := e.Rows[n][columnId].(type) {
	case float32:
		return float64(i), nil
	case float64:
		return i, nil
	default:
		return 0, fmt.Errorf("unsupported column type: %v", columnName)
	}
}

func (m *BinlogParser) boolHelper(e *canal.RowsEvent, n int, columnName string) (bool, error) {
	val, err := m.intHelper(e, n, columnName)
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

func (m *BinlogParser) stringHelper(e *canal.RowsEvent, n int, columnName string) (string, error) {

	columnId, err := m.getBinlogIdByName(e, columnName)
	if err != nil {
		return "", err
	}
	fmt.Println(fmt.Sprintf("Helper columnName %v,columnId %v ,type %T",
		columnName, columnId, e.Rows[n][columnId]))
	// enum
	if e.Table.Columns[columnId].Type == schema.TYPE_ENUM {
		values := e.Table.Columns[columnId].EnumValues
		if len(values) == 0 {
			return "", nil
		}
		if e.Rows[n][columnId] == nil {
			return "", nil
		}

		return values[e.Rows[n][columnId].(int64)-1], nil
	}
	// string
	value := e.Rows[n][columnId]
	switch i := value.(type) {
	case []byte:
		return string(i), nil
	case string:
		return i, nil
	}
	return "", nil
}

func (m *BinlogParser) nullStringHelper(e *canal.RowsEvent, n int, columnName string) (sql.NullString, error) {

	nullString := sql.NullString{}
	columnId, err := m.getBinlogIdByName(e, columnName)
	if err != nil {
		return nullString, err
	}
	fmt.Println(fmt.Sprintf("Helper columnName %v,columnId %v ,type %T",
		columnName, columnId, e.Rows[n][columnId]))

	value := e.Rows[n][columnId]
	if value == nil {
		return nullString, nil
	}
	// string
	switch i := value.(type) {
	case []byte:
		nullString.String = string(i)
		nullString.Valid = true
		return nullString, nil
	case string:
		nullString.String = i
		nullString.Valid = true
		return nullString, nil
	}
	return nullString, nil
}

func (m *BinlogParser) nullInt64Helper(e *canal.RowsEvent, n int, columnName string) (sql.NullInt64, error) {

	nullInt64 := sql.NullInt64{}

	columnId, err := m.getBinlogIdByName(e, columnName)
	if err != nil {
		return nullInt64, err
	}

	fmt.Println(fmt.Sprintf("Helper columnName %v,columnId %v ,type %T",
		columnName, columnId, e.Rows[n][columnId]))

	value := e.Rows[n][columnId]
	if value == nil {
		return nullInt64, nil
	}
	// int64
	switch i := value.(type) {
	case int64:
		nullInt64.Valid = false
		nullInt64.Int64 = i
		return nullInt64, nil
	}

	return nullInt64, nil
}

func (m *BinlogParser) getBinlogIdByName(e *canal.RowsEvent, name string) (int, error) {
	for id, value := range e.Table.Columns {
		if value.Name == name {
			return id, nil
		}
	}
	fmt.Println(fmt.Sprintf("There is no column %s in table %s.%s",
		name, e.Table.Schema, e.Table.Name))

	return 0, errors.New("GetBinlogIdByName ERROR")
}

func parseTagSetting(tags reflect.StructTag) map[string]string {
	settings := map[string]string{}
	for _, str := range []string{tags.Get("sql"), tags.Get("gorm")} {
		tags := strings.Split(str, ";")
		for _, value := range tags {
			v := strings.Split(value, ":")
			k := strings.TrimSpace(strings.ToUpper(v[0]))
			if len(v) >= 2 {
				settings[k] = strings.Join(v[1:], ":")
			} else {
				settings[k] = k
			}
		}
	}
	return settings
}
