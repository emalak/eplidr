package eplidr

import (
	"encoding/hex"
	"fmt"
	"github.com/oppositemc/nonimus"
	"math/big"
	"strconv"
)

var (
	shiftTableIterator int
	pool               *nonimus.Pool
)

func init() {
	shiftTableIterator = 1
	SetLogger(&DefaultLogger{})
	pool = nonimus.NewPoolCollectorSize(2, 1000)
}

type Column struct { // Make column an interface
	Name  string
	Value interface{}
}
type Columns []Column

type Key struct {
	Name  string
	Value interface{}
}
type Keys []Key
type SelectColumn struct {
	Name   string
	Output interface{}
}
type SelectColumns []SelectColumn

func (s SelectColumns) Names() []string {
	var result []string
	for _, column := range s {
		result = append(result, column.Name)
	}
	return result
}

func (s SelectColumns) Query(table *Table) string {
	result := ""
	for i := 0; i < len(s.Names()); i++ {
		name := s.Names()[i]
		field := table.getField(name)
		if field == nil {
			continue
		}
		switch field.GetType() {
		case TypeUUID:
			result += fmt.Sprintf("BIN_TO_UUID(%s, true) AS %s,", name, name)
		default:
			result += fmt.Sprintf("`%s`,", name)
		}
	}
	return result[:len(result)-1]
}

func (keys Keys) Query(table *Table) string {
	if len(keys) == 0 {
		return ""
	}
	query := "WHERE "
	for i := 0; i < len(keys); i++ {
		if i == len(keys)-1 {
			query += fmt.Sprintf("`%s` = %s", keys[i].Name, keys[i].GetStringValue(table))
		} else {
			query += fmt.Sprintf("`%s` = %s AND ", keys[i].Name, keys[i].GetStringValue(table))
		}
	}
	return query
}
func (key Key) GetStringValue(table *Table) string {
	if table.getField(key.Name) == nil {
		logger.Debug(table.name)
		logger.Debug(key.Name)
	}
	if table.getField(key.Name).GetType().GetBasicType() == BasicTypeVarChar {
		return fmt.Sprintf("'%s'", key.Value)
	} else if table.getField(key.Name).GetType() == TypeUUID {
		return fmt.Sprintf("UUID_TO_BIN('%s', true)", key.Value)
	} else if table.getField(key.Name).GetType().GetBasicType() == BasicTypeVarByte {
		return fmt.Sprintf("UNHEX(%s)", value(key.Value))
	} else if table.getField(key.Name).GetType().GetBasicType() == BasicTypeBinary {
		return fmt.Sprintf("UNHEX(%s)", value(key.Value))
	} else {
		return value(key.Value)
	}

}
func (column Column) GetStringValue(table *Table) string {
	if table.getField(column.Name).GetType().GetBasicType() == BasicTypeVarChar {
		return fmt.Sprintf("'%s'", column.Value)
	} else if table.getField(column.Name).GetType() == TypeUUID {
		return fmt.Sprintf("UUID_TO_BIN('%s', true)", column.Value)
	} else if table.getField(column.Name).GetType().GetBasicType() == BasicTypeVarByte {
		return fmt.Sprintf("UNHEX(%s)", value(column.Value))
	} else if table.getField(column.Name).GetType().GetBasicType() == BasicTypeBinary {
		return fmt.Sprintf("UNHEX(%s)", value(column.Value))
	} else {
		return value(column.Value)
	}
}

func value(i interface{}) string {
	switch v := i.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case []interface{}: // Serialize s
		return fmt.Sprintf("'%v'", v[0])
	case int64:
		return strconv.FormatInt(v, 10)
	case big.Int:
		//logger.Debug(v.String(), hex.EncodeToString(v.Bytes()))
		return fmt.Sprintf("'%s'", hex.EncodeToString(v.Bytes()))
	case *big.Int:
		//logger.Debug(v.String(), hex.EncodeToString(v.Bytes()))
		return fmt.Sprintf("'%s'", hex.EncodeToString(v.Bytes()))
	case float64:
		return fmt.Sprintf("%f", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func ColumnNamesToQuery(names ...string) string {
	result := ""
	for i := 0; i < len(names); i++ {
		result += " `" + names[i] + "`,"
	}
	return result[:len(result)-1]
}

func PlainToColumns(keys []string, values []interface{}) Columns {
	columns := make(Columns, len(keys))
	for i := 0; i < len(keys); i++ {
		columns[i] = Column{
			Name:  keys[i],
			Value: values[i],
		}
	}
	return columns
}

func StandardGetShardFunc(key interface{}) uint {
	return fnv32(fmt.Sprintf("%v", key))
}
func fnv32(key string) uint {
	hash := uint(2166136261)
	const prime32 = uint(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint(key[i])
	}
	return hash
}
