package eplidr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/oppositemc/nonimus"
	"math/big"
	"strings"
)

type Shard struct {
	table  *Table
	driver *sql.DB
	num    uint
}

// GradualSelectResult is using for select when you do not want to save all the selected data
type GradualSelectResult struct {
	cache  map[string]interface{} // Pointers
	fields TableFields
	rows   *sql.Rows
}

func (res *GradualSelectResult) Next() (bool, error) {
	ok := res.rows.Next()
	if !ok {
		return false, nil
	}
	var sgbdrn []interface{}
	i := 0
	for _, field := range res.fields {
		var fieldDestPtr any
		switch field.GetType().GetBasicType() {
		case BasicTypeFloat:
			{
				fieldDestPtr = new(float64)
			}
		case BasicTypeBool:
			{
				fieldDestPtr = new(bool)
			}
		case BasicTypeVarChar:
			{
				fieldDestPtr = new(string)
			}
		case BasicTypeInt64:
			{
				fieldDestPtr = new(int64)
			}
		case BasicTypeInt32:
			{
				fieldDestPtr = new(int)
			}
		case BasicTypeUint64:
			{
				fieldDestPtr = new(uint64)
			}
		case BasicTypeUint32:
			{
				fieldDestPtr = new(uint)
			}
		case BasicTypeBinary:
			{
				fieldDestPtr = new([]byte)
			}
		case BasicTypeVarByte:
			{
				fieldDestPtr = new([]byte)
			}
		case BasicTypeNone:
			{
				logger.Debug("typeNone ", field.GetName())
				continue
			}
		default:
			panic("Error! No basic type found for " + field.GetName() + " type query: " + field.GetType().Query())
		}
		sgbdrn = append(sgbdrn, fieldDestPtr)
		i++
	}
	err := res.rows.Scan(sgbdrn...)
	if err != nil {
		return false, err
	}
	i = 0
	for _, field := range res.fields {
		//logger.Debug("Field: ", field.GetName(), field.GetType().GetBasicType())
		switch field.GetType().GetBasicType() {
		case BasicTypeFloat:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*float64))
			}
		case BasicTypeBool:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*bool))
			}
		case BasicTypeVarChar:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*string))
			}
		case BasicTypeInt64:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*int64))
			}
		case BasicTypeInt32:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*int))
			}
		case BasicTypeUint64:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*uint64))
			}
		case BasicTypeUint32:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*uint))
			}
		case BasicTypeBinary:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*[]byte))
			}
		case BasicTypeVarByte:
			{
				res.cache[field.GetName()] = *(sgbdrn[i].(*[]byte))
			}
		case BasicTypeNone:
			{
				continue
			}
		default:
			panic("Error! No basic type found for " + field.GetName() + " type query: " + field.GetType().Query())
		}
		i++
	}
	for _, field := range res.fields {
		switch field.GetType() {
		case TypeBigInt:
			res.cache[field.GetName()] = new(big.Int).SetBytes(res.cache[field.GetName()].([]byte))
		}
	}
	return true, nil
}
func (res *GradualSelectResult) Get(name string) interface{} {
	return res.cache[name]
}
func (res *GradualSelectResult) GetString(name string) string {
	return res.cache[name].(string)
}
func (res *GradualSelectResult) GetInt(name string) int {
	return res.cache[name].(int)
}
func (res *GradualSelectResult) GetInt64(name string) int64 {
	return res.cache[name].(int64)
}
func (res *GradualSelectResult) GetUint64(name string) uint64 {
	return res.cache[name].(uint64)
}
func (res *GradualSelectResult) GetBool(name string) bool {
	return res.cache[name].(bool)
}
func (res *GradualSelectResult) GetFloat64(name string) float64 {
	return res.cache[name].(float64)
}

func (res *GradualSelectResult) GetBigInt(name string) *big.Int {
	return res.cache[name].(*big.Int)
}

func (shard *Shard) GradualSelect(keys Keys) (*GradualSelectResult, error) {
	query := fmt.Sprintf("SELECT %s FROM {table} %s;", ColumnNamesToQuery(shard.table.getFieldNames()...), keys.Query(shard.table))
	rows, err := shard.Query(query)
	if err != nil {
		return nil, err
	}
	return &GradualSelectResult{
		cache:  make(map[string]interface{}),
		fields: shard.table.fields,
		rows:   rows,
	}, nil
}

type FullSelectResult struct {
	cache   [][]interface{} // Pointers
	pointer int
	fields  TableFields
	rows    *sql.Rows
}

func (res *FullSelectResult) scan() error {
	for res.rows.Next() {
		var data []any
		var pointers []any
		columnIndex := 0
		for _, field := range res.fields {
			switch field.GetType().GetBasicType() {
			case BasicTypeFloat:
				{
					data = append(data, float64(0))
					pointers = append(pointers, &data[columnIndex])
				}
			case BasicTypeBool:
				{
					data = append(data, false)
					pointers = append(pointers, &data[columnIndex])
				}
			case BasicTypeVarChar:
				{
					data = append(data, "")
					pointers = append(pointers, &data[columnIndex])
				}
			case BasicTypeInt64:
				{
					data = append(data, int64(0))
					pointers = append(pointers, &data[columnIndex])
				}
			case BasicTypeInt32:
				{
					data = append(data, int32(0))
					pointers = append(pointers, &data[columnIndex])
				}
			case BasicTypeUint64:
				{
					data = append(data, uint64(0))
					pointers = append(pointers, &data[columnIndex])
				}
			case BasicTypeUint32:
				{
					data = append(data, uint32(0))
					pointers = append(pointers, &data[columnIndex])
				}
			case BasicTypeNone:
				{
					logger.Debug("typeNone ", field.GetName())
					continue
				}
			default:
				panic("Error! No basic type found for " + field.GetName() + " type query: " + field.GetType().Query())
			}
			columnIndex++
		}
		err := res.rows.Scan(pointers...)
		if err != nil {
			return err
		}
		res.cache = append(res.cache, data)
	}
	return nil
}

func (res *FullSelectResult) Next() bool {
	res.pointer++
	var result = len(res.cache) > res.pointer
	return result
}
func (res *FullSelectResult) Get(name string) interface{} {
	for i := 0; i < len(res.fields); i++ {
		if res.fields[i].GetName() == name {
			return res.cache[res.pointer][i]
		}
	}
	return nil
}
func (res *FullSelectResult) GetString(name string) string {
	return res.Get(name).(string)
}
func (res *FullSelectResult) GetUUID(name string) *uuid.UUID {
	for i := 0; i < len(res.fields); i++ {
		if res.fields[i].GetName() == name {
			id := res.cache[res.pointer][i].(uuid.UUID)
			return &id
		}
	}
	return nil
}

func (shard *Shard) FullSelect(keys Keys) (*FullSelectResult, error) {
	query := fmt.Sprintf("SELECT %s FROM {table} %s;", ColumnNamesToQuery(shard.table.getFieldNames()...), keys.Query(shard.table))
	rows, err := shard.Query(query)
	if err != nil {
		return nil, err
	}
	result := &FullSelectResult{
		fields:  shard.table.fields,
		rows:    rows,
		pointer: -1,
	}
	return result, result.scan()
}
func (shard *Shard) AsyncFullSelect(keys Keys) *nonimus.Promise[*FullSelectResult] {
	return nonimus.AddPromise(pool, func(resolve func(*FullSelectResult), reject func(error)) {
		query := fmt.Sprintf("SELECT %s FROM {table} %s;", ColumnNamesToQuery(shard.table.getFieldNames()...), keys.Query(shard.table))
		rows, err := shard.Query(query)
		if err != nil {
			reject(err)
			return
		}
		result := &FullSelectResult{
			fields:  shard.table.fields,
			rows:    rows,
			pointer: -1,
		}
		err = result.scan()
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}

func (shard *Shard) GetString(key Key, column string) (string, bool, error) {
	var result string
	err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return "", found, err
	}
	return result, found, nil
}
func (shard *Shard) GetInt(key Key, column string) (int, bool, error) {
	var result int
	err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (shard *Shard) GetInt64(key Key, column string) (int64, bool, error) {
	var result int64
	err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (shard *Shard) GetFloat(key Key, column string) (float64, bool, error) {
	var result float64
	err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (shard *Shard) GetUint64(key Key, column string) (uint64, bool, error) {
	var result uint64
	err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (shard *Shard) GetUint(key Key, column string) (uint, bool, error) {
	var result uint
	err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (shard *Shard) GetBoolean(key Key, column string) (bool, bool, error) {
	var result bool
	err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return false, found, err
	}
	return result, found, nil
}

type GetResult[T any] struct {
	Result T
	Found  bool
}

func (shard *Shard) AsyncGetString(key Key, column string) *nonimus.Promise[GetResult[string]] {
	return nonimus.AddPromise(pool, func(resolve func(GetResult[string]), reject func(error)) {
		var result string
		err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
		if err != nil {
			reject(err)
			return
		}
		resolve(GetResult[string]{Result: result, Found: found})
	})
}
func (shard *Shard) AsyncGetInt(key Key, column string) *nonimus.Promise[GetResult[int]] {
	return nonimus.AddPromise(pool, func(resolve func(GetResult[int]), reject func(error)) {
		var result int
		err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
		if err != nil {
			reject(err)
			return
		}
		resolve(GetResult[int]{Result: result, Found: found})
	})
}
func (shard *Shard) AsyncGetInt64(key Key, column string) *nonimus.Promise[GetResult[int64]] {
	return nonimus.AddPromise(pool, func(resolve func(GetResult[int64]), reject func(error)) {
		var result int64
		err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
		if err != nil {
			reject(err)
			return
		}
		resolve(GetResult[int64]{Result: result, Found: found})
	})
}
func (shard *Shard) AsyncGetFloat(key Key, column string) *nonimus.Promise[GetResult[float64]] {
	return nonimus.AddPromise(pool, func(resolve func(GetResult[float64]), reject func(error)) {
		var result float64
		err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
		if err != nil {
			reject(err)
			return
		}
		resolve(GetResult[float64]{Result: result, Found: found})
	})
}
func (shard *Shard) AsyncGetUint64(key Key, column string) *nonimus.Promise[GetResult[uint64]] {
	return nonimus.AddPromise(pool, func(resolve func(GetResult[uint64]), reject func(error)) {
		var result uint64
		err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
		if err != nil {
			reject(err)
			return
		}
		resolve(GetResult[uint64]{Result: result, Found: found})
	})
}
func (shard *Shard) AsyncGetUint(key Key, column string) *nonimus.Promise[GetResult[uint]] {
	return nonimus.AddPromise(pool, func(resolve func(GetResult[uint]), reject func(error)) {
		var result uint
		err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
		if err != nil {
			reject(err)
			return
		}
		resolve(GetResult[uint]{Result: result, Found: found})
	})
}
func (shard *Shard) AsyncGetBoolean(key Key, column string) *nonimus.Promise[GetResult[bool]] {
	return nonimus.AddPromise(pool, func(resolve func(GetResult[bool]), reject func(error)) {
		var result bool
		err, found := shard.Get(Keys{key}, SelectColumns{{column, &result}})
		if err != nil {
			reject(err)
			return
		}
		resolve(GetResult[bool]{Result: result, Found: found})
	})
}

type PostProcessScanField struct {
	Type       Type
	TempOutput interface{}
	RealOutput interface{}
}

func (shard *Shard) Get(keys Keys, columns SelectColumns) (error, bool) {
	query := fmt.Sprintf("SELECT %s FROM {table} %s;", columns.Query(shard.table), keys.Query(shard.table))
	var outputs []interface{}
	var postProcesses []PostProcessScanField
	for _, column := range columns {
		switch shard.table.getField(column.Name).GetType() {
		case TypeBigInt:
			{
				var bytes []byte
				outputs = append(outputs, &bytes)
				postProcesses = append(postProcesses, PostProcessScanField{TypeBigInt, &bytes, column.Output})
				break
			}
		default:
			{
				outputs = append(outputs, column.Output)
				break
			}
		}
	}
	rows, err := shard.Query(query)
	if err != nil {
		return err, false
	}
	if rows.Next() {
		err = rows.Scan(outputs...)
		if err != nil {
			err = rows.Close()
			if err != nil {
				return err, false
			}
			return err, true
		}
		err = rows.Close()
		if err != nil {
			return err, false
		}
		for _, postProcess := range postProcesses {
			switch postProcess.Type {
			case TypeBigInt:
				{
					intBytesPtr, ok := (postProcess.TempOutput).(*[]byte)
					if !ok {
						return errors.New("error on postProcess, postProcess.TempOutput is not *[]byte"), true
					}
					intPtr, ok := (postProcess.RealOutput).(**big.Int)
					if !ok {
						intPtr, ok := (postProcess.RealOutput).(*big.Int)
						if !ok {
							return errors.New("error on postProcess, postProcess.RealOutput is not **big.Int or *big.Int"), true
						}
						*intPtr = *new(big.Int).SetBytes(*intBytesPtr)
					} else {
						*intPtr = new(big.Int).SetBytes(*intBytesPtr)
					}
					break
				}
			}
		}
	} else {
		err = rows.Close()
		if err != nil {
			return err, false
		}
		return nil, false
	}
	return nil, true
}
func (shard *Shard) Put(values Columns) error {
	// `%s` = ?
	columnsString := ""
	valuesString := ""
	for i := 0; i < len(values); i++ {
		if i == len(values)-1 {
			columnsString += fmt.Sprintf("`%s`", values[i].Name)
		} else {
			columnsString += fmt.Sprintf("`%s`, ", values[i].Name)
		}
	}
	for i := 0; i < len(values); i++ {
		if i == len(values)-1 {
			valuesString += fmt.Sprintf("%s", values[i].GetStringValue(shard.table))
		} else {
			valuesString += fmt.Sprintf("%s, ", values[i].GetStringValue(shard.table))
		}
	}
	query := fmt.Sprintf("INSERT INTO {table} (%s) values (%s);", columnsString, valuesString)
	_, err := shard.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (shard *Shard) PutOrUpdate(values Columns) error {
	columnsString := ""
	valuesString := ""
	updateString := ""
	for i := 0; i < len(values); i++ {
		if i == len(values)-1 {
			columnsString += fmt.Sprintf("`%s`", values[i].Name)
		} else {
			columnsString += fmt.Sprintf("`%s`, ", values[i].Name)
		}
		if i == len(values)-1 {
			valuesString += fmt.Sprintf("%s", values[i].GetStringValue(shard.table))
		} else {
			valuesString += fmt.Sprintf("%s, ", values[i].GetStringValue(shard.table))
		}
		if i == len(values)-1 {
			updateString += fmt.Sprintf("`%s` = %s", values[i].Name, values[i].GetStringValue(shard.table))
		} else {
			updateString += fmt.Sprintf("`%s` = %s, ", values[i].Name, values[i].GetStringValue(shard.table))
		}
	}
	query := fmt.Sprintf("INSERT INTO {table} (%s) values (%s) ON DUPLICATE KEY UPDATE %s;", columnsString, valuesString, updateString)
	_, err := shard.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (shard *Shard) Set(keys Keys, values Columns) error {
	s := ""
	for i := 0; i < len(values); i++ {
		if i == len(values)-1 {
			s += fmt.Sprintf("`%s` = %s", values[i].Name, values[i].GetStringValue(shard.table))
		} else {
			s += fmt.Sprintf("`%s` = %s, ", values[i].Name, values[i].GetStringValue(shard.table))
		}
	}
	query := fmt.Sprintf("UPDATE {table} SET %s %s;", s, keys.Query(shard.table))
	_, err := shard.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (shard *Shard) Add(keys Keys, values Columns) error {
	s := ""
	for i := 0; i < len(values); i++ {
		if i == len(values)-1 {
			s += fmt.Sprintf("`%s` = `%s` + %s", values[i].Name, values[i].Name, value(values[i].Value))
		} else {
			s += fmt.Sprintf("`%s` = `%s` + %s, ", values[i].Name, values[i].Name, value(values[i].Value))
		}
	}
	query := fmt.Sprintf("UPDATE {table} SET %s %s;", s, keys.Query(shard.table))
	_, err := shard.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (shard *Shard) Remove(keys Keys) error {
	query := fmt.Sprintf("DELETE FROM {table} %s;", keys.Query(shard.table))
	_, err := shard.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (shard *Shard) AsyncGet(keys Keys, columns SelectColumns) *nonimus.Promise[bool] {
	return nonimus.AddPromise(pool, func(resolve func(bool), reject func(error)) {
		query := fmt.Sprintf("SELECT %s FROM {table} %s;", columns.Query(shard.table), keys.Query(shard.table))
		var outputs []interface{}
		var postProcesses []PostProcessScanField
		for _, column := range columns {
			switch shard.table.getField(column.Name).GetType() {
			case TypeBigInt:
				{
					var bytes []byte
					outputs = append(outputs, &bytes)
					postProcesses = append(postProcesses, PostProcessScanField{TypeBigInt, &bytes, column.Output})
					break
				}
			default:
				{
					outputs = append(outputs, column.Output)
					break
				}
			}
		}
		rows, err := shard.Query(query)
		if err != nil {
			reject(err)
			return
		}
		if rows.Next() {
			err = rows.Scan(outputs...)
			if err != nil {
				closeErr := rows.Close()
				if err != nil {
					logger.Error(closeErr.Error())
				}
				reject(err)
				return
			}
			err = rows.Close()
			if err != nil {
				reject(err)
				return
			}
			for _, postProcess := range postProcesses {
				switch postProcess.Type {
				case TypeBigInt:
					{
						intBytesPtr, ok := (postProcess.TempOutput).(*[]byte)
						if !ok {
							reject(errors.New("error on postProcess, postProcess.TempOutput is not *[]byte"))
							return
						}
						intPtr, ok := (postProcess.RealOutput).(**big.Int)
						if !ok {
							intPtr, ok := (postProcess.RealOutput).(*big.Int)
							if !ok {
								reject(errors.New("error on postProcess, postProcess.RealOutput is not **big.Int or *big.Int"))
								return
							}
							*intPtr = *new(big.Int).SetBytes(*intBytesPtr)
						} else {
							*intPtr = new(big.Int).SetBytes(*intBytesPtr)
						}
						break
					}
				}
			}
		} else {
			err = rows.Close()
			if err != nil {
				reject(err)
				return
			}
			return
		}
		resolve(true)
		return
	})
}
func (shard *Shard) AsyncPut(values Columns) *nonimus.Promise[sql.Result] {
	return nonimus.AddPromise(pool, func(resolve func(sql.Result), reject func(error)) {
		columnsString := ""
		valuesString := ""
		for i := 0; i < len(values); i++ {
			if i == len(values)-1 {
				columnsString += fmt.Sprintf("`%s`", values[i].Name)
			} else {
				columnsString += fmt.Sprintf("`%s`, ", values[i].Name)
			}
		}
		for i := 0; i < len(values); i++ {
			if i == len(values)-1 {
				valuesString += fmt.Sprintf("%s", values[i].GetStringValue(shard.table))
			} else {
				valuesString += fmt.Sprintf("%s, ", values[i].GetStringValue(shard.table))
			}
		}
		query := fmt.Sprintf("INSERT INTO {table} (%s) values (%s);", columnsString, valuesString)
		result, err := shard.Exec(query)
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}
func (shard *Shard) AsyncPutOrUpdate(values Columns) *nonimus.Promise[sql.Result] {
	return nonimus.AddPromise(pool, func(resolve func(sql.Result), reject func(error)) {
		columnsString := ""
		valuesString := ""
		updateString := ""
		for i := 0; i < len(values); i++ {
			if i == len(values)-1 {
				columnsString += fmt.Sprintf("`%s`", values[i].Name)
			} else {
				columnsString += fmt.Sprintf("`%s`, ", values[i].Name)
			}
			if i == len(values)-1 {
				valuesString += fmt.Sprintf("%s", values[i].GetStringValue(shard.table))
			} else {
				valuesString += fmt.Sprintf("%s, ", values[i].GetStringValue(shard.table))
			}
			if i == len(values)-1 {
				updateString += fmt.Sprintf("`%s` = %s", values[i].Name, values[i].GetStringValue(shard.table))
			} else {
				updateString += fmt.Sprintf("`%s` = %s, ", values[i].Name, values[i].GetStringValue(shard.table))
			}
		}
		result, err := shard.Exec(fmt.Sprintf("INSERT INTO {table} (%s) values (%s) ON DUPLICATE KEY UPDATE %s;", columnsString, valuesString, updateString))
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}
func (shard *Shard) AsyncSet(keys Keys, values Columns) *nonimus.Promise[sql.Result] {
	return nonimus.AddPromise(pool, func(resolve func(sql.Result), reject func(error)) {
		s := ""
		for i := 0; i < len(values); i++ {
			if i == len(values)-1 {
				s += fmt.Sprintf("`%s` = %s", values[i].Name, values[i].GetStringValue(shard.table))
			} else {
				s += fmt.Sprintf("`%s` = %s, ", values[i].Name, values[i].GetStringValue(shard.table))
			}
		}
		result, err := shard.Exec(fmt.Sprintf("UPDATE {table} SET %s %s;", s, keys.Query(shard.table)))
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}
func (shard *Shard) AsyncAdd(keys Keys, values Columns) *nonimus.Promise[sql.Result] {
	return nonimus.AddPromise(pool, func(resolve func(sql.Result), reject func(error)) {
		s := ""
		for i := 0; i < len(values); i++ {
			if i == len(values)-1 {
				s += fmt.Sprintf("`%s` = `%s` + %s", values[i].Name, values[i].Name, value(values[i].Value))
			} else {
				s += fmt.Sprintf("`%s` = `%s` + %s, ", values[i].Name, values[i].Name, value(values[i].Value))
			}
		}
		query := fmt.Sprintf("UPDATE {table} SET %s %s;", s, keys.Query(shard.table))
		result, err := shard.Exec(query)
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}
func (shard *Shard) AsyncRemove(keys Keys) *nonimus.Promise[sql.Result] {
	return nonimus.AddPromise(pool, func(resolve func(sql.Result), reject func(error)) {
		result, err := shard.Exec(fmt.Sprintf("DELETE FROM {table} %s;", keys.Query(shard.table)))
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}

func (shard *Shard) AsyncExec(query string) *nonimus.Promise[sql.Result] {
	return nonimus.AddPromise(pool, func(resolve func(sql.Result), reject func(error)) {
		query = strings.Replace(query, "{table}", fmt.Sprintf("`%s`", shard.table.GetName(shard.num)), 1)
		//logger.Debug(query)
		result, err := shard.driver.Exec(query)
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}
func (shard *Shard) Exec(query string) (sql.Result, error) {
	query = strings.Replace(query, "{table}", fmt.Sprintf("`%s`", shard.table.GetName(shard.num)), 1)
	//logger.Debug(query)
	return shard.driver.Exec(query)
}
func (shard *Shard) AsyncQuery(query string) *nonimus.Promise[*sql.Rows] {
	return nonimus.AddPromise(pool, func(resolve func(*sql.Rows), reject func(error)) {
		query = strings.Replace(query, "{table}", fmt.Sprintf("`%s`", shard.table.GetName(shard.num)), 1)
		//logger.Debug(query)
		rows, err := shard.driver.Query(query)
		if err != nil {
			reject(err)
			return
		}
		resolve(rows)
	})
}
func (shard *Shard) Query(query string) (*sql.Rows, error) {
	query = strings.Replace(query, "{table}", fmt.Sprintf("`%s`", shard.table.GetName(shard.num)), 1)
	//logger.Debug(query)
	return shard.driver.Query(query)
}

func (shard *Shard) ReleaseRows(rows *sql.Rows) error {
	return rows.Close()
}

func (shard *Shard) RawTx() (*sql.Tx, error) {
	return shard.driver.Begin()
}

func (shard *Shard) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return shard.driver.BeginTx(ctx, opts)
}

func (shard *Shard) SingleSet(keys Keys, column Column) error {
	return shard.Set(keys, Columns{column})
}

func (shard *Shard) Drop() error {
	_, err := shard.driver.Exec(fmt.Sprintf("DROP TABLE %s;", shard.table.GetName(shard.num)))
	return err
}
