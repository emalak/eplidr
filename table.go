package eplidr

import (
	"database/sql"
	"fmt"
	"github.com/oppositemc/nonimus"
	"strconv"
	"strings"
)

type Table struct {
	name string

	shardsCount uint
	Shards      []*Shard

	// fields = columns
	fields    TableFields
	fieldsMap map[FieldName]TableField

	hashFunc func(interface{}) uint
}

type Drivers interface{}

func NewTable(name string, shardsCount uint, fields TableFields, driverParam Drivers) (*Table, error) {
	var table *Table
	switch dataSource := driverParam.(type) {
	case []*sql.DB:
		table = &Table{
			name:        name,
			fields:      fields,
			shardsCount: shardsCount,
			hashFunc:    StandardGetShardFunc,
		}
		shards := make([]*Shard, len(dataSource))
		for i := 0; i < len(dataSource); i++ {
			shards[i] = &Shard{
				table:  table,
				driver: dataSource[i],
				num:    uint(i),
			}
		}
		table.Shards = shards
	case *sql.DB:
		drivers := make([]*sql.DB, shardsCount)
		for i := 0; i < int(shardsCount); i++ {
			drivers[i] = dataSource
		}
		table = &Table{
			name:        name,
			fields:      fields,
			shardsCount: shardsCount,
			hashFunc:    StandardGetShardFunc,
		}
		shards := make([]*Shard, len(drivers))
		for i := 0; i < len(drivers); i++ {
			shards[i] = &Shard{
				table:  table,
				driver: drivers[i],
				num:    uint(i),
			}
		}
		table.Shards = shards
	}
	table.fields = fields
	fieldsMap := make(map[FieldName]TableField)
	for _, field := range table.fields {
		fieldsMap[FieldName(strings.ToLower(field.GetName()))] = field
	}
	table.fieldsMap = fieldsMap
	return table, table.Init()
}

func (table *Table) GetName(shard uint) string {
	return table.name + strconv.FormatUint(uint64(shard), 10)
}
func (table *Table) getField(name string) TableField {
	return table.fieldsMap[FieldName(strings.ToLower(name))]
}
func (table *Table) getFieldNames() []string {
	var result []string
	for _, field := range table.fields {
		result = append(result, field.GetName())
	}
	return result
}
func (table *Table) GetShardNum(key interface{}) uint {
	return table.hashFunc(key) % table.shardsCount
}
func (table *Table) GetShard(num uint) *Shard {
	return table.Shards[num]
}

func (table *Table) Init() error {
	for shardId := 0; shardId < len(table.Shards); shardId++ {
		fieldsString := ""
		var postSQLs []string
		for _, field := range table.fields {
			fieldsString += field.QueryInit(table.GetName(uint(shardId))) + ", "
			queryAfter := field.QueryAfter(table.GetName(uint(shardId)))
			if queryAfter != "" {
				postSQLs = append(postSQLs, field.QueryAfter(table.GetName(uint(shardId)))+";")
			}
		}
		fieldsString = fieldsString[:len(fieldsString)-2]
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", table.GetName(uint(shardId)), fieldsString)
		rows, err := table.Shards[shardId].Query(fmt.Sprintf("SHOW TABLES LIKE '%s';", table.GetName(uint(shardId)))) // TODO DESCRIBE TABLES AND ALTER TABLE IF NEEDED
		if err != nil {
			return err
		}
		if rows.Next() {
			err = rows.Close()
			if err != nil {
				return err
			}
			continue
		}
		_, err = table.Shards[shardId].Exec(sql)
		if err != nil {
			return err
		}
		for _, postSQL := range postSQLs {
			logger.Info(postSQL)
			_, err = table.Shards[shardId].Exec(postSQL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (table *Table) GradualSelect(shardKey interface{}, keys Keys) (*GradualSelectResult, error) {
	return table.GetShard(table.GetShardNum(shardKey)).GradualSelect(keys)
}
func (table *Table) FullSelect(shardKey interface{}, keys Keys) (*FullSelectResult, error) {
	return table.GetShard(table.GetShardNum(shardKey)).FullSelect(keys)
}

func (table *Table) GetString(key Key, column string) (string, bool, error) {
	var result string
	err, found := table.Get(key.Value, Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return "", found, err
	}
	return result, found, nil
}
func (table *Table) GetInt(key Key, column string) (int, bool, error) {
	var result int
	err, found := table.Get(key.Value, Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetInt64(key Key, column string) (int64, bool, error) {
	var result int64
	err, found := table.Get(key.Value, Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetFloat(key Key, column string) (float64, bool, error) {
	var result float64
	err, found := table.Get(key.Value, Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetUint(key Key, column string) (uint64, bool, error) {
	var result uint64
	err, found := table.Get(key.Value, Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetBoolean(key Key, column string) (bool, bool, error) {
	var result bool
	err, found := table.Get(key.Value, Keys{key}, SelectColumns{{column, &result}})
	if err != nil {
		return false, found, err
	}
	return result, found, nil
}
func (table *Table) Put(shardKey interface{}, values Columns) error {
	return table.Shards[table.GetShardNum(shardKey)].Put(values)
}
func (table *Table) PutOrUpdate(shardKey interface{}, values Columns) error {
	return table.Shards[table.GetShardNum(shardKey)].PutOrUpdate(values)
}
func (table *Table) Set(shardKey interface{}, keys Keys, values Columns) error {
	return table.Shards[table.GetShardNum(shardKey)].Set(keys, values)
}
func (table *Table) Add(shardKey interface{}, keys Keys, values Columns) error {
	return table.Shards[table.GetShardNum(shardKey)].Add(keys, values)
}
func (table *Table) Remove(shardKey interface{}, keys Keys) error {
	return table.Shards[table.GetShardNum(shardKey)].Remove(keys)
}

func (table *Table) Get(shardKey interface{}, keys Keys, columns SelectColumns) (error, bool) { // Promise: found
	return table.Shards[table.GetShardNum(shardKey)].Get(keys, columns)
}
func (table *Table) AsyncGet(shardKey interface{}, keys Keys, columns SelectColumns) *nonimus.Promise[bool] { // Promise: found
	return table.Shards[table.GetShardNum(shardKey)].AsyncGet(keys, columns)
}
func (table *Table) AsyncPut(shardKey interface{}, values Columns) *nonimus.Promise[sql.Result] {
	return table.Shards[table.GetShardNum(shardKey)].AsyncPut(values)
}
func (table *Table) AsyncPutOrUpdate(shardKey interface{}, values Columns) *nonimus.Promise[sql.Result] {
	return table.Shards[table.GetShardNum(shardKey)].AsyncPutOrUpdate(values)
}
func (table *Table) AsyncSet(shardKey interface{}, keys Keys, values Columns) *nonimus.Promise[sql.Result] {
	return table.Shards[table.GetShardNum(shardKey)].AsyncSet(keys, values)
}
func (table *Table) AsyncAdd(shardKey interface{}, keys Keys, values Columns) *nonimus.Promise[sql.Result] {
	return table.Shards[table.GetShardNum(shardKey)].AsyncAdd(keys, values)
}
func (table *Table) AsyncRemove(shardKey interface{}, keys Keys) *nonimus.Promise[sql.Result] {
	return table.Shards[table.GetShardNum(shardKey)].AsyncRemove(keys)
}

func (table *Table) AsyncExec(query string, key interface{}) *nonimus.Promise[sql.Result] {
	shardNum := table.GetShardNum(key)
	return table.Shards[shardNum].AsyncExec(query)
}
func (table *Table) Exec(query string, key interface{}) (sql.Result, error) {
	shardNum := table.GetShardNum(key)
	return table.Shards[shardNum].Exec(query)
}
func (table *Table) StartTx(key interface{}) (*sql.Tx, error) {
	shardNum := table.GetShardNum(key)
	return table.Shards[shardNum].RawTx()
}
func (table *Table) Query(query string, key interface{}) (*sql.Rows, error) {
	shardNum := table.GetShardNum(key)
	return table.Shards[shardNum].Query(query)
}

func (table *Table) ReleaseRows(rows *sql.Rows) error {
	return rows.Close()
}

func (table *Table) SingleSet(shardKey interface{}, keys Keys, column Column) error {
	return table.Set(shardKey, keys, Columns{column})
}

func (table *Table) DropUnsafe() {
	for i := 0; i < len(table.Shards); i++ {
		table.Shards[i].Drop()
	}
}

func (table *Table) GlobalExecUnsafe(query string) error {
	for i := 0; i < len(table.Shards); i++ {
		_, err := table.Shards[i].Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (table *Table) GetFields() TableFields {
	return table.fields
}
