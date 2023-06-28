package eplidr

import (
	"context"
	"database/sql"
	"github.com/oppositemc/nonimus"
)

type SingleKeyTable struct {
	Table *Table
	key   string
}

func NewSingleKeyTable(name string, key string, shardsCount uint, fields TableFields, drivers Drivers) (*SingleKeyTable, error) {
	// params:
	// [0] dataSource
	// [1]
	table, err := NewTable(name, shardsCount, fields, drivers)
	if err != nil {
		return nil, err
	}
	return &SingleKeyTable{
		Table: table,
		key:   key,
	}, nil
}

func SingleKeyImplementation(keyTable *Table, key string) *SingleKeyTable {
	return &SingleKeyTable{
		Table: keyTable,
		key:   key,
	}
}

func (table *SingleKeyTable) GetString(key interface{}, column string) (string, bool, error) {
	return table.Table.GetString(Key{Name: table.key, Value: key}, column)
}
func (table *SingleKeyTable) GetInt(key interface{}, column string) (int, bool, error) {
	return table.Table.GetInt(Key{Name: table.key, Value: key}, column)
}
func (table *SingleKeyTable) GetInt64(key interface{}, column string) (int64, bool, error) {
	return table.Table.GetInt64(Key{Name: table.key, Value: key}, column)
}
func (table *SingleKeyTable) GetFloat(key interface{}, column string) (float64, bool, error) {
	return table.Table.GetFloat(Key{Name: table.key, Value: key}, column)
}
func (table *SingleKeyTable) GetUint(key interface{}, column string) (uint64, bool, error) {
	return table.Table.GetUint(Key{Name: table.key, Value: key}, column)
}
func (table *SingleKeyTable) GetBoolean(key interface{}, column string) (bool, bool, error) {
	return table.Table.GetBoolean(Key{Name: table.key, Value: key}, column)
}

func (table *SingleKeyTable) Get(key interface{}, columns SelectColumns) (error, bool) {
	return table.Table.Get(key, Keys{{table.key, key}}, columns)
}
func (table *SingleKeyTable) Set(key interface{}, columns Columns) error {
	return table.Table.Set(key, Keys{{table.key, key}}, columns)
}
func (table *SingleKeyTable) Add(key interface{}, columns Columns) error {
	return table.Table.Add(key, Keys{{table.key, key}}, columns)
}
func (table *SingleKeyTable) SingleSet(key interface{}, column Column) error {
	return table.Table.Set(key, Keys{{table.key, key}}, Columns{column})
}
func (table *SingleKeyTable) Put(key interface{}, columns Columns) error {
	return table.Table.Put(key, columns)
}
func (table *SingleKeyTable) Remove(key interface{}) error {
	return table.Table.Remove(table.key, Keys{{table.key, key}})
}
func (table *SingleKeyTable) AsyncGet(key interface{}, columns SelectColumns) *nonimus.Promise[bool] {
	return table.Table.AsyncGet(key, Keys{{table.key, key}}, columns)
}
func (table *SingleKeyTable) AsyncSet(key interface{}, columns Columns) *nonimus.Promise[sql.Result] {
	return table.Table.AsyncSet(key, Keys{{table.key, key}}, columns)
}
func (table *SingleKeyTable) AsyncAdd(key interface{}, columns Columns) *nonimus.Promise[sql.Result] {
	return table.Table.AsyncAdd(key, Keys{{table.key, key}}, columns)
}
func (table *SingleKeyTable) AsyncSingleSet(key interface{}, column Column) *nonimus.Promise[sql.Result] {
	return table.Table.AsyncSet(key, Keys{{table.key, key}}, Columns{column})
}
func (table *SingleKeyTable) AsyncPut(key interface{}, columns Columns) *nonimus.Promise[sql.Result] {
	return table.Table.AsyncPut(key, columns)
}
func (table *SingleKeyTable) AsyncRemove(key interface{}) *nonimus.Promise[sql.Result] {
	return table.Table.AsyncRemove(table.key, Keys{{table.key, key}})
}

func (table *SingleKeyTable) ReleaseRows(rows *sql.Rows) error {
	return rows.Close()
}

func (table *SingleKeyTable) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, nil
}

func (table *SingleKeyTable) Exec(query string, key interface{}) (sql.Result, error) {
	return table.Table.Exec(query, key)
}
func (table *SingleKeyTable) AsyncExec(query string, key interface{}) *nonimus.Promise[sql.Result] {
	return table.Table.AsyncExec(query, key)
}
func (table *SingleKeyTable) Query(query string, key interface{}) (*sql.Rows, error) {
	return table.Table.Query(query, key)
}

/*
rows, err = Database.Landmarks.Query(fmt.Sprintf(
"SELECT `id`, `name`, `description`, `timestamp`, `photo`, `creator`, `location` FROM {table} WHERE `name` LIKE '%%%s%%' OR `description` LIKE '%%%s%%' OR `id` IN (%s) OR `location` IN (%s)", text, text, idsSerialized, locationsSerialized), "") // TODO dedicated indexed tables for search
			if err != nil {
				return ctx.InternalError(), nil
			}
*/
