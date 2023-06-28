package eplidr

import (
	"fmt"
)

type FieldName string
type TableFields []TableField
type TableField interface {
	GetName() string
	QueryInit(table string) string
	QueryAfter(table string) string
	QueryAlter(table string) string
	GetType() Type
}

type DefaultTableField struct {
	Name         string
	Type         Type
	Index        bool
	PrimaryKey   bool
	DefaultValue interface{}
	Nullable     bool
}

func (f DefaultTableField) GetName() string {
	return f.Name
}

func (f DefaultTableField) GetType() Type {
	return f.Type
}

func (f DefaultTableField) DefaultValueQuery() string {
	if f.DefaultValue != nil {
		return fmt.Sprintf(" default %v", f.DefaultValue)
	} else {
		return ""
	}
}

func (f DefaultTableField) PrimaryKeyQuery() string {
	if f.PrimaryKey {
		return " primary key"
	} else {
		return ""
	}
}

func (f DefaultTableField) QueryInit(table string) string {
	if f.Nullable {
		return fmt.Sprintf("`%s` %s%s%s", f.Name, f.Type.Query(), f.PrimaryKeyQuery(), f.DefaultValueQuery())
	}
	return fmt.Sprintf("`%s` %s%s%s %s", f.Name, f.Type.Query(), f.PrimaryKeyQuery(), f.DefaultValueQuery(), "NOT NULL")
}
func (f DefaultTableField) QueryAfter(table string) string {
	if f.Index {
		return fmt.Sprintf("CREATE INDEX I%s ON %s (%s)", f.Name, table, f.Name)
	} else {
		return ""
	}
}
func (f DefaultTableField) QueryAlter(table string) string {
	return "" // TODO
}

type SConstraintPrimaryKey struct {
	Keys []string
}

func (f SConstraintPrimaryKey) GetName() string {
	return "PK_" + PlainListNoSep(f.Keys)
}

func (f SConstraintPrimaryKey) GetType() Type {
	return TypeNone
}

func (f SConstraintPrimaryKey) QueryInit(table string) string {
	return fmt.Sprintf("CONSTRAINT %s PRIMARY KEY (%s)", f.GetName(), PlainList(f.Keys))
}
func (f SConstraintPrimaryKey) QueryAfter(table string) string {
	return ""
}
func (f SConstraintPrimaryKey) QueryAlter(table string) string {
	return "" // TODO
}

func ConstraintPrimaryKey(keys ...string) SConstraintPrimaryKey {
	return SConstraintPrimaryKey{Keys: keys}
}
