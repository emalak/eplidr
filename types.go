package eplidr

import (
	"fmt"
)

type Type interface {
	GetBasicType() BasicType
	Query() string
}
type BasicType int

type SizedType struct {
	NamedType BasicType
	Size      int
}

func (t *SizedType) GetBasicType() BasicType {
	return t.NamedType
}

func (t *SizedType) Query() string {
	switch t.NamedType {
	case BasicTypeUint64:
		return "BIGINT UNSIGNED"
	case BasicTypeInt64:
		return "BIGINT"
	case BasicTypeInt32:
		return "INTEGER"
	case BasicTypeUint32:
		return "INTEGER UNSIGNED"
	case BasicTypeFloat:
		return "DOUBLE"
	case BasicTypeBool:
		return "BOOL"
	case BasicTypeBinary:
		return fmt.Sprintf(`BINARY(%d)`, t.Size)
	case BasicTypeVarChar:
		return fmt.Sprintf(`VARCHAR(%d)`, t.Size)
	case BasicTypeVarByte:
		return fmt.Sprintf(`VARBINARY(%d)`, t.Size)
	}
	return ""
}

func GetSizedType(namedType BasicType, size int) Type {
	return &SizedType{
		NamedType: namedType,
		Size:      size,
	}
}
func GetType(namedType BasicType) Type {
	return &SizedType{
		NamedType: namedType,
		Size:      1,
	}
}

// TODO Discuss if verifying types (for example, on PUT check if UUID is valid, or on SELECT check if value is valid UUID) is needed
const (
	BasicTypeNone BasicType = iota
	BasicTypeInt64
	BasicTypeInt32
	BasicTypeUint32
	BasicTypeFloat
	BasicTypeVarChar
	BasicTypeVarByte
	BasicTypeBinary
	BasicTypeBool
	BasicTypeUint64
)

type sTypeNone struct{}

func (key sTypeNone) Query() string {
	return ""
}

func (key sTypeNone) GetBasicType() BasicType {
	return BasicTypeNone
}

var (
	TypeNone        = sTypeNone{}
	TypeUint64 Type = GetType(BasicTypeUint64)
	TypeInt64  Type = GetType(BasicTypeInt64)
	TypeInt32  Type = GetType(BasicTypeInt32)
	TypeUint32 Type = GetType(BasicTypeUint32)
	TypeFloat  Type = GetType(BasicTypeFloat)
	TypeBool   Type = GetType(BasicTypeBool)
	// TypeTimestamp = BIGINT UNSIGNED
	TypeTimestamp Type = TypeUint64
	// TypeUUID = binary(16)
	TypeUUID Type = GetSizedType(BasicTypeBinary, 16)
	// TypeSHA256 = varchar(44)
	TypeSHA256 Type = GetSizedType(BasicTypeVarChar, 44)
	// TypeIP = varchar(40)
	TypeIP Type = GetSizedType(BasicTypeVarChar, 40)
	// TypeEmail = varchar(256)
	TypeEmail Type = GetSizedType(BasicTypeVarChar, 256)
	// TypeUsername = varchar(64)
	TypeUsername Type = GetSizedType(BasicTypeVarChar, 32)
	// TypeBigInt = varbinary(32) ~max: 78 decimal digits
	TypeBigInt Type = GetSizedType(BasicTypeVarByte, 32)
	// TypeHugeInt = varbinary(128) ~max: 309 decimal digits
	TypeHugeInt Type = GetSizedType(BasicTypeVarByte, 32)
	// TypeHugeHugeInt = varbinary(512) ~max: 1234 decimal digits
	TypeHugeHugeInt Type = GetSizedType(BasicTypeVarByte, 32)
	// TypeURL = varchar(256)
	TypeURL Type = GetSizedType(BasicTypeVarChar, 256)
)
