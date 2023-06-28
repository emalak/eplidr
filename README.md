# eplidr
 
Database lib

## Setup connection
```
db, err = sql.Open("mysql", "{user}:{password}@tcp(localhost:41091)/{db}")
if err != nil {
 penic(err)
}
```
## Create table (global variable)
### Default table
```
Table1, err = eplidr.NewTable("tableName1",
		1, // Number of shards
		eplidr.TableFields{
			eplidr.DefaultTableField{Name: "id1", Type: eplidr.TypeUUID},
			eplidr.DefaultTableField{Name: "id2", Type: eplidr.TypeUUID},
			eplidr.DefaultTableField{Name: "metadata", Type: eplidr.GetSizedType(eplidr.BasicTypeVarChar, 500)},
			eplidr.DefaultTableField{Name: "time", Type: eplidr.TypeTimestamp},
			eplidr.ConstraintPrimaryKey("id1", "id2"),
		},
		db,
	)
	if err != nil {
	 panic(err)
	}
```
### SingleKeyTable (one primary key) 
```
Table2, err =eplidr.NewSingleKeyTable("tableName2",
		"id", // primary key name
		1,
		eplidr.TableFields{
			eplidr.DefaultTableField{
				Name:       "id",
				Type:       eplidr.TypeUUID,
				PrimaryKey: true,
			},
			eplidr.DefaultTableField{Name: "name", Type: eplidr.GetSizedType(eplidr.BasicTypeVarChar, 32)},
			eplidr.DefaultTableField{
				Name: "description",
				Type: eplidr.GetSizedType(eplidr.BasicTypeVarChar, 400),
			},
			eplidr.DefaultTableField{Name: "keywords", Type: eplidr.GetSizedType(eplidr.BasicTypeVarChar, 100)},
			eplidr.DefaultTableField{Name: "timestamp", Type: eplidr.TypeTimestamp},
			eplidr.DefaultTableField{Name: "latitude", Type: eplidr.TypeFloat},
			eplidr.DefaultTableField{Name: "longitude", Type: eplidr.TypeFloat},
			eplidr.DefaultTableField{Name: "link", Type: eplidr.GetSizedType(eplidr.BasicTypeVarChar, 300)},
		},
		db)
	if err != nil {
		panic(err)
	}
```

## Use table
### Default table
Choose 'shardKey', use eplidr.Keys for request
```
Table1.Remove(id1+id2, eplidr.Keys{{"id1", id1}, {"id2", id2}})
```
### SingleKeyTable (one primary key) 
```
Table2.Set(id, eplidr.Columns{
	{"description", value},
})
```
