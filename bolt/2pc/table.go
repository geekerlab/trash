package main

import (
//    "crypto/md5"
//    "encoding/hex"
    "fmt"
    "log"
//    "math/rand"
//    "os"
//    "strconv"
    "time"
    "sync"
    "errors"
    "github.com/boltdb/bolt"
)

type TableSchema struct {
    columns map[string]bool

    rwlock_  sync.Mutex
}


type RowMutation struct {

}

type Table struct {
    schema_ TableSchema
    path_  string

    rwlock_  sync.Mutex
    db_ *bolt.DB
}

func (s *TableSchema) AddColumn(column string) error {
    s.rwlock_.Lock()
    defer s.rwlock_.Unlock()
    if ok := s.columns[column]; ok == true {
        return errors.New("column exist")
    }
    s.columns[column] = true
    return nil
}

func (s *TableSchema) PrintColumn() {
    for c := range s.columns {
        fmt.Println(c)
    }
}

func (s *TableSchema) NumColumns() int {
    s.rwlock_.Lock()
    defer s.rwlock_.Unlock()
    var list = []bool{}
    for _, v := range s.columns {
        list = append(list, v)
    }
    return len(list)
}

func OpenTable(path string, table_schema *TableSchema) (*Table, error) {
    if table_schema.NumColumns() == 0 {
        return nil, errors.New("no column is defined")
    }
    var table = &Table{path_:path}
    db, err := bolt.Open(table.path_, 0600, &bolt.Options{Timeout: 1 * time.Second})
    if err != nil {
        log.Fatal(err)
        return nil, err
    }
    table.db_ = db
    return table, nil
}

func (t *Table) NewRowMutation(key string) (*RowMutation, error) {
    mutation := new(RowMutation)
    return mutation, nil
}

func (t *Table) UpdateMutation(key string, mutation *RowMutation) error {
    return nil
}

func main() {
    schema := new(TableSchema)
    schema.columns = make(map[string]bool)
    schema.AddColumn("c1")
    schema.AddColumn("c2")
    schema.AddColumn("c3")
    fmt.Println(schema.NumColumns())
}
