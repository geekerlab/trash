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

const _FLAG_DELETE = "_FLAG_DELETE"

type TableSchema struct {
    columns map[string]bool

    rwlock_  sync.Mutex
}


type RowMutation struct {
    pairs map[string]string
    num int
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

func (s *TableSchema) IsColumn(column string) bool {
    s.rwlock_.Lock()
    defer s.rwlock_.Unlock()
    return s.columns[column]
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

func (m *RowMutation) Write(key, value string) {
    m.pairs[key] = value
}

func (m *RowMutation) Delete(key string) {
    m.pairs[key] = _FLAG_DELETE
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
    log.Printf("NewRowMutation")
    mutation := new(RowMutation)
    mutation.pairs = make(map[string]string)
    return mutation, nil
}

func (t *Table) UpdateMutation(key string, mutation *RowMutation) error {
    log.Printf("UpdateMutation")
    t.db_.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte("iqiyi"))
        if err != nil {
            return err
        }
        return nil
    })
    for c, v := range mutation.pairs {
        if err := t.db_.Update(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte("iqiyi"))
            err := b.Put([]byte(key + "/" + c), []byte(v))
            return err
        }); err == nil {

        }
    }
    return nil
}

func (t *Table) PrintRow(key string) (error) {
    log.Printf("PrintRow")
    if err := t.db_.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte("iqiyi"))
        for column := range t.schema_.columns {
            internal_key := key + "/" + column
            fmt.Printf("{%s: %s}", column, b.Get([]byte(internal_key)))
        }
        return nil
    }); err == nil {
        return err
    }
    return nil
}

func main() {
    schema := new(TableSchema)
    schema.columns = make(map[string]bool)
    schema.AddColumn("c1")
    schema.AddColumn("c2")
    schema.AddColumn("c3")
    fmt.Println(schema.NumColumns())

    table, err := OpenTable("./test.db", schema)
    if err != nil {
        log.Fatal(err)
    }
    key := "my_key"
    mutation, err := table.NewRowMutation(key)
    mutation.Write("k1", "v1")
    mutation.Write("k2", "v2")
    mutation.Delete("k1")
    if err != nil {
        log.Fatal(err)
    }

    if err := table.UpdateMutation(key, mutation); err != nil {
        log.Fatal(err)
    }

    table.PrintRow(key)
}
