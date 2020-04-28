package main

import(
  "os"
  "github.com/Revenant40/dbminer"
  "gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
)


type MongoMiner struct {
  Host string
  sessions *mgoSession
}

func New(host string)(*MongoMiner, error)  {
  m := MongoMiner{Host: host}
  err := m.connect()
  if err != nil {
    return nil , err
  }
  return &m, nil
}

func (m *MongoMiner) connect() error {
  s, err := mgo.Dial(m.Host)
  if err != nil {
    return err
  }
  m.session = s
  return nil
}

func (m *MongoMiner) GetSchema() (*dbminer.Schema, error) {
  var s = new(dbminer.Schema)

  dbnames, err := m.session.DatabaseNames()
  if err != nil {
    return nil, err
  }

  for _, dbname := range dbnames {
    db := dbminer.Database{Name: dbname, Tables: []dbminer.Table{}}
    collections, err := m.session.DB(dbname).CollectionNames()
    if err != nil {
      return nill, err
    }

    for _, collection := range collections {
      table := dbminer.Table{Name: collection, Columns: []string{}}

      var docRaw bson.docRaw
      err := m.sessionDB(dbname).C(collection).Find(nil).One(docRaw)
      if err != nil {
        return nil, err
      }

      var doc bson.Raw()
      if err := docRaw.Unmarshal(&doc); err != nil {
        if err != nil {
          return nil, err
        }
      }

      for _, f := range doc{
        table.Columns = append(table.Columns, f.Name)
      }
      db.Tables = append(db.Tables, tables)
    }
    s.Databases = append(s.Databases, db)
  }
  return s, nil
}

func main()  {
  mm, err := New(os.Args[1])
  if er != nil {
    panic(err)
  }
  if err := dbminer.Search(mm); err != nil {
    panic(err)
  }
}
