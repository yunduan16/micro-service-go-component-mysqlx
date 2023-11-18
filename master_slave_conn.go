package mysqlx

import (
    "database/sql"
    "fmt"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "log"
    "math/rand"
    "time"
)

var dbs []*sql.DB

type Conns struct {
    Masters map[string]*gorm.DB
    Slaves  map[string][]*gorm.DB
}

type Conf struct {
    Dsns            []string
    MaxOpenConns    int
    MaxIdleConns    int
    ConnMaxLifetime time.Duration
}

// ConnMysql 连接mysql
func ConnMysql(conf map[string]map[string]*Conf) *Conns {
    var databases = make([]string, 0, len(conf))
    for database := range conf {
        databases = append(databases, database)
    }
    conns := &Conns{}
    conns.Masters = make(map[string]*gorm.DB, 0)
    conns.Slaves = make(map[string][]*gorm.DB, 0)
    for _, database := range databases {
        masterConf := conf[database]["master"]
        slaveConf := conf[database]["slave"]

        conns.connectSlave(slaveConf, database)
        conns.connectMaster(masterConf, database)
    }

    return conns
}

func (c *Conns) GetReader(database string) *gorm.DB {
    connSet := c.Slaves[database]
    return connSet[rand.Intn(len(connSet))]
}

func (c *Conns) GetWriter(database string) *gorm.DB {
    return c.Masters[database]
}

// 数据库从连接
func (c *Conns) connectSlave(conf *Conf, database string) {
    var slaves []*gorm.DB
    c.Slaves[database] = make([]*gorm.DB, 0)
    for _, dsn := range conf.Dsns {
        slave, openErr := gorm.Open(mysql.Open(dsn), &gorm.Config{})
        if openErr != nil {
            panic("open mysql err" + openErr.Error())
        }

        sqlDB, _ := slave.DB()
        sqlDB.SetMaxOpenConns(conf.MaxOpenConns)
        sqlDB.SetMaxIdleConns(conf.MaxIdleConns)
        sqlDB.SetConnMaxLifetime(conf.ConnMaxLifetime)
        dbs = append(dbs, sqlDB)
        slaves = append(slaves, slave)
    }

    c.Slaves[database] = slaves
    if conns := len(slaves); conns == 0 {
        log.Fatal(database + "slave connection count is 0")
    }
}

// 数据库主连接
func (c *Conns) connectMaster(conf *Conf, database string) {
    master, err := gorm.Open(mysql.Open(conf.Dsns[0]), &gorm.Config{})
    if err != nil {
        panic(fmt.Sprintf("The mysql %s is open err:%s", database, err.Error()))
    }

    sqlDB, _ := master.DB()
    sqlDB.SetMaxOpenConns(conf.MaxOpenConns)
    sqlDB.SetMaxIdleConns(conf.MaxIdleConns)
    sqlDB.SetConnMaxLifetime(conf.ConnMaxLifetime)
    dbs = append(dbs, sqlDB)
    c.Masters[database] = master
}

func (c *Conns) Close() {
    if len(dbs) == 0 {
        return
    }
    for _, conn := range dbs {
        _ = conn.Close()
    }
}
