package mysqlx

import (
    "fmt"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

type ConnsSingle struct {
    Db map[string]*gorm.DB
}

// ConnMysqlSingle 连接mysql，不进行主从分离
func ConnMysqlSingle(conf map[string]*Conf) *ConnsSingle {
    lenConf := len(conf)
    var databases = make([]string, 0, lenConf)
    for database := range conf {
        databases = append(databases, database)
    }
    conns := &ConnsSingle{}
    conns.Db = make(map[string]*gorm.DB, lenConf)
    for _, database := range databases {
        conns.connectSingle(conf[database], database)
    }

    return conns
}

func (c *ConnsSingle) GetDb(database string) *gorm.DB {
    return c.Db[database]
}

// 数据库主连接
func (c *ConnsSingle) connectSingle(conf *Conf, database string) {
    master, err := gorm.Open(mysql.Open(conf.Dsns[0]), &gorm.Config{})
    if err != nil {
        panic(fmt.Sprintf("The mysql %s is open err:%s", database, err.Error()))
    }

    sqlDB, _ := master.DB()
    sqlDB.SetMaxOpenConns(conf.MaxOpenConns)
    sqlDB.SetMaxIdleConns(conf.MaxIdleConns)
    sqlDB.SetConnMaxLifetime(conf.ConnMaxLifetime)
    dbs = append(dbs, sqlDB)
    c.Db[database] = master
}

func (c *ConnsSingle) Close() {
    if len(dbs) == 0 {
        return
    }
    for _, conn := range dbs {
        _ = conn.Close()
    }
}
