package goToolMSSqlHelper

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/Deansquirrel/goToolMSSql"
	"github.com/Deansquirrel/goToolMSSql2000"
	"strconv"
	"strings"
	"time"
)

//根据字符串配置，获取数据库连接配置
func GetDBConfigByStr(connStr string) (*goToolMSSql.MSSqlConfig, error) {
	connStr = strings.Trim(connStr, " ")
	strList := strings.Split(connStr, "|")
	if len(strList) != 5 {
		return nil, errors.New(fmt.Sprintf("db config num error,exp 5,act %d", len(strList)))
	}

	port, err := strconv.Atoi(strList[1])
	if err != nil {
		return nil, errors.New(fmt.Sprintf("db config port[%s] trans err: %s", strList[1], err.Error()))
	}

	return &goToolMSSql.MSSqlConfig{
		Server: strList[0],
		Port:   port,
		User:   strList[2],
		Pwd:    strList[3],
		DbName: strList[4],
	}, nil
}

//将普通数据库连接配置转换为Sql2000可用的配置
func ConvertDbConfigTo2000(dbConfig *goToolMSSql.MSSqlConfig) *goToolMSSql2000.MSSqlConfig {
	return &goToolMSSql2000.MSSqlConfig{
		Server: dbConfig.Server,
		Port:   dbConfig.Port,
		DbName: dbConfig.DbName,
		User:   dbConfig.User,
		Pwd:    dbConfig.Pwd,
	}
}

func GetRowsBySQL(dbConfig *goToolMSSql.MSSqlConfig, sql string, args ...interface{}) (*sql.Rows, error) {
	conn, err := goToolMSSql.GetConn(dbConfig)
	if err != nil {
		return nil, err
	}
	if args == nil {
		rows, err := conn.Query(sql)
		if err != nil {
			return nil, err
		}
		return rows, nil
	} else {
		rows, err := conn.Query(sql, args...)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
}

func SetRowsBySQL(dbConfig *goToolMSSql.MSSqlConfig, sql string, args ...interface{}) error {
	conn, err := goToolMSSql.GetConn(dbConfig)
	if err != nil {
		return err
	}
	if args == nil {
		_, err = conn.Exec(sql)
		if err != nil {
			return err
		}
		return nil
	} else {
		_, err := conn.Exec(sql, args...)
		if err != nil {
			return err
		}
		return nil
	}
}

func GetRowsBySQL2000(dbConfig *goToolMSSql2000.MSSqlConfig, sql string, args ...interface{}) (*sql.Rows, error) {
	conn, err := goToolMSSql2000.GetConn(dbConfig)
	if err != nil {
		return nil, err
	}
	if args == nil {
		rows, err := conn.Query(sql)
		if err != nil {
			return nil, err
		}
		return rows, nil
	} else {
		rows, err := conn.Query(sql, args...)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
}

func SetRowsBySQL2000(dbConfig *goToolMSSql2000.MSSqlConfig, sql string, args ...interface{}) error {
	conn, err := goToolMSSql2000.GetConn(dbConfig)
	if err != nil {
		return err
	}
	if args == nil {
		_, err = conn.Exec(sql)
		if err != nil {
			return err
		}
		return nil
	} else {
		_, err := conn.Exec(sql, args...)
		if err != nil {
			return err
		}
		return nil
	}
}

//返回默认时间
func GetDefaultOprTime() time.Time {
	return time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local)
}
