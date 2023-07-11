package storage

import (
	"errors"
	"github.com/danmaina/HttpResponse/v2"
	"github.com/danmaina/logger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm"
	"net/http"
	"time"
)

const (
	InternalProcessingError = "an application error occurred while processing"
)

type Mysql struct {
	Host               string        `yaml:"host" json:"host"`
	Port               string        `yaml:"port" json:"port"`
	Username           string        `yaml:"username" json:"username"`
	Password           string        `yaml:"password" json:"password"`
	Database           string        `yaml:"database" json:"database"`
	TotalConnections   int           `yaml:"totalConnections" json:"totalConnections"`
	MaxIdleConnections int           `yaml:"maxIdleConnection" json:"maxIdleConnection"`
	MaxLifetime        time.Duration `yaml:"maxLifetime" json:"maxLifetime"`
}

func (mysql *Mysql) Connect() (*gorm.DB, error) {
	con, err := gorm.Open("mysql", mysql.Username+":"+mysql.Password+"@"+"tcp("+mysql.Host+":"+mysql.Port+")"+"/"+mysql.Database+"?parseTime=true")

	if err != nil {
		return nil, err
	}

	if mysql.TotalConnections == 0 {
		mysql.TotalConnections = 1
	} else if mysql.MaxIdleConnections == 0 {
		mysql.MaxIdleConnections = 1
	} else if mysql.MaxLifetime == 0 || mysql.MaxLifetime < 1 {
		mysql.MaxLifetime = time.Second * 60
	}

	con.DB().SetMaxOpenConns(mysql.TotalConnections)
	con.DB().SetMaxIdleConns(mysql.MaxIdleConnections)
	con.DB().SetConnMaxLifetime(mysql.MaxLifetime)

	return con, nil
}

func (mysql *Mysql) ConnectHttp(res http.ResponseWriter) *gorm.DB {
	logger.INFO("Retrieving MySQL Connection")

	db, errDb := mysql.Connect()

	if errDb != nil {
		handlers.ReturnResponse(http.StatusInternalServerError, errors.New(InternalProcessingError), nil, res)
		logger.ERR("Error while getting a connection to Mysql: ", errDb)
		return nil
	}

	return db
}
