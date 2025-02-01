package storage

import (
	"errors"
	"github.com/danmaina/HttpResponse/v2"
	"github.com/danmaina/logger"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"net/http"
)

const (
	InternalProcessingError = "an application error occurred while processing"
)

type Mysql struct {
	Host     string `yaml:"host" json:"host"`
	Port     string `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
}

func (mysqlObj *Mysql) Connect() (*gorm.DB, error) {
	dsn := mysqlObj.Username + ":" + mysqlObj.Password + "@tcp(" + mysqlObj.Host + ":" + mysqlObj.Port + ")/" + mysqlObj.Database + "?parseTime=true"
	con, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})

	if err != nil {
		return nil, err
	}

	return con, nil
}

func (mysqlObj *Mysql) ConnectHttp(res http.ResponseWriter) *gorm.DB {
	logger.INFO("Retrieving MySQL Connection")

	db, errDb := mysqlObj.Connect()

	if errDb != nil {
		handlers.ReturnResponse(http.StatusInternalServerError, errors.New(InternalProcessingError), nil, res)
		logger.ERR("Error while getting a connection to Mysql: ", errDb)
		return nil
	}

	return db
}
