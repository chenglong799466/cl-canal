package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	DbConfig DBConfig `mapstructure:"db_config"`
}

// DBConfig ...
type DBConfig struct {
	User     string `mapstructure:"username"`
	PWD      string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
	Port     int32  `mapstructure:"port"`
	DataBase string `mapstructure:"database"`
}

var DbConfig *DBConfig

func init() {
	//导入配置文件
	viper.SetConfigType("yaml")
	viper.SetConfigFile(fmt.Sprintf("./config/config.yml"))
	//读取配置文件
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Errorf(" Init read config file err:%v", err.Error())
	}
	config := Config{}
	//将配置文件读到结构体中
	err = viper.Unmarshal(&config)
	if err != nil {
		fmt.Errorf(" Init unmarshal config file err:%v", err.Error())
	}

	DbConfig = &DBConfig{
		User:     config.DbConfig.User,
		PWD:      config.DbConfig.PWD,
		Host:     config.DbConfig.Host,
		Port:     config.DbConfig.Port,
		DataBase: config.DbConfig.DataBase,
	}
}
