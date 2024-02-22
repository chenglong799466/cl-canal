package canal

import (
	"cl-canal/config"
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
)

func BinLogListener() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", config.DbConfig.Host, config.DbConfig.Port)
	cfg.User = config.DbConfig.User
	cfg.Password = config.DbConfig.PWD
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""
	// NewCanal
	newCanal, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Errorf(fmt.Sprintf("getDefaultCanal ERROR: %v", err))
		return err
	}
	// SHOW MASTER STATUS 查询当前listener的binlog起始位置
	coords, err := newCanal.GetMasterPos()
	if err != nil {
		fmt.Errorf(fmt.Sprintf("GetMasterPos ERROR: %v", err))
		return err
	}
	// handler
	handler := &binlogHandler{
		SchemaName:    config.DbConfig.DataBase,
		TableNameList: TableArray,
	}

	newCanal.SetEventHandler(handler)
	err = newCanal.RunFrom(coords)
	if err != nil {
		fmt.Errorf(fmt.Sprintf("RunFrom ERROR: %v", err))
		return err
	}
	return nil
}
