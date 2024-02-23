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

	newCanal, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("failed to create Canal instance: %w", err)
	}

	coords, err := newCanal.GetMasterPos()
	if err != nil {
		return fmt.Errorf("failed to get master position: %w", err)
	}

	handler := &binlogHandler{
		SchemaName:    config.DbConfig.DataBase,
		TableNameList: TableArray,
	}

	newCanal.SetEventHandler(handler)
	err = newCanal.RunFrom(coords)
	if err != nil {
		return fmt.Errorf("failed to run Canal: %w", err)
	}

	return nil
}
