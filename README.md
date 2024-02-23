# cl-canal使用方法

以下是使用cl-canal的步骤：

1. 配置 MySQL 连接：在 `./config/config.yml` 文件中配置 MySQL 连接信息（如果文件不存在，则创建一个）。你可以先使用本地的 MySQL 进行测试。
   在配置文件中，填写正确的 MySQL 主机、端口、用户名、密码和数据库名称。
   `config.yml` 文件内容示例：
   ```yaml
   # config.yml
   
   mysql:
     host: localhost
     port: 3306
     username: your_username
     password: your_password
     database: your_database
   ```

2. 运行方式：

   修改`canal/const.go`的TableArray指定监听的表名

   直接运行main方法，此时TableArray的表的增删改操作都会日志记录。
