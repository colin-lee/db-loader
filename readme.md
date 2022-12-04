# postgresql的导入导出脚本
业务开发过程中，很多时候需要做数据的导入导出。

默认的pg-dump有如下的限制：
1. 不能限定schema
2. 不能指定导出的数据条数
3. 不能指定筛选的数据列
4. 不能筛选指定条件的数据 

默认的pg-restore有如下限制：
1. 无法应对主键冲突等情况
2. 处理自增id的冲突很麻烦

## 操作流程
1. 用pg-export-csv.py导出满足条件的csv数据文件
2. 用sed等工具针对导出的数据做预处理，比如替换某些信息等
3. 用pg-import-csv.py导入数据到目标数据库

## 依赖项
1. pip install psycopg2 configparser

### 帮助说明
```text
Usage: pg-export-csv.py [options]

export postgres data with where clause

Options:
  --version             show program version number and exit
  -h HOST, --host=HOST  Host address of the postgres database.
  -p PORT, --port=PORT  Port number at which the postgres instance is listening.
  -U USERNAME, --username=USERNAME
                        Username to connect to the postgres database.
  -u USERNAME, --user=USERNAME
                        Username to connect to the postgres database.
  -W PASSWORD, --password=PASSWORD
                        Password to connect to the postgres database.
  -w, --no-password     Never prompt for password. read env PGPASSWORD
  -d DATABASE, --database=DATABASE
                        database to dump.
  -n SCHEMA, --schema=SCHEMA
                        schema to dump.
  -t TABLE, --table=TABLE
                        table to dump.
  -c COMMAND, --command=COMMAND
                        run only single command (SQL or internal) and exit
  -D DIR, --dir=DIR     Directory for output
  -q, --quiet           
  -v, --verbose         
  --limit=LIMIT         number to dump.
  --help 
  
  
Usage: pg-export-csv.py [options]

export postgres data with where clause

Options:
  --version             show program's version number and exit
  -h HOST, --host=HOST  Host address of the postgres database.
  -p PORT, --port=PORT  Port number at which the postgres instance is
                        listening.
  -U USERNAME, --username=USERNAME
                        Username to connect to the postgres database.
  -u USERNAME, --user=USERNAME
                        Username to connect to the postgres database.
  -W PASSWORD, --password=PASSWORD
                        Password to connect to the postgres database.
  -w, --no-password     Never prompt for password. read env PGPASSWORD
  -d DATABASE, --database=DATABASE
                        database to dump.
  -n SCHEMA, --schema=SCHEMA
                        schema to dump.
  -t TABLE, --table=TABLE
                        table to dump.
  -c COMMAND, --command=COMMAND
                        run only single command (SQL or internal) and exit
  -D DIR, --dir=DIR     Directory for output
  -q, --quiet           
  -v, --verbose         
  --limit=LIMIT         number to dump.
  --help                  
```

## pg-export-csv.py
从postgresql数据库导出满足要求的数据到csv文件，为下一步的数据导入做准备

```bash
# 建议通过环境变量设置密码信息
export PGPASSWORD=xxxxyyyyyzzzz

# 导出一个库的所有表
python pg-export-csv.py -h 10.112.52.2 -u src_db_usr -w -d src_db_name -D dump -v

# 导出一个库某个schema下的所有表
python pg-export-csv.py -h 10.112.52.2 -u src_db_usr -w -d src_db_name -D dump -v -n public 

# 只导出某个表的数据
python pg-export-csv.py -h 10.112.52.2 -u src_db_usr -w -d src_db_name -D dump -v -t mt_data 

# 导出多张表的数据
python pg-export-csv.py -h 10.112.52.2 -u src_db_usr -w -d src_db_name -D dump -v -t mt_data -t object_data 

# 只导出满足条件的数据
python pg-export-csv.py -h 10.112.52.2 -u src_db_usr -w -d src_db_name -D dump -v -t mt_data -c "SELECT id,name FROM mt_data WHERE tenant_id='123'"

# 限定导出的数据条数
python pg-export-csv.py -h 10.112.52.2 -u src_db_usr -w -d src_db_name -D dump -v -t mt_data --limit 10 
```

## 处理csv文件
通常会有替换文本内容，修改列名等操作

## pg-import-csv.py
把处理好的csv文件，逐个导入到目标系统中。导入的过程是：
1. 根据目标表创建一个临时表（在事务结束后删除）
2. 用copy命令把数据导入临时表（没有索引，导入很快）
3. 用select into on conflicts do nothing导入目标表，避免主键冲突导致失败


```bash
# 建议通过环境变量设置密码信息
export PGPASSWORD=xxxxyyyyyzzzz

# 导入整理好的csv文件
python pg-import-csv.py -h 10.112.54.3 -u dst_db_usr -w -d dst_db_name -t mt_data -f dump/mt_data.csv -v 

```
