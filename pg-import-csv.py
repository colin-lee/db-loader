# -*- coding: utf-8 -*-
import os
import re
import sys
import psycopg2
from optparse import OptionParser

parser = OptionParser(usage="usage: %prog [options]", version="%prog 1.0", add_help_option=False,
                      description="export import data from csv file, ignore duplicated keys")
parser.add_option("-h", "--host", dest="host", help="Host address of the postgres database.")
parser.add_option("-p", "--port", dest="port", default=5432, type="int",
                  help="Port number at which the postgres instance is listening.")
parser.add_option("-U", "--username", dest="username", help="Username to connect to the postgres database.")
parser.add_option("-u", "--user", dest="username", help="Username to connect to the postgres database.")
parser.add_option("-W", "--password", dest="password", help="Password to connect to the postgres database.")
parser.add_option("-w", "--no-password", action="store_true", dest="no_pwd",
                  help="Never prompt for password. read env PGPASSWORD")
parser.add_option("-d", "--database", dest="database", help="database to dump.")
parser.add_option("-n", "--schema", dest="schema", default="public", help="schema to dump.")
parser.add_option("-t", "--table", dest="table", help="table to dump.")
parser.add_option("-f", "--file", dest="file", help="run only single command (SQL or internal) and exit")
parser.add_option("-q", "--quiet", action="store_false", dest="verbose")
parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
parser.add_option("", "--help", action="help", dest="Show this help and exit.")

# 保留字或者名称包含大写字母需要用双引号括起来
UPPER = re.compile(r'[A-Z]')
WORDS = 'key,limit,offset,remark,comment,type,case,range,add,alter,case,when,time,rank,primary,index,create,drop'
KEYS = WORDS.split(',')
CONVERTOR = lambda s: '"' + s + '"' if s[0] != '"' and (UPPER.match(s) or s in KEYS) else s


def fetchall(stmt, q):
    stmt.execute(q)
    try:
        return stmt.fetchall()
    except Exception as ex:
        print("sql: " + q, ex)
    return []


def fetchone(stmt, q):
    stmt.execute(q)
    try:
        return stmt.fetchone()
    except Exception as ex:
        print("sql: " + q, ex)


def quote(items):
    return ','.join([CONVERTOR(s) for s in items.split(',')])


def linenum(fn):
    num = 0
    with open(fn) as fd:
        for _ in fd:
            num += 1
    return num


def firstline(name):
    with open(name) as fp:
        head = fp.readline().strip()
        formal = []
        for s in head.split(','):
            strip = s.strip()
            if UPPER.search(s) or strip in KEYS:
                formal.append('"' + strip + '"')
            else:
                formal.append(strip)
        return ','.join(formal)


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if not options.host or not options.database or not options.username:
        parser.print_help()
        sys.exit(1)

    pwd = options.password
    env_pwd = os.environ.get('PGPASSWORD')
    if options.no_pwd and env_pwd:
        pwd = env_pwd
    os.environ['PGOPTIONS'] = '-c statement_timeout=0'

    # 连接数据库，pgbouncer可能不支持对应参数，必须直连数据库
    conn = None
    try:
        conn = psycopg2.connect(database=options.database, user=options.username, password=pwd,
                                host=options.host, port=options.port)
    except Exception as e:
        print('connect db error, ', e)
        sys.exit(1)

    cursor = conn.cursor()

    tbl = options.table
    csv = options.file

    # 创建临时表做导入，避免主键冲突
    sql = "CREATE TEMP TABLE t_%s_temp ON COMMIT DROP AS SELECT * FROM %s LIMIT 0" % (tbl, tbl)
    # sql = 'SELECT * INTO TEMP TABLE %s FROM %s LIMIT 0' % (tbl_tmp, tbl)
    if options.verbose:
        print(sql)
    cursor.execute(sql)

    # 检查临时表字段
    sql = 'SELECT * FROM t_%s_temp LIMIT 0' % tbl
    if options.verbose:
        print(sql)
    cursor.execute(sql)
    names = [desc[0] for desc in cursor.description]
    print(names)

    # 导入csv到临时表
    head = firstline(csv)
    sql = "COPY t_%s_temp(%s) FROM STDIN WITH CSV HEADER" % (tbl, head)
    if options.verbose:
        print(sql)
    csf = open(csv)
    try:
        cursor.copy_expert(sql, csf)
    except Exception as e:
        print('cannot copy into t_' + tbl + "_temp from " + csv, e)
        sys.exit(1)
    finally:
        csf.close()

    # 统计临时表数据
    sql = 'SELECT count(*) FROM t_%s_temp' % tbl
    if options.verbose:
        print(sql)
    count = fetchone(cursor, sql)[0]
    print('IMPORT %d items from %s' % (count, csv))

    # 从临时表复制到正式表，忽略主键冲突
    sql = 'INSERT INTO %s(%s) SELECT %s FROM t_%s_temp ON CONFLICT DO NOTHING' % (tbl, head, head, tbl)
    if options.verbose:
        print(sql)
    cursor.execute(sql)

    # 提交事务并删除临时表
    conn.commit()
    cursor.close()
    conn.close()
