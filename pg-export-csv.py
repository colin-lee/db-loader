# -*- coding: utf-8 -*-
import os
import re
import sys

import psycopg2
from optparse import OptionParser

parser = OptionParser(usage="usage: %prog [options]", version="%prog 1.0", add_help_option=False,
                      description="export postgres data with where clause")
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
parser.add_option("-t", "--table", dest="tables", action="append", help="table to dump.")
parser.add_option("-c", "--command", dest="command", help="run only single command (SQL or internal) and exit")
parser.add_option("-D", "--dir", dest="dir", help="Directory for output")
parser.add_option("-q", "--quiet", action="store_false", dest="verbose")
parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
parser.add_option("", "--limit", dest="limit", help="number to dump.")
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


def quote(values):
    return ','.join([CONVERTOR(s) for s in values.split(',')])


def linenum(fn):
    num = 0
    with open(fn) as fd:
        for _ in fd:
            num += 1
    return num


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if not options.host or not options.database or not options.username:
        parser.print_help()
        sys.exit(1)

    pwd = options.password
    env_pwd = os.environ.get('PGPASSWORD')
    if options.no_pwd and env_pwd:
        pwd = env_pwd

    # 连接数据库
    conn = None
    try:
        conn = psycopg2.connect(database=options.database, user=options.username, password=pwd,
                                host=options.host, port=options.port)
    except Exception as e:
        print('connect db error, ', e)
        sys.exit(1)

    cursor = conn.cursor()

    tables = []
    if options.tables and len(options.tables) > 0:
        for i in options.tables:
            for j in i.split(','):
                tables.append(j)
    else:
        sql = "SELECT relname FROM pg_stat_user_tables WHERE schemaname='%s'" % options.schema
        if options.verbose:
            print(sql)
        tables = [r[0] for r in fetchall(cursor, sql)]

    workdir = '.'
    if options.dir:
        if not os.path.exists(options.dir):
            os.mkdir(options.dir)
        workdir = options.dir

    # 逐个表导出成文件
    for tbl in tables:
        query = 'SELECT * FROM ' + options.schema + '.' + tbl
        cmd = options.command if options.command else query
        limit = 'LIMIT ' + options.limit if options.limit else ""
        sql = "COPY(%s %s) TO STDOUT WITH CSV HEADER" % (cmd, limit)
        if options.verbose:
            print(sql)
        csv = workdir + '/' + tbl + ".csv"
        with open(csv, 'w+') as fp:
            try:
                cursor.copy_expert(sql, fp)
            except Exception as e:
                print('cannot copy ' + tbl, e)
                continue
            finally:
                conn.commit()
        if options.verbose:
            print('import %d lines in %s' % (linenum(csv) - 1, csv))

    conn.commit()
    cursor.close()
    conn.close()
