import os
import pymysql.cursors

def execute_sql(sql):
    server = os.environ["RDB_HOST"]
    user = os.environ["RDB_USER"]
    passwd = os.environ["RDB_PASSWORD"]
    database = os.environ["RDB_DATABASE"]
    port = os.environ["RDB_PORT"]
    connection = pymysql.connect(
        host=server,
        user=user,
        password=passwd,
        db=database,
        port=int(port),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchAll()