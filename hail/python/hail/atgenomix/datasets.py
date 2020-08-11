import os
import pymysql.cursors

def execute_sql(sql):
    server = "{}.mysql.database.azure.com".format(os.environ["RDB_SERVER"])
    user = "{}@{}".format(os.environ["RDB_USER"],os.environ["RDB_SERVER"])
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