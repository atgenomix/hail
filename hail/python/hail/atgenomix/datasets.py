import pyodbc 
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'tcp:atgxdbs.mysql.database.azure.com'
database = 'atgxdbs'
username = 'artemis'
password = 'ALX5u2HHMC'
cursor = None

def show_database_detail():
    print(server)
    print(database)
    print(username)

def get_cursor():
    if cursor is None:
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
    return cursor