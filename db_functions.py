import psycopg2

def connectDB():
    conn = psycopg2.connect('dbname=xchem user=uzw12877 host=localhost')
    c = conn.cursor()

    return conn, c

def table_exists(c, tablename):
    c.execute('''select exists(select * from information_schema.tables where table_name=%s);''', (tablename,))
    exists = c.fetchone()[0]
    return exists