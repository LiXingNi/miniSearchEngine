import sqlite3

def func():
    conn = sqlite3.connect("zhidao_org.db")

    c = conn.cursor()

    c.execute('SELECT id, name, url FROM infoLib')

    s = c.fetchall()

    conns = sqlite3.connect('orgDb.db')
    cs = conns.cursor()
    cs.execute('''CREATE TABLE IF NOT EXISTS orgTable
        (id INTEGER PRIMARY KEY ,title text,url text)''')

    for obj in s:
        cs.execute('INSERT INTO orgTable VALUES(?,?,?)',(obj[0],obj[1],obj[2]))
    conns.commit()
    conns.close()
    
