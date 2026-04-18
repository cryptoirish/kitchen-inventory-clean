from flask import Flask, render_template, request, redirect, url_for
import psycopg
from psycopg.rows import dict_row
import os

app = Flask(__name__)

# Get database URL from environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return conn

# Initialize database
def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            category VARCHAR(50),
            stock DECIMAL(10,2),
            reorder_point DECIMAL(10,2),
            cost DECIMAL(10,2),
            unit VARCHAR(20)
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/inventory')
def inventory():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM items ORDER BY name')
    items = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('inventory.html', items=items)

@app.route('/add', methods=['GET', 'POST'])
def add():
    if request.method == 'POST':
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO items (name, category, stock, reorder_point, cost, unit)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            request.form['name'],
            request.form['category'],
            request.form['stock'],
            request.form['reorder'],
            request.form['cost'],
            request.form['unit']
        ))
        conn.commit()
        cur.close()
        conn.close()
        return redirect('/inventory')
    return render_template('add.html')

@app.route('/update/<int:id>', methods=['POST'])
def update(id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('UPDATE items SET stock = %s WHERE id = %s', 
                (request.form['stock'], id))
    conn.commit()
    cur.close()
    conn.close()
    return redirect('/inventory')

@app.route('/delete/<int:id>')
def delete(id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('DELETE FROM items WHERE id = %s', (id,))
    conn.commit()
    cur.close()
    conn.close()
    return redirect('/inventory')

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
