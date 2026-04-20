from flask import Flask, render_template, request, redirect, url_for
import psycopg
from psycopg.rows import dict_row
import os
import traceback
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db():
    try:
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

def init_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT,
                stock NUMERIC DEFAULT 0,
                reorder_point NUMERIC DEFAULT 0,
                cost NUMERIC DEFAULT 0,
                unit TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        cur.close()
        conn.close()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Database init error: {e}")
        traceback.print_exc()

@app.route('/')
def home():
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Get summary stats
        cur.execute('SELECT COUNT(*) as total_items FROM items')
        total_items = cur.fetchone()['total_items']
        
        cur.execute('SELECT COUNT(*) as low_stock FROM items WHERE stock <= reorder_point')
        low_stock_count = cur.fetchone()['low_stock']
        
        cur.execute('SELECT SUM(stock * cost) as total_value FROM items')
        result = cur.fetchone()
        total_value = result['total_value'] if result['total_value'] else 0
        
        cur.close()
        conn.close()
        
        return render_template('home.html', 
                             total_items=total_items,
                             low_stock_count=low_stock_count,
                             total_value=float(total_value))
    except Exception as e:
        print(f"Home page error: {e}")
        return render_template('home.html', 
                             total_items=0,
                             low_stock_count=0,
                             total_value=0)

@app.route('/inventory')
def inventory():
    try:
        search = request.args.get('search', '')
        category_filter = request.args.get('category', '')
        
        conn = get_db()
        cur = conn.cursor()
        
        query = 'SELECT * FROM items WHERE 1=1'
        params = []
        
        if search:
            query += ' AND LOWER(name) LIKE LOWER(%s)'
            params.append(f'%{search}%')
        
        if category_filter:
            query += ' AND category = %s'
            params.append(category_filter)
        
        query += ' ORDER BY name'
        
        cur.execute(query, params)
        items = cur.fetchall()
        
        # Get all categories for filter
        cur.execute('SELECT DISTINCT category FROM items WHERE category IS NOT NULL ORDER BY category')
        categories = [row['category'] for row in cur.fetchall()]
        
        # Calculate total value
        cur.execute('SELECT SUM(stock * cost) as total FROM items')
        result = cur.fetchone()
        total_value = float(result['total']) if result['total'] else 0
        
        cur.close()
        conn.close()
        
        return render_template('inventory.html', 
                             items=items, 
                             categories=categories,
                             search=search,
                             category_filter=category_filter,
                             total_value=total_value)
    except Exception as e:
        print(f"Inventory error: {e}")
        traceback.print_exc()
        return f"Error loading inventory: {str(e)}", 500

@app.route('/add', methods=['GET', 'POST'])
def add():
    if request.method == 'POST':
        try:
            print(f"Form data: {request.form}")
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO items (name, category, stock, reorder_point, cost, unit, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (
                request.form['name'],
                request.form['category'],
                request.form['stock'],
                request.form['reorder'],
                request.form['cost'],
                request.form['unit'],
                datetime.now()
            ))
            conn.commit()
            cur.close()
            conn.close()
            print("Item added successfully")
            return redirect('/inventory')
        except Exception as e:
            print(f"Add item error: {e}")
            traceback.print_exc()
            return f"Error adding item: {str(e)}", 500
    return render_template('add.html')

@app.route('/update/<int:id>', methods=['POST'])
def update(id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('UPDATE items SET stock = %s, updated_at = %s WHERE id = %s', 
                    (request.form['stock'], datetime.now(), id))
        conn.commit()
        cur.close()
        conn.close()
        return redirect('/inventory')
    except Exception as e:
        print(f"Update error: {e}")
        traceback.print_exc()
        return f"Error updating item: {str(e)}", 500

@app.route('/delete/<int:id>')
def delete(id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM items WHERE id = %s', (id,))
        conn.commit()
        cur.close()
        conn.close()
        return redirect('/inventory')
    except Exception as e:
        print(f"Delete error: {e}")
        traceback.print_exc()
        return f"Error deleting item: {str(e)}", 500

@app.route('/alerts')
def alerts():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT * FROM items 
            WHERE stock <= reorder_point 
            ORDER BY stock ASC
        ''')
        low_stock = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('alerts.html', items=low_stock)
    except Exception as e:
        print(f"Alerts error: {e}")
        return f"Error loading alerts: {str(e)}", 500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
