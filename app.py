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
        
        # Create items table
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
        
        # Create recipes table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS recipes (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT,
                selling_price NUMERIC DEFAULT 0,
                portion_size TEXT,
                servings INTEGER DEFAULT 1,
                instructions TEXT,
                notes TEXT,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create recipe_ingredients table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS recipe_ingredients (
                id BIGSERIAL PRIMARY KEY,
                recipe_id BIGINT REFERENCES recipes(id) ON DELETE CASCADE,
                inventory_item_id BIGINT REFERENCES items(id) ON DELETE CASCADE,
                quantity NUMERIC NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        
        # Get inventory stats
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
        
        cur.execute('SELECT DISTINCT category FROM items WHERE category IS NOT NULL ORDER BY category')
        categories = [row['category'] for row in cur.fetchall()]
        
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

# RECIPE ROUTES

@app.route('/recipes')
def recipes():
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Get all recipes with calculated costs
        cur.execute('''
            SELECT 
                r.*,
                COALESCE(SUM(ri.quantity * i.cost), 0) as total_cost,
                COUNT(ri.id) as ingredient_count
            FROM recipes r
            LEFT JOIN recipe_ingredients ri ON r.id = ri.recipe_id
            LEFT JOIN items i ON ri.inventory_item_id = i.id
            GROUP BY r.id
            ORDER BY r.name
        ''')
        recipes_list = cur.fetchall()
        
        # Calculate profitability metrics
        for recipe in recipes_list:
            if recipe['selling_price'] and recipe['selling_price'] > 0:
                recipe['food_cost_percent'] = (recipe['total_cost'] / recipe['selling_price']) * 100
                recipe['gross_profit'] = recipe['selling_price'] - recipe['total_cost']
            else:
                recipe['food_cost_percent'] = 0
                recipe['gross_profit'] = 0
        
        cur.close()
        conn.close()
        
        return render_template('recipes.html', recipes=recipes_list)
    except Exception as e:
        print(f"Recipes error: {e}")
        traceback.print_exc()
        return f"Error loading recipes: {str(e)}", 500

@app.route('/recipes/add', methods=['GET', 'POST'])
def add_recipe():
    if request.method == 'POST':
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO recipes (name, category, selling_price, portion_size, servings, instructions, notes, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                request.form['name'],
                request.form['category'],
                request.form['selling_price'],
                request.form['portion_size'],
                request.form['servings'],
                request.form.get('instructions', ''),
                request.form.get('notes', ''),
                datetime.now()
            ))
            recipe_id = cur.fetchone()['id']
            conn.commit()
            cur.close()
            conn.close()
            return redirect(f'/recipes/{recipe_id}')
        except Exception as e:
            print(f"Add recipe error: {e}")
            traceback.print_exc()
            return f"Error adding recipe: {str(e)}", 500
    
    return render_template('add_recipe.html')

@app.route('/recipes/<int:id>')
def recipe_detail(id):
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Get recipe details
        cur.execute('SELECT * FROM recipes WHERE id = %s', (id,))
        recipe = cur.fetchone()
        
        if not recipe:
            return "Recipe not found", 404
        
        # Get recipe ingredients with item details
        cur.execute('''
            SELECT 
                ri.*,
                i.name as item_name,
                i.unit,
                i.cost as unit_cost,
                (ri.quantity * i.cost) as ingredient_cost
            FROM recipe_ingredients ri
            JOIN items i ON ri.inventory_item_id = i.id
            WHERE ri.recipe_id = %s
            ORDER BY i.name
        ''', (id,))
        ingredients = cur.fetchall()
        
        # Calculate totals
        total_cost = sum(ing['ingredient_cost'] for ing in ingredients)
        if recipe['selling_price'] and recipe['selling_price'] > 0:
            food_cost_percent = (total_cost / recipe['selling_price']) * 100
            gross_profit = recipe['selling_price'] - total_cost
        else:
            food_cost_percent = 0
            gross_profit = 0
        
        # Get all inventory items for adding ingredients
        cur.execute('SELECT * FROM items ORDER BY name')
        all_items = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return render_template('recipe_detail.html',
                             recipe=recipe,
                             ingredients=ingredients,
                             total_cost=total_cost,
                             food_cost_percent=food_cost_percent,
                             gross_profit=gross_profit,
                             all_items=all_items)
    except Exception as e:
        print(f"Recipe detail error: {e}")
        traceback.print_exc()
        return f"Error loading recipe: {str(e)}", 500

@app.route('/recipes/<int:recipe_id>/add-ingredient', methods=['POST'])
def add_ingredient_to_recipe(recipe_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO recipe_ingredients (recipe_id, inventory_item_id, quantity, notes)
            VALUES (%s, %s, %s, %s)
        ''', (
            recipe_id,
            request.form['item_id'],
            request.form['quantity'],
            request.form.get('notes', '')
        ))
        conn.commit()
        cur.close()
        conn.close()
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Add ingredient error: {e}")
        return f"Error: {str(e)}", 500

@app.route('/recipes/<int:recipe_id>/remove-ingredient/<int:ingredient_id>')
def remove_ingredient(recipe_id, ingredient_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM recipe_ingredients WHERE id = %s', (ingredient_id,))
        conn.commit()
        cur.close()
        conn.close()
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Remove ingredient error: {e}")
        return f"Error: {str(e)}", 500

@app.route('/recipes/delete/<int:id>')
def delete_recipe(id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM recipes WHERE id = %s', (id,))
        conn.commit()
        cur.close()
        conn.close()
        return redirect('/recipes')
    except Exception as e:
        print(f"Delete recipe error: {e}")
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
