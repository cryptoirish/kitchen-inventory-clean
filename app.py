from flask import Flask, render_template, request, redirect, url_for, flash, session
import psycopg
from psycopg.rows import dict_row
import os
import traceback
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import csv
from io import StringIO

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db():
    try:
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

# Authentication decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Get current user's organization ID
def get_current_org_id():
    if 'organization_id' in session:
        return session['organization_id']
    return None

def init_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Create organizations table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS organizations (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                address TEXT,
                subscription_tier TEXT DEFAULT 'starter',
                subscription_status TEXT DEFAULT 'trial',
                trial_ends_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT true
            )
        ''')
        
        # Create users table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                organization_id BIGINT REFERENCES organizations(id) ON DELETE CASCADE,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                role TEXT DEFAULT 'staff',
                is_active BOOLEAN DEFAULT true,
                last_login TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create items table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS items (
                id BIGSERIAL PRIMARY KEY,
                organization_id BIGINT REFERENCES organizations(id) ON DELETE CASCADE,
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
                organization_id BIGINT REFERENCES organizations(id) ON DELETE CASCADE,
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

# AUTH ROUTES

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        try:
            email = request.form['email']
            password = request.form['password']
            
            conn = get_db()
            cur = conn.cursor()
            cur.execute('SELECT * FROM users WHERE email = %s AND is_active = true', (email,))
            user = cur.fetchone()
            
            if user and check_password_hash(user['password_hash'], password):
                # Set session
                session['user_id'] = user['id']
                session['organization_id'] = user['organization_id']
                session['user_name'] = f"{user['first_name']} {user['last_name']}"
                session['user_role'] = user['role']
                
                # Update last login
                cur.execute('UPDATE users SET last_login = %s WHERE id = %s', (datetime.now(), user['id']))
                conn.commit()
                
                cur.close()
                conn.close()
                
                flash(f"Welcome back, {user['first_name']}!", 'success')
                return redirect(url_for('home'))
            else:
                flash('Invalid email or password', 'danger')
                cur.close()
                conn.close()
        except Exception as e:
            print(f"Login error: {e}")
            flash('Login failed. Please try again.', 'danger')
    
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        try:
            conn = get_db()
            cur = conn.cursor()
            
            # Create organization
            cur.execute('''
                INSERT INTO organizations (name, email, subscription_tier, subscription_status)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            ''', (
                request.form['restaurant_name'],
                request.form['email'],
                'starter',
                'trial'
            ))
            org_id = cur.fetchone()['id']
            
            # Create user
            password_hash = generate_password_hash(request.form['password'])
            cur.execute('''
                INSERT INTO users (organization_id, email, password_hash, first_name, last_name, role)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                org_id,
                request.form['email'],
                password_hash,
                request.form['first_name'],
                request.form['last_name'],
                'owner'
            ))
            user_id = cur.fetchone()['id']
            
            conn.commit()
            cur.close()
            conn.close()
            
            flash('Account created successfully! Please log in.', 'success')
            return redirect(url_for('login'))
            
        except Exception as e:
            print(f"Registration error: {e}")
            flash('Registration failed. Email may already be in use.', 'danger')
    
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

# MAIN ROUTES

@app.route('/')
def home():
    # If not logged in, show public landing page
    if 'user_id' not in session:
        return render_template('landing.html')
    
    # If logged in, show dashboard
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('SELECT COUNT(*) as total_items FROM items WHERE organization_id = %s', (org_id,))
        total_items = cur.fetchone()['total_items']
        
        cur.execute('SELECT COUNT(*) as low_stock FROM items WHERE organization_id = %s AND stock <= reorder_point', (org_id,))
        low_stock_count = cur.fetchone()['low_stock']
        
        cur.execute('SELECT SUM(stock * cost) as total_value FROM items WHERE organization_id = %s', (org_id,))
        result = cur.fetchone()
        total_value = result['total_value'] if result['total_value'] else 0
        
        # Get recipe count
        cur.execute('SELECT COUNT(*) as total_recipes FROM recipes WHERE organization_id = %s', (org_id,))
        total_recipes = cur.fetchone()['total_recipes']
        
        cur.close()
        conn.close()
        
        return render_template('dashboard.html', 
                             total_items=total_items,
                             low_stock_count=low_stock_count,
                             total_value=float(total_value),
                             total_recipes=total_recipes)
    except Exception as e:
        print(f"Dashboard error: {e}")
        return render_template('dashboard.html', 
                             total_items=0,
                             low_stock_count=0,
                             total_value=0,
                             total_recipes=0)

@app.route('/inventory')
@login_required
def inventory():
    try:
        org_id = get_current_org_id()
        search = request.args.get('search', '')
        category_filter = request.args.get('category', '')
        
        conn = get_db()
        cur = conn.cursor()
        
        query = 'SELECT * FROM items WHERE organization_id = %s'
        params = [org_id]
        
        if search:
            query += ' AND LOWER(name) LIKE LOWER(%s)'
            params.append(f'%{search}%')
        
        if category_filter:
            query += ' AND category = %s'
            params.append(category_filter)
        
        query += ' ORDER BY name'
        
        cur.execute(query, params)
        items = cur.fetchall()
        
        cur.execute('SELECT DISTINCT category FROM items WHERE organization_id = %s AND category IS NOT NULL ORDER BY category', (org_id,))
        categories = [row['category'] for row in cur.fetchall()]
        
        cur.execute('SELECT SUM(stock * cost) as total FROM items WHERE organization_id = %s', (org_id,))
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
@login_required
def add():
    if request.method == 'POST':
        try:
            org_id = get_current_org_id()
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO items (organization_id, name, category, stock, reorder_point, cost, unit, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                org_id,
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
            flash('Item added successfully!', 'success')
            return redirect('/inventory')
        except Exception as e:
            print(f"Add item error: {e}")
            flash('Error adding item', 'danger')
    return render_template('add.html')

@app.route('/update/<int:id>', methods=['POST'])
@login_required
def update(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        # Security: Make sure item belongs to user's organization
        cur.execute('UPDATE items SET stock = %s, updated_at = %s WHERE id = %s AND organization_id = %s', 
                    (request.form['stock'], datetime.now(), id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Stock updated!', 'success')
        return redirect('/inventory')
    except Exception as e:
        print(f"Update error: {e}")
        flash('Error updating item', 'danger')
        return redirect('/inventory')

@app.route('/delete/<int:id>')
@login_required
def delete(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM items WHERE id = %s AND organization_id = %s', (id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Item deleted!', 'success')
        return redirect('/inventory')
    except Exception as e:
        print(f"Delete error: {e}")
        flash('Error deleting item', 'danger')
        return redirect('/inventory')

@app.route('/alerts')
@login_required
def alerts():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT * FROM items 
            WHERE organization_id = %s AND stock <= reorder_point 
            ORDER BY stock ASC
        ''', (org_id,))
        low_stock = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('alerts.html', items=low_stock)
    except Exception as e:
        print(f"Alerts error: {e}")
        return f"Error loading alerts: {str(e)}", 500

# RECIPE ROUTES

@app.route('/recipes')
@login_required
def recipes():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT 
                r.*,
                COALESCE(SUM(ri.quantity * i.cost), 0) as total_cost,
                COUNT(ri.id) as ingredient_count
            FROM recipes r
            LEFT JOIN recipe_ingredients ri ON r.id = ri.recipe_id
            LEFT JOIN items i ON ri.inventory_item_id = i.id
            WHERE r.organization_id = %s
            GROUP BY r.id
            ORDER BY r.name
        ''', (org_id,))
        recipes_list = cur.fetchall()
        
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
@login_required
def add_recipe():
    if request.method == 'POST':
        try:
            org_id = get_current_org_id()
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO recipes (organization_id, name, category, selling_price, portion_size, servings, instructions, notes, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                org_id,
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
            flash('Recipe created successfully!', 'success')
            return redirect(f'/recipes/{recipe_id}')
        except Exception as e:
            print(f"Add recipe error: {e}")
            flash('Error creating recipe', 'danger')
    
    return render_template('add_recipe.html')

@app.route('/recipes/<int:id>')
@login_required
def recipe_detail(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('SELECT * FROM recipes WHERE id = %s AND organization_id = %s', (id, org_id))
        recipe = cur.fetchone()
        
        if not recipe:
            flash('Recipe not found', 'danger')
            return redirect('/recipes')
        
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
        
        total_cost = sum(ing['ingredient_cost'] for ing in ingredients)
        if recipe['selling_price'] and recipe['selling_price'] > 0:
            food_cost_percent = (total_cost / recipe['selling_price']) * 100
            gross_profit = recipe['selling_price'] - total_cost
        else:
            food_cost_percent = 0
            gross_profit = 0
        
        cur.execute('SELECT * FROM items WHERE organization_id = %s ORDER BY name', (org_id,))
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
        flash('Error loading recipe', 'danger')
        return redirect('/recipes')

@app.route('/recipes/<int:recipe_id>/add-ingredient', methods=['POST'])
@login_required
def add_ingredient_to_recipe(recipe_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        # Verify recipe belongs to user's org
        cur.execute('SELECT id FROM recipes WHERE id = %s AND organization_id = %s', (recipe_id, org_id))
        if not cur.fetchone():
            flash('Recipe not found', 'danger')
            return redirect('/recipes')
        
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
        flash('Ingredient added!', 'success')
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Add ingredient error: {e}")
        flash('Error adding ingredient', 'danger')
        return redirect(f'/recipes/{recipe_id}')

@app.route('/recipes/<int:recipe_id>/remove-ingredient/<int:ingredient_id>')
@login_required
def remove_ingredient(recipe_id, ingredient_id):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM recipe_ingredients WHERE id = %s', (ingredient_id,))
        conn.commit()
        cur.close()
        conn.close()
        flash('Ingredient removed!', 'success')
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Remove ingredient error: {e}")
        flash('Error removing ingredient', 'danger')
        return redirect(f'/recipes/{recipe_id}')

@app.route('/recipes/delete/<int:id>')
@login_required
def delete_recipe(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM recipes WHERE id = %s AND organization_id = %s', (id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Recipe deleted!', 'success')
        return redirect('/recipes')
    except Exception as e:
        print(f"Delete recipe error: {e}")
        flash('Error deleting recipe', 'danger')
        return redirect('/recipes')
@app.route('/inventory/export')
@login_required
def export_inventory():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT name, category, stock, unit, reorder_point, cost, 
                   (stock * cost) as total_value,
                   CASE 
                       WHEN stock <= reorder_point THEN 'REORDER NOW'
                       WHEN stock <= reorder_point * 1.5 THEN 'Low Stock'
                       ELSE 'Good'
                   END as status
            FROM items 
            WHERE organization_id = %s 
            ORDER BY category, name
        ''', (org_id,))
        
        items = cur.fetchall()
        cur.close()
        conn.close()
        
        # Create CSV
        output = StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow([
            'Item Name', 'Category', 'Current Stock', 'Unit', 
            'Reorder Point', 'Cost per Unit', 'Total Value', 'Status'
        ])
        
        # Data rows
        for item in items:
            writer.writerow([
                item['name'],
                item['category'],
                f"{item['stock']:.2f}",
                item['unit'],
                f"{item['reorder_point']:.2f}",
                f"£{item['cost']:.2f}",
                f"£{item['total_value']:.2f}",
                item['status']
            ])
        
        # Create response
        from flask import make_response
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=inventory_export.csv'
        response.headers['Content-Type'] = 'text/csv'
        
        return response
        
    except Exception as e:
        print(f"Export error: {e}")
        flash('Error exporting inventory', 'danger')
        return redirect('/inventory')
if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
