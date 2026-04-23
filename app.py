from flask import Flask, render_template, request, redirect, url_for, flash, session, make_response
import psycopg
from psycopg.rows import dict_row
import os
import traceback
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import csv
from io import StringIO
import stripe

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

DATABASE_URL = os.environ.get('DATABASE_URL')

# Stripe configuration
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')
STRIPE_PUBLISHABLE_KEY = os.environ.get('STRIPE_PUBLISHABLE_KEY')

PRICE_IDS = {
    'haccp_monthly': os.environ.get('STRIPE_HACCP_MONTHLY'),
    'haccp_yearly': os.environ.get('STRIPE_HACCP_YEARLY'),
    'essentials_monthly': os.environ.get('STRIPE_ESSENTIALS_MONTHLY'),
    'essentials_yearly': os.environ.get('STRIPE_ESSENTIALS_YEARLY'),
    'complete_monthly': os.environ.get('STRIPE_COMPLETE_MONTHLY'),
    'complete_yearly': os.environ.get('STRIPE_COMPLETE_YEARLY'),
}

PLANS = {
    'haccp': {
        'name': 'HACCP Compliance',
        'monthly_price': 12.99,
        'yearly_price': 129.90,
        'features': [
            'Full HACCP compliance',
            'Temperature logging',
            'Cleaning schedules',
            'Allergen tracking',
            'Pest control logs',
            'Supplier verification',
            'Delivery inspection',
            'Staff training records',
            'Compliance reports',
            'Multi-user access'
        ]
    },
    'essentials': {
        'name': 'Essentials',
        'monthly_price': 24.99,
        'yearly_price': 249.90,
        'features': [
            'Everything in HACCP',
            'Inventory management',
            'Recipe costing',
            'Stock alerts',
            'Food cost tracking',
            'Supplier management',
            'Wastage tracking',
            'Export to Excel/PDF',
            'Reorder reports'
        ]
    },
    'complete': {
        'name': 'Complete',
        'monthly_price': 39.99,
        'yearly_price': 399.90,
        'features': [
            'Everything in Essentials',
            'Reservation management',
            'Table management',
            'Guest database',
            'Online booking widget',
            'Kitchen-to-front integration',
            'Priority support',
            'Advanced reports'
        ]
    }
}

def get_db():
    try:
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def get_current_org_id():
    if 'organization_id' in session:
        return session['organization_id']
    return None

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
                session['user_id'] = user['id']
                session['organization_id'] = user['organization_id']
                session['user_name'] = f"{user['first_name']} {user['last_name']}"
                session['user_role'] = user['role']
                
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

@app.route('/')
def home():
    if 'user_id' not in session:
        return render_template('landing.html')
    
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
        org = cur.fetchone()
        
        cur.execute('SELECT COUNT(*) as total_items FROM items WHERE organization_id = %s', (org_id,))
        total_items = cur.fetchone()['total_items']
        
        cur.execute('SELECT COUNT(*) as low_stock FROM items WHERE organization_id = %s AND stock <= reorder_point', (org_id,))
        low_stock_count = cur.fetchone()['low_stock']
        
        cur.execute('SELECT SUM(stock * cost) as total_value FROM items WHERE organization_id = %s', (org_id,))
        result = cur.fetchone()
        total_value = result['total_value'] if result['total_value'] else 0
        
        cur.execute('SELECT COUNT(*) as total_recipes FROM recipes WHERE organization_id = %s', (org_id,))
        total_recipes = cur.fetchone()['total_recipes']
        
        cur.close()
        conn.close()
        
        return render_template('dashboard.html', 
                             org=org,
                             plans=PLANS,
                             total_items=total_items,
                             low_stock_count=low_stock_count,
                             total_value=float(total_value),
                             total_recipes=total_recipes)
    except Exception as e:
        print(f"Dashboard error: {e}")
        traceback.print_exc()
        return render_template('dashboard.html', 
                             org=None,
                             plans=PLANS,
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
        
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow(['Item Name', 'Category', 'Current Stock', 'Unit', 'Reorder Point', 'Cost per Unit', 'Total Value', 'Status'])
        
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
        
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=inventory_export.csv'
        response.headers['Content-Type'] = 'text/csv'
        
        return response
        
    except Exception as e:
        print(f"Export error: {e}")
        flash('Error exporting inventory', 'danger')
        return redirect('/inventory')

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
        cur.execute('SELECT * FROM items WHERE organization_id = %s AND stock <= reorder_point ORDER BY stock ASC', (org_id,))
        low_stock = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('alerts.html', items=low_stock)
    except Exception as e:
        print(f"Alerts error: {e}")
        return f"Error loading alerts: {str(e)}", 500

@app.route('/recipes')
@login_required
def recipes():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT r.*, COALESCE(SUM(ri.quantity * i.cost), 0) as total_cost, COUNT(ri.id) as ingredient_count
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
            return redirect('/recipes')
        
        cur.execute('''
            SELECT ri.*, i.name as item_name, i.unit, i.cost as unit_cost, (ri.quantity * i.cost) as ingredient_cost
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
        return redirect('/recipes')

@app.route('/recipes/<int:recipe_id>/add-ingredient', methods=['POST'])
@login_required
def add_ingredient_to_recipe(recipe_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('SELECT id FROM recipes WHERE id = %s AND organization_id = %s', (recipe_id, org_id))
        if not cur.fetchone():
            return redirect('/recipes')
        
        cur.execute('''
            INSERT INTO recipe_ingredients (recipe_id, inventory_item_id, quantity, notes)
            VALUES (%s, %s, %s, %s)
        ''', (recipe_id, request.form['item_id'], request.form['quantity'], request.form.get('notes', '')))
        conn.commit()
        cur.close()
        conn.close()
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Add ingredient error: {e}")
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
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
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
        return redirect('/recipes')
    except Exception as e:
        return redirect('/recipes')

@app.route('/billing')
@login_required
def billing():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
        org = cur.fetchone()
        cur.close()
        conn.close()
        
        return render_template('billing.html', org=org, plans=PLANS, stripe_key=STRIPE_PUBLISHABLE_KEY)
    except Exception as e:
        print(f"Billing error: {e}")
        traceback.print_exc()
        flash('Error loading billing information', 'danger')
        return redirect('/')

@app.route('/create-checkout-session', methods=['POST'])
@login_required
def create_checkout_session():
    try:
        org_id = get_current_org_id()
        plan = request.form.get('plan', 'haccp')
        billing_cycle = request.form.get('billing_cycle', 'monthly')
        
        price_key = f'{plan}_{billing_cycle}'
        price_id = PRICE_IDS.get(price_key)
        
        if not price_id:
            flash('Invalid plan selection', 'danger')
            return redirect('/billing')
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT email FROM organizations WHERE id = %s', (org_id,))
        org = cur.fetchone()
        cur.close()
        conn.close()
        
        checkout_session = stripe.checkout.Session.create(
            customer_email=org['email'],
            payment_method_types=['card'],
            line_items=[{'price': price_id, 'quantity': 1}],
            mode='subscription',
            success_url=request.host_url + 'billing/success?session_id={CHECKOUT_SESSION_ID}&plan=' + plan,
            cancel_url=request.host_url + 'billing',
            metadata={'organization_id': str(org_id), 'plan': plan},
            subscription_data={'trial_period_days': 14, 'metadata': {'organization_id': str(org_id)}}
        )
        
        return redirect(checkout_session.url, code=303)
    except Exception as e:
        print(f"Checkout error: {e}")
        traceback.print_exc()
        flash(f'Error: {str(e)}', 'danger')
        return redirect('/billing')

@app.route('/billing/success')
@login_required
def billing_success():
    try:
        session_id = request.args.get('session_id')
        plan = request.args.get('plan', 'haccp')
        
        if session_id:
            checkout_session = stripe.checkout.Session.retrieve(session_id)
            org_id = get_current_org_id()
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                UPDATE organizations 
                SET stripe_customer_id = %s, stripe_subscription_id = %s, subscription_plan = %s, subscription_status = 'active'
                WHERE id = %s
            ''', (checkout_session.customer, checkout_session.subscription, plan, org_id))
            conn.commit()
            cur.close()
            conn.close()
            flash(f'🎉 Welcome! Your {PLANS[plan]["name"]} plan is active. 14-day free trial started.', 'success')
        return redirect('/billing')
    except Exception as e:
        print(f"Success error: {e}")
        return redirect('/billing')

@app.route('/billing/portal')
@login_required
def billing_portal():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT stripe_customer_id FROM organizations WHERE id = %s', (org_id,))
        org = cur.fetchone()
        cur.close()
        conn.close()
        
        if not org['stripe_customer_id']:
            flash('No active subscription found', 'warning')
            return redirect('/billing')
        
        portal_session = stripe.billing_portal.Session.create(
            customer=org['stripe_customer_id'],
            return_url=request.host_url + 'billing',
        )
        return redirect(portal_session.url, code=303)
    except Exception as e:
        print(f"Portal error: {e}")
        return redirect('/billing')
# ========== HACCP ROUTES ==========

# HACCP Dashboard
@app.route('/haccp')
# @login_required
def haccp_dashboard():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        # Today's temp checks
        cur.execute('''
            SELECT COUNT(*) as count FROM temperature_logs 
            WHERE organization_id = %s AND DATE(logged_at) = CURRENT_DATE
        ''', (org_id,))
        today_temps = cur.fetchone()['count']
        
        # Failed temp checks this week
        cur.execute('''
            SELECT COUNT(*) as count FROM temperature_logs 
            WHERE organization_id = %s AND status = 'fail' 
            AND logged_at >= CURRENT_DATE - INTERVAL '7 days'
        ''', (org_id,))
        failed_temps = cur.fetchone()['count']
        
        # Cleaning tasks today
        cur.execute('''
            SELECT COUNT(*) as count FROM cleaning_records 
            WHERE organization_id = %s AND DATE(completed_at) = CURRENT_DATE
        ''', (org_id,))
        today_cleanings = cur.fetchone()['count']
        
        # Equipment count
        cur.execute('SELECT COUNT(*) as count FROM equipment WHERE organization_id = %s AND is_active = true', (org_id,))
        equipment_count = cur.fetchone()['count']
        
        # Recent temperature logs
        cur.execute('''
            SELECT * FROM temperature_logs 
            WHERE organization_id = %s 
            ORDER BY logged_at DESC LIMIT 5
        ''', (org_id,))
        recent_temps = cur.fetchall()
        
        # Recent cleanings
        cur.execute('''
            SELECT cr.*, ct.task_name 
            FROM cleaning_records cr
            JOIN cleaning_tasks ct ON cr.task_id = ct.id
            WHERE cr.organization_id = %s
            ORDER BY cr.completed_at DESC LIMIT 5
        ''', (org_id,))
        recent_cleanings = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return render_template('haccp_dashboard.html',
                             today_temps=today_temps,
                             failed_temps=failed_temps,
                             today_cleanings=today_cleanings,
                             equipment_count=equipment_count,
                             recent_temps=recent_temps,
                             recent_cleanings=recent_cleanings)
    except Exception as e:
        print(f"HACCP dashboard error: {e}")
        traceback.print_exc()
        flash('Error loading HACCP dashboard', 'danger')
        return redirect('/')

# Temperature Logs
@app.route('/haccp/temperatures')
@login_required
def temperatures():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT * FROM temperature_logs 
            WHERE organization_id = %s 
            ORDER BY logged_at DESC LIMIT 100
        ''', (org_id,))
        logs = cur.fetchall()
        
        cur.execute('SELECT * FROM equipment WHERE organization_id = %s AND is_active = true ORDER BY name', (org_id,))
        equipment_list = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return render_template('temperatures.html', logs=logs, equipment_list=equipment_list)
    except Exception as e:
        print(f"Temperatures error: {e}")
        traceback.print_exc()
        return f"Error: {str(e)}", 500

@app.route('/haccp/temperatures/add', methods=['POST'])
@login_required
def add_temperature():
    try:
        org_id = get_current_org_id()
        temp = float(request.form['temperature'])
        target_min = float(request.form.get('target_min', 0))
        target_max = float(request.form.get('target_max', 0))
        
        # Determine status
        if target_min and target_max:
            if temp < target_min or temp > target_max:
                status = 'fail'
            else:
                status = 'pass'
        else:
            status = 'logged'
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO temperature_logs 
            (organization_id, equipment_name, equipment_type, temperature, unit, target_min, target_max, status, notes, corrective_action, logged_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            org_id,
            request.form['equipment_name'],
            request.form.get('equipment_type', ''),
            temp,
            request.form.get('unit', 'C'),
            target_min or None,
            target_max or None,
            status,
            request.form.get('notes', ''),
            request.form.get('corrective_action', ''),
            session.get('user_name', 'Unknown')
        ))
        conn.commit()
        cur.close()
        conn.close()
        
        if status == 'fail':
            flash('⚠️ Temperature logged - OUT OF RANGE! Add corrective action.', 'warning')
        else:
            flash('✅ Temperature logged successfully!', 'success')
        
        return redirect('/haccp/temperatures')
    except Exception as e:
        print(f"Add temperature error: {e}")
        flash('Error logging temperature', 'danger')
        return redirect('/haccp/temperatures')

# Equipment Management
@app.route('/haccp/equipment', methods=['GET', 'POST'])
@login_required
def equipment():
    if request.method == 'POST':
        try:
            org_id = get_current_org_id()
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO equipment (organization_id, name, equipment_type, location, target_min, target_max)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                org_id,
                request.form['name'],
                request.form.get('equipment_type', ''),
                request.form.get('location', ''),
                request.form.get('target_min') or None,
                request.form.get('target_max') or None
            ))
            conn.commit()
            cur.close()
            conn.close()
            flash('Equipment added!', 'success')
            return redirect('/haccp/equipment')
        except Exception as e:
            print(f"Add equipment error: {e}")
            flash('Error adding equipment', 'danger')
    
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM equipment WHERE organization_id = %s ORDER BY name', (org_id,))
        equipment_list = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('equipment.html', equipment_list=equipment_list)
    except Exception as e:
        print(f"Equipment error: {e}")
        return f"Error: {str(e)}", 500

@app.route('/haccp/equipment/delete/<int:id>')
@login_required
def delete_equipment(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM equipment WHERE id = %s AND organization_id = %s', (id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Equipment deleted!', 'success')
        return redirect('/haccp/equipment')
    except Exception as e:
        flash('Error deleting equipment', 'danger')
        return redirect('/haccp/equipment')

# Cleaning Tasks
@app.route('/haccp/cleaning')
@login_required
def cleaning():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('SELECT * FROM cleaning_tasks WHERE organization_id = %s AND is_active = true ORDER BY frequency, task_name', (org_id,))
        tasks = cur.fetchall()
        
        cur.execute('''
            SELECT cr.*, ct.task_name 
            FROM cleaning_records cr
            JOIN cleaning_tasks ct ON cr.task_id = ct.id
            WHERE cr.organization_id = %s
            ORDER BY cr.completed_at DESC LIMIT 50
        ''', (org_id,))
        records = cur.fetchall()
        
        cur.close()
        conn.close()
        return render_template('cleaning.html', tasks=tasks, records=records)
    except Exception as e:
        print(f"Cleaning error: {e}")
        return f"Error: {str(e)}", 500

@app.route('/haccp/cleaning/add-task', methods=['POST'])
@login_required
def add_cleaning_task():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO cleaning_tasks (organization_id, task_name, frequency, area, instructions)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            org_id,
            request.form['task_name'],
            request.form.get('frequency', 'daily'),
            request.form.get('area', ''),
            request.form.get('instructions', '')
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Cleaning task added!', 'success')
        return redirect('/haccp/cleaning')
    except Exception as e:
        flash('Error adding task', 'danger')
        return redirect('/haccp/cleaning')

@app.route('/haccp/cleaning/complete/<int:task_id>', methods=['POST'])
@login_required
def complete_cleaning(task_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO cleaning_records (organization_id, task_id, completed_by, notes)
            VALUES (%s, %s, %s, %s)
        ''', (
            org_id,
            task_id,
            session.get('user_name', 'Unknown'),
            request.form.get('notes', '')
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('✅ Task marked complete!', 'success')
        return redirect('/haccp/cleaning')
    except Exception as e:
        flash('Error completing task', 'danger')
        return redirect('/haccp/cleaning')

# Delivery Inspections
@app.route('/haccp/deliveries', methods=['GET', 'POST'])
@login_required
def deliveries():
    if request.method == 'POST':
        try:
            org_id = get_current_org_id()
            temp = float(request.form.get('temperature', 0)) if request.form.get('temperature') else None
            date_codes_ok = request.form.get('date_codes_ok') == 'yes'
            
            # Determine status
            status = 'pass'
            if temp and temp > 5:
                status = 'fail'
            if not date_codes_ok:
                status = 'fail'
            
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO delivery_logs 
                (organization_id, supplier_name, delivery_date, temperature, packaging_condition, date_codes_ok, status, notes, corrective_action, inspected_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                org_id,
                request.form['supplier_name'],
                request.form.get('delivery_date'),
                temp,
                request.form.get('packaging_condition', 'good'),
                date_codes_ok,
                status,
                request.form.get('notes', ''),
                request.form.get('corrective_action', ''),
                session.get('user_name', 'Unknown')
            ))
            conn.commit()
            cur.close()
            conn.close()
            
            if status == 'fail':
                flash('⚠️ Delivery inspection recorded - ISSUES FOUND!', 'warning')
            else:
                flash('✅ Delivery inspection recorded!', 'success')
            
            return redirect('/haccp/deliveries')
        except Exception as e:
            print(f"Delivery error: {e}")
            flash('Error recording delivery', 'danger')
    
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM delivery_logs WHERE organization_id = %s ORDER BY logged_at DESC LIMIT 50', (org_id,))
        deliveries_list = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('deliveries.html', deliveries=deliveries_list)
    except Exception as e:
        return f"Error: {str(e)}", 500

# Compliance Reports
@app.route('/haccp/reports')
@login_required
def haccp_reports():
    try:
        org_id = get_current_org_id()
        from datetime import timedelta
        
        conn = get_db()
        cur = conn.cursor()
        
        # Last 30 days stats
        cur.execute('''
            SELECT COUNT(*) as total, 
                   COUNT(*) FILTER (WHERE status = 'pass') as passed,
                   COUNT(*) FILTER (WHERE status = 'fail') as failed
            FROM temperature_logs 
            WHERE organization_id = %s AND logged_at >= CURRENT_DATE - INTERVAL '30 days'
        ''', (org_id,))
        temp_stats = cur.fetchone()
        
        cur.execute('''
            SELECT COUNT(*) as total FROM cleaning_records 
            WHERE organization_id = %s AND completed_at >= CURRENT_DATE - INTERVAL '30 days'
        ''', (org_id,))
        cleaning_count = cur.fetchone()['total']
        
        cur.execute('''
            SELECT COUNT(*) as total, 
                   COUNT(*) FILTER (WHERE status = 'pass') as passed,
                   COUNT(*) FILTER (WHERE status = 'fail') as failed
            FROM delivery_logs 
            WHERE organization_id = %s AND logged_at >= CURRENT_DATE - INTERVAL '30 days'
        ''', (org_id,))
        delivery_stats = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return render_template('haccp_reports.html',
                             temp_stats=temp_stats,
                             cleaning_count=cleaning_count,
                             delivery_stats=delivery_stats)
    except Exception as e:
        print(f"Reports error: {e}")
        return f"Error: {str(e)}", 500
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
