from flask import Flask, render_template, request, redirect, url_for, flash, session, make_response
import psycopg
from psycopg.rows import dict_row
import os
import traceback
from datetime import datetime, timedelta
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


def get_compliance_alerts(org_id):
    """Returns a dict of categorised alerts for HACCP compliance."""
    alerts = {'critical': [], 'warning': [], 'info': []}
    conn = get_db()
    cur = conn.cursor()

    # 1. Temperature checks overdue per equipment
    cur.execute('''
        SELECT e.id, e.name, e.equipment_type, e.check_frequency_hours,
               MAX(tl.logged_at) as last_log
        FROM haccp_equipment e
        LEFT JOIN haccp_temperature_logs tl
            ON tl.equipment_id = e.id AND tl.is_voided = false
        WHERE e.organization_id = %s AND e.is_active = true
        GROUP BY e.id, e.name, e.equipment_type, e.check_frequency_hours
    ''', (org_id,))
    equipment_status = cur.fetchall()

    now = datetime.now()
    for eq in equipment_status:
        freq_hours = eq['check_frequency_hours'] or 24
        if eq['last_log'] is None:
            alerts['warning'].append({
                'type': 'temp_never_logged',
                'severity': 'warning',
                'title': f"No temperature ever logged for {eq['name']}",
                'detail': f"{eq['equipment_type']} \u2014 add a first reading to start the audit trail",
                'link': '/haccp/temperatures',
            })
            continue
        last_log = eq['last_log']
        if last_log.tzinfo is not None:
            last_log = last_log.replace(tzinfo=None)
        hours_since = (now - last_log).total_seconds() / 3600
        if hours_since > freq_hours * 2:
            alerts['critical'].append({
                'type': 'temp_severely_overdue',
                'severity': 'critical',
                'title': f"{eq['name']} \u2014 temperature check severely overdue",
                'detail': f"Last logged {int(hours_since)}h ago (expected every {freq_hours}h)",
                'link': '/haccp/temperatures',
            })
        elif hours_since > freq_hours:
            alerts['warning'].append({
                'type': 'temp_overdue',
                'severity': 'warning',
                'title': f"{eq['name']} \u2014 temperature check overdue",
                'detail': f"Last logged {int(hours_since)}h ago (expected every {freq_hours}h)",
                'link': '/haccp/temperatures',
            })

    # 2. Recent temperature failures without corrective action
    cur.execute('''
        SELECT tl.id, tl.temperature, tl.logged_at, tl.corrective_action,
               e.name as equipment_name
        FROM haccp_temperature_logs tl
        JOIN haccp_equipment e ON e.id = tl.equipment_id
        WHERE tl.organization_id = %s
          AND tl.status = 'fail'
          AND tl.is_voided = false
          AND (tl.corrective_action IS NULL OR tl.corrective_action = '')
          AND tl.logged_at >= NOW() - INTERVAL '7 days'
        ORDER BY tl.logged_at DESC
    ''', (org_id,))
    for fail in cur.fetchall():
        alerts['critical'].append({
            'type': 'temp_fail_no_action',
            'severity': 'critical',
            'title': "Failed temperature with no corrective action recorded",
            'detail': f"{fail['equipment_name']}: {fail['temperature']}\u00b0C on {fail['logged_at'].strftime('%d %b %H:%M')}",
            'link': '/haccp/temperatures',
        })

    # 3. Cleaning tasks overdue based on frequency
    cur.execute('''
        SELECT ct.id, ct.task_name, ct.frequency,
               MAX(cl.completed_at) as last_done
        FROM haccp_cleaning_tasks ct
        LEFT JOIN haccp_cleaning_logs cl
            ON cl.task_id = ct.id AND cl.is_voided = false
        WHERE ct.organization_id = %s AND ct.is_active = true
        GROUP BY ct.id, ct.task_name, ct.frequency
    ''', (org_id,))
    freq_to_hours = {
        'Daily': 24, 'Weekly': 168, 'Monthly': 720,
        'Quarterly': 2160, 'Annually': 8760, 'After use': None,
    }
    for task in cur.fetchall():
        max_hours = freq_to_hours.get(task['frequency'])
        if max_hours is None:
            continue
        if task['last_done'] is None:
            alerts['warning'].append({
                'type': 'cleaning_never_done',
                'severity': 'warning',
                'title': f"Cleaning task never recorded: {task['task_name']}",
                'detail': f"Expected frequency: {task['frequency']}",
                'link': '/haccp/cleaning',
            })
            continue
        last_done = task['last_done']
        if last_done.tzinfo is not None:
            last_done = last_done.replace(tzinfo=None)
        hours_since = (now - last_done).total_seconds() / 3600
        if hours_since > max_hours * 1.5:
            alerts['critical'].append({
                'type': 'cleaning_severely_overdue',
                'severity': 'critical',
                'title': f"{task['task_name']} \u2014 severely overdue",
                'detail': f"Last done {int(hours_since/24)} days ago ({task['frequency']})",
                'link': '/haccp/cleaning',
            })
        elif hours_since > max_hours:
            alerts['warning'].append({
                'type': 'cleaning_overdue',
                'severity': 'warning',
                'title': f"{task['task_name']} \u2014 overdue",
                'detail': f"Last done {int(hours_since/24)} days ago ({task['frequency']})",
                'link': '/haccp/cleaning',
            })

    # 4. Business settings incomplete (info-level reminder)
    cur.execute('SELECT business_name, food_business_registration FROM organizations WHERE id = %s', (org_id,))
    org = cur.fetchone()
    if org and (not org.get('business_name') or not org.get('food_business_registration')):
        alerts['info'].append({
            'type': 'settings_incomplete',
            'severity': 'info',
            'title': 'Business details incomplete',
            'detail': 'Add your trading name and FBO registration so PDFs are audit-ready',
            'link': '/settings/business',
        })

    cur.close()
    conn.close()
    return alerts


def cleaning_task_due_status(last_done, frequency):
    """Return (status, label) for a cleaning task."""
    freq_to_hours = {
        'Daily': 24, 'Weekly': 168, 'Monthly': 720,
        'Quarterly': 2160, 'Annually': 8760, 'After use': None,
    }
    max_hours = freq_to_hours.get(frequency)
    if max_hours is None:
        return ('no_recurrence', 'as needed')
    if last_done is None:
        return ('never', 'never done')
    last = last_done.replace(tzinfo=None) if last_done.tzinfo else last_done
    hours_since = (datetime.now() - last).total_seconds() / 3600
    hours_remaining = max_hours - hours_since
    if hours_remaining < 0:
        days_over = abs(int(hours_remaining / 24))
        return ('overdue', f"overdue by {days_over}d" if days_over else "overdue")
    if hours_remaining < max_hours * 0.25:
        hrs = int(hours_remaining)
        return ('due_soon', f"due in {hrs}h" if hrs < 48 else f"due in {int(hrs/24)}d")
    return ('ok', f"next due in {int(hours_remaining/24)}d" if hours_remaining > 48 else f"next due in {int(hours_remaining)}h")


@app.route('/haccp')
@login_required
def haccp_dashboard():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        cur.execute('SELECT COUNT(*) as count FROM haccp_equipment WHERE organization_id = %s AND is_active = true', (org_id,))
        equipment_count = cur.fetchone()['count']

        cur.execute("SELECT COUNT(*) as count FROM haccp_temperature_logs WHERE organization_id = %s AND logged_at >= NOW() - INTERVAL '24 hours'", (org_id,))
        temps_today = cur.fetchone()['count']

        cur.execute("SELECT COUNT(*) as count FROM haccp_temperature_logs WHERE organization_id = %s AND status = 'fail' AND logged_at >= NOW() - INTERVAL '7 days'", (org_id,))
        temp_failures = cur.fetchone()['count']

        cur.execute('SELECT COUNT(*) as count FROM haccp_cleaning_tasks WHERE organization_id = %s AND is_active = true', (org_id,))
        cleaning_tasks = cur.fetchone()['count']

        cur.execute("SELECT COUNT(*) as count FROM haccp_delivery_logs WHERE organization_id = %s AND created_at >= NOW() - INTERVAL '7 days'", (org_id,))
        deliveries_week = cur.fetchone()['count']

        cur.execute('''
            SELECT tl.*, e.name as equipment_name, e.equipment_type, e.min_temp, e.max_temp
            FROM haccp_temperature_logs tl
            JOIN haccp_equipment e ON tl.equipment_id = e.id
            WHERE tl.organization_id = %s
            ORDER BY tl.logged_at DESC
            LIMIT 10
        ''', (org_id,))
        recent_temps = cur.fetchall()

        cur.close()
        conn.close()

        alerts = get_compliance_alerts(org_id)

        return render_template('haccp_dashboard.html',
                             equipment_count=equipment_count,
                             temps_today=temps_today,
                             temp_failures=temp_failures,
                             cleaning_tasks=cleaning_tasks,
                             deliveries_week=deliveries_week,
                             recent_temps=recent_temps,
                             alerts=alerts)
    except Exception as e:
        print(f"HACCP dashboard error: {e}")
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@app.route('/haccp/temperatures')
@login_required
def haccp_temperatures():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        cur.execute('SELECT * FROM haccp_equipment WHERE organization_id = %s AND is_active = true ORDER BY name', (org_id,))
        equipment = cur.fetchall()

        cur.execute('''
            SELECT tl.*, e.name as equipment_name, e.equipment_type, e.min_temp, e.max_temp
            FROM haccp_temperature_logs tl
            JOIN haccp_equipment e ON tl.equipment_id = e.id
            WHERE tl.organization_id = %s
            ORDER BY tl.logged_at DESC
            LIMIT 50
        ''', (org_id,))
        logs = cur.fetchall()

        cur.close()
        conn.close()

        return render_template('haccp_temperatures.html', equipment=equipment, logs=logs)
    except Exception as e:
        print(f"Temperature error: {e}")
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@app.route('/haccp/equipment/add', methods=['POST'])
@login_required
def add_equipment():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO haccp_equipment (organization_id, name, equipment_type, location, min_temp, max_temp)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            org_id,
            request.form['name'],
            request.form['equipment_type'],
            request.form.get('location', ''),
            float(request.form['min_temp']) if request.form.get('min_temp') else None,
            float(request.form['max_temp']) if request.form.get('max_temp') else None
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Equipment added!', 'success')
        return redirect('/haccp/temperatures')
    except Exception as e:
        print(f"Add equipment error: {e}")
        flash('Error adding equipment', 'danger')
        return redirect('/haccp/temperatures')


@app.route('/haccp/equipment/edit/<int:id>', methods=['POST'])
@login_required
def edit_equipment(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE haccp_equipment
            SET name = %s, equipment_type = %s, location = %s, min_temp = %s, max_temp = %s
            WHERE id = %s AND organization_id = %s
        ''', (
            request.form['name'],
            request.form['equipment_type'],
            request.form.get('location', ''),
            float(request.form['min_temp']) if request.form.get('min_temp') else None,
            float(request.form['max_temp']) if request.form.get('max_temp') else None,
            id,
            org_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Equipment updated!', 'success')
        return redirect('/haccp/temperatures')
    except Exception as e:
        print(f"Edit equipment error: {e}")
        flash('Error updating equipment', 'danger')
        return redirect('/haccp/temperatures')


@app.route('/haccp/equipment/delete/<int:id>', methods=['POST'])
@login_required
def soft_delete_equipment(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('UPDATE haccp_equipment SET is_active = false WHERE id = %s AND organization_id = %s', (id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Equipment removed from active list. Historical logs preserved.', 'success')
        return redirect('/haccp/temperatures')
    except Exception as e:
        print(f"Delete equipment error: {e}")
        flash('Error removing equipment', 'danger')
        return redirect('/haccp/temperatures')


@app.route('/haccp/temperature/log', methods=['POST'])
@login_required
def log_temperature():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        cur.execute('SELECT min_temp, max_temp FROM haccp_equipment WHERE id = %s', (request.form['equipment_id'],))
        equipment = cur.fetchone()

        temp = float(request.form['temperature'])
        status = 'pass'

        if equipment['min_temp'] is not None and temp < equipment['min_temp']:
            status = 'fail'
        elif equipment['max_temp'] is not None and temp > equipment['max_temp']:
            status = 'fail'

        cur.execute('''
            INSERT INTO haccp_temperature_logs (organization_id, equipment_id, temperature, status, notes, corrective_action, logged_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (
            org_id,
            request.form['equipment_id'],
            temp,
            status,
            request.form.get('notes', ''),
            request.form.get('corrective_action', ''),
            session.get('user_name', 'Unknown')
        ))
        conn.commit()
        cur.close()
        conn.close()

        if status == 'fail':
            flash(f'\u26a0\ufe0f Temperature {temp}\u00b0C is OUT OF RANGE! Log corrective action.', 'danger')
        else:
            flash(f'\u2705 Temperature {temp}\u00b0C logged successfully', 'success')

        return redirect('/haccp/temperatures')
    except Exception as e:
        print(f"Log temperature error: {e}")
        flash('Error logging temperature', 'danger')
        return redirect('/haccp/temperatures')


@app.route('/haccp/temperature/void/<int:id>', methods=['POST'])
@login_required
def void_temperature_log(id):
    try:
        org_id = get_current_org_id()
        reason = request.form.get('void_reason', '').strip()
        if not reason:
            flash('A reason is required to void a log entry.', 'danger')
            return redirect('/haccp/temperatures')
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE haccp_temperature_logs
            SET is_voided = true, void_reason = %s, voided_by = %s, voided_at = NOW()
            WHERE id = %s AND organization_id = %s
        ''', (reason, session.get('user_name', 'Unknown'), id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Temperature log voided. Original record preserved for audit.', 'success')
        return redirect('/haccp/temperatures')
    except Exception as e:
        print(f"Void temperature error: {e}")
        flash('Error voiding log', 'danger')
        return redirect('/haccp/temperatures')


@app.route('/haccp/cleaning')
@login_required
def haccp_cleaning():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        cur.execute('''
            SELECT ct.*, MAX(cl.completed_at) as last_done
            FROM haccp_cleaning_tasks ct
            LEFT JOIN haccp_cleaning_logs cl
                ON cl.task_id = ct.id AND cl.is_voided = false
            WHERE ct.organization_id = %s AND ct.is_active = true
            GROUP BY ct.id
            ORDER BY ct.task_name
        ''', (org_id,))
        tasks = cur.fetchall()

        for task in tasks:
            status, label = cleaning_task_due_status(task['last_done'], task['frequency'])
            task['due_status'] = status
            task['due_label'] = label

        cur.execute('''
            SELECT cl.*, ct.task_name, ct.area, ct.frequency
            FROM haccp_cleaning_logs cl
            JOIN haccp_cleaning_tasks ct ON cl.task_id = ct.id
            WHERE cl.organization_id = %s
            ORDER BY cl.completed_at DESC
            LIMIT 30
        ''', (org_id,))
        logs = cur.fetchall()

        cur.close()
        conn.close()

        return render_template('haccp_cleaning.html', tasks=tasks, logs=logs)
    except Exception as e:
        print(f"Cleaning error: {e}")
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@app.route('/haccp/cleaning/add-task', methods=['POST'])
@login_required
def add_cleaning_task():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO haccp_cleaning_tasks (organization_id, task_name, area, frequency, chemicals_used, instructions)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            org_id,
            request.form['task_name'],
            request.form.get('area', ''),
            request.form['frequency'],
            request.form.get('chemicals_used', ''),
            request.form.get('instructions', '')
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Cleaning task added!', 'success')
        return redirect('/haccp/cleaning')
    except Exception as e:
        print(f"Add cleaning task error: {e}")
        flash('Error adding task', 'danger')
        return redirect('/haccp/cleaning')


@app.route('/haccp/cleaning/edit-task/<int:id>', methods=['POST'])
@login_required
def edit_cleaning_task(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE haccp_cleaning_tasks
            SET task_name = %s, area = %s, frequency = %s, chemicals_used = %s, instructions = %s
            WHERE id = %s AND organization_id = %s
        ''', (
            request.form['task_name'],
            request.form.get('area', ''),
            request.form['frequency'],
            request.form.get('chemicals_used', ''),
            request.form.get('instructions', ''),
            id,
            org_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Cleaning task updated!', 'success')
        return redirect('/haccp/cleaning')
    except Exception as e:
        print(f"Edit cleaning task error: {e}")
        flash('Error updating task', 'danger')
        return redirect('/haccp/cleaning')


@app.route('/haccp/cleaning/delete-task/<int:id>', methods=['POST'])
@login_required
def soft_delete_cleaning_task(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('UPDATE haccp_cleaning_tasks SET is_active = false WHERE id = %s AND organization_id = %s', (id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Task removed from active list. Historical logs preserved.', 'success')
        return redirect('/haccp/cleaning')
    except Exception as e:
        print(f"Delete cleaning task error: {e}")
        flash('Error removing task', 'danger')
        return redirect('/haccp/cleaning')


@app.route('/haccp/cleaning/log', methods=['POST'])
@login_required
def log_cleaning():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO haccp_cleaning_logs (organization_id, task_id, completed, notes, completed_by)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            org_id,
            request.form['task_id'],
            True,
            request.form.get('notes', ''),
            session.get('user_name', 'Unknown')
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Cleaning logged!', 'success')
        return redirect('/haccp/cleaning')
    except Exception as e:
        print(f"Log cleaning error: {e}")
        flash('Error logging cleaning', 'danger')
        return redirect('/haccp/cleaning')


@app.route('/haccp/cleaning/void/<int:id>', methods=['POST'])
@login_required
def void_cleaning_log(id):
    try:
        org_id = get_current_org_id()
        reason = request.form.get('void_reason', '').strip()
        if not reason:
            flash('A reason is required to void a log entry.', 'danger')
            return redirect('/haccp/cleaning')
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE haccp_cleaning_logs
            SET is_voided = true, void_reason = %s, voided_by = %s, voided_at = NOW()
            WHERE id = %s AND organization_id = %s
        ''', (reason, session.get('user_name', 'Unknown'), id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Cleaning log voided. Original record preserved for audit.', 'success')
        return redirect('/haccp/cleaning')
    except Exception as e:
        print(f"Void cleaning error: {e}")
        flash('Error voiding log', 'danger')
        return redirect('/haccp/cleaning')


@app.route('/haccp/deliveries')
@login_required
def haccp_deliveries():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM haccp_delivery_logs WHERE organization_id = %s ORDER BY delivery_date DESC LIMIT 50', (org_id,))
        deliveries = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('haccp_deliveries.html', deliveries=deliveries)
    except Exception as e:
        print(f"Deliveries error: {e}")
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@app.route('/haccp/deliveries/add', methods=['POST'])
@login_required
def add_delivery():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO haccp_delivery_logs (organization_id, supplier_name, delivery_date, chilled_temp, frozen_temp, packaging_ok, expiry_dates_ok, quality_ok, accepted, notes, inspected_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            org_id,
            request.form['supplier_name'],
            request.form['delivery_date'],
            float(request.form['chilled_temp']) if request.form.get('chilled_temp') else None,
            float(request.form['frozen_temp']) if request.form.get('frozen_temp') else None,
            request.form.get('packaging_ok') == 'on',
            request.form.get('expiry_dates_ok') == 'on',
            request.form.get('quality_ok') == 'on',
            request.form.get('accepted') == 'on',
            request.form.get('notes', ''),
            session.get('user_name', 'Unknown')
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Delivery logged!', 'success')
        return redirect('/haccp/deliveries')
    except Exception as e:
        print(f"Add delivery error: {e}")
        flash('Error logging delivery', 'danger')
        return redirect('/haccp/deliveries')


@app.route('/haccp/deliveries/edit/<int:id>', methods=['POST'])
@login_required
def edit_delivery(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE haccp_delivery_logs
            SET supplier_name = %s, delivery_date = %s,
                chilled_temp = %s, frozen_temp = %s,
                packaging_ok = %s, expiry_dates_ok = %s, quality_ok = %s,
                accepted = %s, notes = %s
            WHERE id = %s AND organization_id = %s
        ''', (
            request.form['supplier_name'],
            request.form['delivery_date'],
            float(request.form['chilled_temp']) if request.form.get('chilled_temp') else None,
            float(request.form['frozen_temp']) if request.form.get('frozen_temp') else None,
            request.form.get('packaging_ok') == 'on',
            request.form.get('expiry_dates_ok') == 'on',
            request.form.get('quality_ok') == 'on',
            request.form.get('accepted') == 'on',
            request.form.get('notes', ''),
            id,
            org_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Delivery updated!', 'success')
        return redirect('/haccp/deliveries')
    except Exception as e:
        print(f"Edit delivery error: {e}")
        flash('Error updating delivery', 'danger')
        return redirect('/haccp/deliveries')


@app.route('/haccp/deliveries/delete/<int:id>', methods=['POST'])
@login_required
def delete_delivery(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM haccp_delivery_logs WHERE id = %s AND organization_id = %s', (id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Delivery deleted.', 'success')
        return redirect('/haccp/deliveries')
    except Exception as e:
        print(f"Delete delivery error: {e}")
        flash('Error deleting delivery', 'danger')
        return redirect('/haccp/deliveries')


# BUSINESS SETTINGS

@app.route('/settings/business')
@login_required
def business_settings():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
        org = cur.fetchone()
        cur.close()
        conn.close()
        return render_template('business_settings.html', org=org)
    except Exception as e:
        print(f"Business settings error: {e}")
        return f"Error: {str(e)}", 500


@app.route('/settings/business/save', methods=['POST'])
@login_required
def save_business_settings():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE organizations
            SET business_name = %s, business_address = %s, business_phone = %s,
                business_email = %s, food_business_registration = %s,
                responsible_person = %s, logo_url = %s
            WHERE id = %s
        ''', (
            request.form.get('business_name', '').strip() or None,
            request.form.get('business_address', '').strip() or None,
            request.form.get('business_phone', '').strip() or None,
            request.form.get('business_email', '').strip() or None,
            request.form.get('food_business_registration', '').strip() or None,
            request.form.get('responsible_person', '').strip() or None,
            request.form.get('logo_url', '').strip() or None,
            org_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Business details saved!', 'success')
        return redirect('/settings/business')
    except Exception as e:
        print(f"Save business settings error: {e}")
        flash('Error saving details', 'danger')
        return redirect('/settings/business')


# HACCP REPORTS (PDF EXPORTS)

@app.route('/haccp/reports')
@login_required
def haccp_reports():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
        org = cur.fetchone()
        cur.close()
        conn.close()
        today = datetime.now().date()
        thirty_days_ago = today - timedelta(days=30)
        return render_template('haccp_reports.html',
                             org=org,
                             default_from=thirty_days_ago.strftime('%Y-%m-%d'),
                             default_to=today.strftime('%Y-%m-%d'))
    except Exception as e:
        print(f"Reports page error: {e}")
        return f"Error: {str(e)}", 500


def _parse_date_range():
    today = datetime.now().date()
    try:
        date_to = datetime.strptime(request.args.get('to', today.strftime('%Y-%m-%d')), '%Y-%m-%d').date()
    except ValueError:
        date_to = today
    try:
        date_from = datetime.strptime(request.args.get('from', (today - timedelta(days=30)).strftime('%Y-%m-%d')), '%Y-%m-%d').date()
    except ValueError:
        date_from = today - timedelta(days=30)
    return date_from, date_to


def _get_org(cur, org_id):
    cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
    return cur.fetchone()


def _pdf_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='HeaderTitle', fontSize=16, textColor=colors.HexColor('#2c3e50'), fontName='Helvetica-Bold', spaceAfter=4))
    styles.add(ParagraphStyle(name='HeaderSub', fontSize=9, textColor=colors.HexColor('#4a5568'), spaceAfter=2))
    styles.add(ParagraphStyle(name='ReportTitle', fontSize=18, textColor=colors.HexColor('#27ae60'), fontName='Helvetica-Bold', alignment=TA_CENTER, spaceAfter=6, spaceBefore=10))
    styles.add(ParagraphStyle(name='ReportMeta', fontSize=10, textColor=colors.HexColor('#718096'), alignment=TA_CENTER, spaceAfter=14))
    styles.add(ParagraphStyle(name='SectionHead', fontSize=13, textColor=colors.HexColor('#2c3e50'), fontName='Helvetica-Bold', spaceBefore=10, spaceAfter=8))
    styles.add(ParagraphStyle(name='FooterNote', fontSize=8, textColor=colors.HexColor('#718096'), alignment=TA_CENTER))
    return styles


def _build_header(org, styles):
    flow = []
    business_name = (org.get('business_name') if org else None) or (org.get('name') if org else None) or 'Food Business'
    flow.append(Paragraph(business_name, styles['HeaderTitle']))
    if org:
        if org.get('business_address'):
            flow.append(Paragraph(org['business_address'].replace('\n', '<br/>'), styles['HeaderSub']))
        contact_bits = []
        if org.get('business_phone'): contact_bits.append(f"Tel: {org['business_phone']}")
        if org.get('business_email'): contact_bits.append(f"Email: {org['business_email']}")
        if contact_bits:
            flow.append(Paragraph(' &nbsp;&bull;&nbsp; '.join(contact_bits), styles['HeaderSub']))
        if org.get('food_business_registration'):
            flow.append(Paragraph(f"<b>FBO Registration:</b> {org['food_business_registration']}", styles['HeaderSub']))
    flow.append(Spacer(1, 0.3*cm))
    return flow


def _build_footer_text(org):
    parts = ['Generated by YieldGuard', datetime.now().strftime('%d %b %Y %H:%M')]
    if org and org.get('responsible_person'):
        parts.append(f"Responsible person: {org['responsible_person']}")
    return ' &nbsp;&bull;&nbsp; '.join(parts)


def _table_style(header_bg='#2c5282'):
    return TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor(header_bg)),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 9),
        ('FONTSIZE', (0,1), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,0), 8),
        ('TOPPADDING', (0,0), (-1,0), 6),
        ('BOTTOMPADDING', (0,1), (-1,-1), 5),
        ('TOPPADDING', (0,1), (-1,-1), 5),
        ('GRID', (0,0), (-1,-1), 0.25, colors.HexColor('#cbd5e0')),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f7fafc')]),
    ])


def _generate_pdf(title, org, date_from, date_to, content_builders):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                          leftMargin=1.5*cm, rightMargin=1.5*cm,
                          topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = _pdf_styles()
    story = []
    story.extend(_build_header(org, styles))
    story.append(Paragraph(title, styles['ReportTitle']))
    story.append(Paragraph(f"Period: {date_from.strftime('%d %b %Y')} to {date_to.strftime('%d %b %Y')}", styles['ReportMeta']))
    for builder in content_builders:
        story.extend(builder(styles))
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(_build_footer_text(org), styles['FooterNote']))
    doc.build(story)
    buffer.seek(0)
    return buffer


def _build_temps_section(org_id, date_from, date_to):
    def builder(styles):
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT tl.logged_at, e.name as equipment_name, e.equipment_type,
                   e.min_temp, e.max_temp, tl.temperature, tl.status,
                   tl.logged_by, tl.notes, tl.corrective_action,
                   tl.is_voided, tl.void_reason, tl.voided_by, tl.voided_at
            FROM haccp_temperature_logs tl
            JOIN haccp_equipment e ON tl.equipment_id = e.id
            WHERE tl.organization_id = %s
              AND tl.logged_at >= %s AND tl.logged_at < %s
            ORDER BY tl.logged_at DESC
        ''', (org_id, date_from, date_to + timedelta(days=1)))
        logs = cur.fetchall()
        cur.close()
        conn.close()

        flow = [Paragraph('Temperature Logs', styles['SectionHead'])]
        if not logs:
            flow.append(Paragraph('<i>No temperature logs in this period.</i>', styles['HeaderSub']))
            return flow

        data = [['Date/Time', 'Equipment', 'Range', 'Temp', 'Status', 'By', 'Notes / Action']]
        for log in logs:
            range_str = f"{log['min_temp']} to {log['max_temp']}" if log['min_temp'] is not None else '-'
            status = log['status'].upper()
            if log['is_voided']:
                status += ' (VOIDED)'
            note_bits = []
            if log['is_voided']:
                note_bits.append(f"Voided by {log['voided_by']}: {log['void_reason']}")
            else:
                if log['notes']: note_bits.append(log['notes'])
                if log['corrective_action']: note_bits.append(f"Action: {log['corrective_action']}")
            data.append([
                log['logged_at'].strftime('%d %b %Y %H:%M'),
                f"{log['equipment_name']} ({log['equipment_type']})",
                range_str,
                f"{log['temperature']}",
                status,
                log['logged_by'] or '-',
                Paragraph(' | '.join(note_bits) if note_bits else '-', styles['HeaderSub']),
            ])
        t = Table(data, colWidths=[2.8*cm, 3.5*cm, 2.5*cm, 1.5*cm, 2*cm, 2*cm, 4.5*cm], repeatRows=1)
        t.setStyle(_table_style())
        flow.append(t)
        return flow
    return builder


def _build_cleaning_section(org_id, date_from, date_to):
    def builder(styles):
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT cl.completed_at, ct.task_name, ct.area, ct.frequency,
                   ct.chemicals_used, cl.completed_by, cl.notes,
                   cl.is_voided, cl.void_reason, cl.voided_by
            FROM haccp_cleaning_logs cl
            JOIN haccp_cleaning_tasks ct ON cl.task_id = ct.id
            WHERE cl.organization_id = %s
              AND cl.completed_at >= %s AND cl.completed_at < %s
            ORDER BY cl.completed_at DESC
        ''', (org_id, date_from, date_to + timedelta(days=1)))
        logs = cur.fetchall()
        cur.close()
        conn.close()

        flow = [Paragraph('Cleaning Logs', styles['SectionHead'])]
        if not logs:
            flow.append(Paragraph('<i>No cleaning logs in this period.</i>', styles['HeaderSub']))
            return flow

        data = [['Date/Time', 'Task', 'Area', 'Frequency', 'Chemicals', 'By', 'Notes']]
        for log in logs:
            task = log['task_name']
            if log['is_voided']:
                task += ' (VOIDED)'
            note = f"Voided by {log['voided_by']}: {log['void_reason']}" if log['is_voided'] else (log['notes'] or '-')
            data.append([
                log['completed_at'].strftime('%d %b %Y %H:%M'),
                task,
                log['area'] or '-',
                log['frequency'] or '-',
                log['chemicals_used'] or '-',
                log['completed_by'] or '-',
                Paragraph(note, styles['HeaderSub']),
            ])
        t = Table(data, colWidths=[2.8*cm, 3.5*cm, 2.2*cm, 2*cm, 2.5*cm, 2*cm, 3.8*cm], repeatRows=1)
        t.setStyle(_table_style(header_bg='#27ae60'))
        flow.append(t)
        return flow
    return builder


def _build_deliveries_section(org_id, date_from, date_to):
    def builder(styles):
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT delivery_date, supplier_name, chilled_temp, frozen_temp,
                   packaging_ok, expiry_dates_ok, quality_ok, accepted,
                   notes, inspected_by
            FROM haccp_delivery_logs
            WHERE organization_id = %s
              AND delivery_date >= %s AND delivery_date <= %s
            ORDER BY delivery_date DESC
        ''', (org_id, date_from, date_to))
        deliveries = cur.fetchall()
        cur.close()
        conn.close()

        flow = [Paragraph('Delivery Inspections', styles['SectionHead'])]
        if not deliveries:
            flow.append(Paragraph('<i>No delivery inspections in this period.</i>', styles['HeaderSub']))
            return flow

        data = [['Date', 'Supplier', 'Chilled', 'Frozen', 'Pack', 'Dates', 'Quality', 'Accepted', 'By', 'Notes']]
        for d in deliveries:
            chilled = f"{d['chilled_temp']}" if d['chilled_temp'] is not None else '-'
            frozen = f"{d['frozen_temp']}" if d['frozen_temp'] is not None else '-'
            data.append([
                d['delivery_date'].strftime('%d %b %Y'),
                d['supplier_name'],
                chilled, frozen,
                'Y' if d['packaging_ok'] else 'N',
                'Y' if d['expiry_dates_ok'] else 'N',
                'Y' if d['quality_ok'] else 'N',
                'Yes' if d['accepted'] else 'No',
                d['inspected_by'] or '-',
                Paragraph(d['notes'] or '-', styles['HeaderSub']),
            ])
        t = Table(data, colWidths=[2.2*cm, 3*cm, 1.5*cm, 1.5*cm, 1*cm, 1*cm, 1.3*cm, 1.5*cm, 1.8*cm, 3.2*cm], repeatRows=1)
        t.setStyle(_table_style(header_bg='#d97706'))
        flow.append(t)
        return flow
    return builder


def _send_pdf(buffer, filename):
    response = make_response(buffer.read())
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


@app.route('/haccp/reports/temperatures.pdf')
@login_required
def report_temperatures_pdf():
    org_id = get_current_org_id()
    date_from, date_to = _parse_date_range()
    conn = get_db()
    cur = conn.cursor()
    org = _get_org(cur, org_id)
    cur.close()
    conn.close()
    buffer = _generate_pdf('Temperature Logs Report', org, date_from, date_to,
                           [_build_temps_section(org_id, date_from, date_to)])
    return _send_pdf(buffer, f"temperature-logs-{date_from}-to-{date_to}.pdf")


@app.route('/haccp/reports/cleaning.pdf')
@login_required
def report_cleaning_pdf():
    org_id = get_current_org_id()
    date_from, date_to = _parse_date_range()
    conn = get_db()
    cur = conn.cursor()
    org = _get_org(cur, org_id)
    cur.close()
    conn.close()
    buffer = _generate_pdf('Cleaning Logs Report', org, date_from, date_to,
                           [_build_cleaning_section(org_id, date_from, date_to)])
    return _send_pdf(buffer, f"cleaning-logs-{date_from}-to-{date_to}.pdf")


@app.route('/haccp/reports/deliveries.pdf')
@login_required
def report_deliveries_pdf():
    org_id = get_current_org_id()
    date_from, date_to = _parse_date_range()
    conn = get_db()
    cur = conn.cursor()
    org = _get_org(cur, org_id)
    cur.close()
    conn.close()
    buffer = _generate_pdf('Delivery Inspections Report', org, date_from, date_to,
                           [_build_deliveries_section(org_id, date_from, date_to)])
    return _send_pdf(buffer, f"deliveries-{date_from}-to-{date_to}.pdf")


@app.route('/haccp/reports/full.pdf')
@login_required
def report_full_pdf():
    org_id = get_current_org_id()
    date_from, date_to = _parse_date_range()
    conn = get_db()
    cur = conn.cursor()
    org = _get_org(cur, org_id)
    cur.close()
    conn.close()
    buffer = _generate_pdf('HACCP Compliance Report', org, date_from, date_to, [
        _build_temps_section(org_id, date_from, date_to),
        _build_cleaning_section(org_id, date_from, date_to),
        _build_deliveries_section(org_id, date_from, date_to),
    ])
    return _send_pdf(buffer, f"haccp-full-report-{date_from}-to-{date_to}.pdf")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
