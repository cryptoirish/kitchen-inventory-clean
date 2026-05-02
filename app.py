from flask import Flask, render_template, request, redirect, url_for, flash, session, make_response
import psycopg
from psycopg.rows import dict_row
import os
import traceback
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import csv
import json
from io import StringIO
import stripe
from io import BytesIO
import zipfile
import uuid
import base64
import urllib.request
import urllib.parse
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, KeepTogether, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.pdfgen import canvas
import qrcode

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


def get_all_allergens():
    """Returns the 14 statutory allergens as a list of dicts ordered by sort_order.
    Cached per-request via get_db (cheap query)."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT id, code, display_name, description, icon, sort_order FROM haccp_allergens ORDER BY sort_order')
    allergens = cur.fetchall()
    cur.close()
    conn.close()
    return allergens


def parse_allergen_codes(form_data):
    """Extracts allergen codes from a checkbox form (allergen_<code>=on)."""
    codes = []
    for key in form_data:
        if key.startswith('allergen_') and form_data.get(key) == 'on':
            codes.append(key[len('allergen_'):])
    return codes


def get_allergen_lookup():
    """Returns dict keyed by code -> {display_name, icon, description}.
    Useful for templates rendering allergen badges from a stored code list."""
    allergens = get_all_allergens()
    return {a['code']: a for a in allergens}


def get_recipe_allergens(recipe_id, org_id):
    """Returns a dict with 4 sets:
        from_ingredients: codes derived from recipe_ingredients -> items.allergens
        from_equipment:   codes from equipment used to prepare this recipe
        manual:           codes manually added to the recipe
        combined:         union of all three (what gets declared on PPDS)
    Each value is a sorted list of unique codes."""
    conn = get_db()
    cur = conn.cursor()

    # Ingredients
    cur.execute('''
        SELECT i.allergens
        FROM recipe_ingredients ri
        JOIN items i ON i.id = ri.inventory_item_id
        WHERE ri.recipe_id = %s AND i.organization_id = %s
    ''', (recipe_id, org_id))
    from_ingredients = set()
    for row in cur.fetchall():
        raw = row.get('allergens')
        if raw is None:
            continue
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                raw = []
        for code in raw or []:
            from_ingredients.add(code)

    # Equipment cross-contamination
    cur.execute('''
        SELECT DISTINCT ea.allergen_code
        FROM haccp_recipe_equipment re
        JOIN haccp_equipment_allergens ea ON ea.equipment_id = re.equipment_id
        WHERE re.recipe_id = %s AND re.organization_id = %s
    ''', (recipe_id, org_id))
    from_equipment = {row['allergen_code'] for row in cur.fetchall()}

    # Manual
    cur.execute('SELECT manual_allergens FROM recipes WHERE id = %s AND organization_id = %s', (recipe_id, org_id))
    rec = cur.fetchone()
    manual = set()
    if rec and rec.get('manual_allergens'):
        raw = rec['manual_allergens']
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                raw = []
        for code in raw or []:
            manual.add(code)

    cur.close()
    conn.close()

    return {
        'from_ingredients': sorted(from_ingredients),
        'from_equipment': sorted(from_equipment),
        'manual': sorted(manual),
        'combined': sorted(from_ingredients | from_equipment | manual),
    }


def get_equipment_allergens(equipment_id, org_id):
    """Returns list of allergen codes flagged on a piece of equipment."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT allergen_code FROM haccp_equipment_allergens
        WHERE equipment_id = %s AND organization_id = %s
    ''', (equipment_id, org_id))
    codes = [row['allergen_code'] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return codes

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
                session['user_email'] = user['email']
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

        cur.execute("SELECT COUNT(*) as count FROM haccp_temperature_logs WHERE organization_id = %s AND logged_at >= NOW() - INTERVAL '24 hours'", (org_id,))
        temps_today = cur.fetchone()['count']
        
        cur.close()
        conn.close()

        # Compliance alert summary for the homepage banner
        compliance_alerts = get_compliance_alerts(org_id)
        critical_count = len(compliance_alerts['critical'])
        warning_count = len(compliance_alerts['warning'])
        
        return render_template('dashboard.html', 
                             org=org,
                             plans=PLANS,
                             total_items=total_items,
                             low_stock_count=low_stock_count,
                             total_value=float(total_value),
                             total_recipes=total_recipes,
                             temps_today=temps_today,
                             critical_count=critical_count,
                             warning_count=warning_count,
                             user_first_name=session.get('user_name', 'there').split(' ')[0],
                             today_string=datetime.now().strftime('%A, %d %B'))
    except Exception as e:
        print(f"Dashboard error: {e}")
        traceback.print_exc()
        return render_template('dashboard.html', 
                             org=None,
                             plans=PLANS,
                             total_items=0,
                             low_stock_count=0,
                             total_value=0,
                             total_recipes=0,
                             temps_today=0,
                             critical_count=0,
                             warning_count=0,
                             user_first_name=session.get('user_name', 'there').split(' ')[0],
                             today_string=datetime.now().strftime('%A, %d %B'))

@app.route('/inventory')
@login_required
def inventory():
    try:
        org_id = get_current_org_id()
        search = request.args.get('search', '')
        category_filter = request.args.get('category', '')
        allergen_filter = request.args.get('allergen', '')
        
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
        
        if allergen_filter:
            query += ' AND allergens @> %s::jsonb'
            params.append(f'["{allergen_filter}"]')
        
        query += ' ORDER BY name'
        
        cur.execute(query, params)
        items = cur.fetchall()
        
        # Normalise the allergens JSON field for each item — Jinja can't parse JSON cleanly
        for item in items:
            raw = item.get('allergens')
            if raw is None:
                item['allergens_list'] = []
            elif isinstance(raw, str):
                try:
                    item['allergens_list'] = json.loads(raw)
                except (ValueError, TypeError):
                    item['allergens_list'] = []
            else:
                item['allergens_list'] = list(raw)
        
        cur.execute('SELECT DISTINCT category FROM items WHERE organization_id = %s AND category IS NOT NULL ORDER BY category', (org_id,))
        categories = [row['category'] for row in cur.fetchall()]
        
        cur.execute('SELECT SUM(stock * cost) as total FROM items WHERE organization_id = %s', (org_id,))
        result = cur.fetchone()
        total_value = float(result['total']) if result['total'] else 0
        
        cur.close()
        conn.close()
        
        allergens = get_all_allergens()
        allergen_lookup = {a['code']: a for a in allergens}
        
        return render_template('inventory.html', 
                             items=items, 
                             categories=categories,
                             search=search,
                             category_filter=category_filter,
                             allergen_filter=allergen_filter,
                             total_value=total_value,
                             allergens=allergens,
                             allergen_lookup=allergen_lookup)
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
            allergen_codes = parse_allergen_codes(request.form)
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO items (organization_id, name, category, stock, reorder_point, cost, unit, allergens, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ''', (
                org_id,
                request.form['name'],
                request.form['category'],
                request.form['stock'],
                request.form['reorder'],
                request.form['cost'],
                request.form['unit'],
                json.dumps(allergen_codes),
                datetime.now()
            ))
            conn.commit()
            cur.close()
            conn.close()
            flash('Item added successfully!', 'success')
            return redirect('/inventory')
        except Exception as e:
            print(f"Add item error: {e}")
            traceback.print_exc()
            flash('Error adding item', 'danger')
    allergens = get_all_allergens()
    return render_template('add.html', allergens=allergens)


@app.route('/inventory/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_item(id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        if request.method == 'POST':
            allergen_codes = parse_allergen_codes(request.form)
            cur.execute('''
                UPDATE items
                SET name = %s, category = %s, stock = %s, reorder_point = %s,
                    cost = %s, unit = %s, allergens = %s::jsonb, updated_at = %s
                WHERE id = %s AND organization_id = %s
            ''', (
                request.form['name'],
                request.form['category'],
                request.form['stock'],
                request.form['reorder'],
                request.form['cost'],
                request.form['unit'],
                json.dumps(allergen_codes),
                datetime.now(),
                id,
                org_id
            ))
            conn.commit()
            cur.close()
            conn.close()
            flash('Item updated!', 'success')
            return redirect('/inventory')

        cur.execute('SELECT * FROM items WHERE id = %s AND organization_id = %s', (id, org_id))
        item = cur.fetchone()
        cur.close()
        conn.close()

        if not item:
            flash('Item not found', 'danger')
            return redirect('/inventory')

        allergens = get_all_allergens()
        # Parse the JSONB allergens field for the template
        item_allergens = item.get('allergens') or []
        if isinstance(item_allergens, str):
            item_allergens = json.loads(item_allergens)

        return render_template('edit_item.html', item=item, allergens=allergens, item_allergens=item_allergens)
    except Exception as e:
        print(f"Edit item error: {e}")
        traceback.print_exc()
        flash('Error loading item', 'danger')
        return redirect('/inventory')

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

        # Compliance / HACCP alerts (shared with dashboard)
        compliance = get_compliance_alerts(org_id)
        haccp_alerts = compliance['critical'] + compliance['warning'] + compliance['info']

        return render_template('alerts.html',
                               items=low_stock,
                               haccp_alerts=haccp_alerts,
                               critical_count=len(compliance['critical']),
                               warning_count=len(compliance['warning']),
                               info_count=len(compliance['info']))
    except Exception as e:
        print(f"Alerts error: {e}")
        traceback.print_exc()
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


@app.route('/recipes/<int:id>/quick-edit', methods=['POST'])
@login_required
def quick_edit_recipe(id):
    """Inline edit of common recipe fields from the detail page."""
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE recipes
            SET name = %s, category = %s, selling_price = %s,
                portion_size = %s, servings = %s, updated_at = %s
            WHERE id = %s AND organization_id = %s
        ''', (
            request.form['name'],
            request.form.get('category', ''),
            float(request.form['selling_price']) if request.form.get('selling_price') else 0,
            request.form.get('portion_size', ''),
            int(request.form['servings']) if request.form.get('servings') else 1,
            datetime.now(),
            id,
            org_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Recipe updated.', 'success')
        return redirect(f'/recipes/{id}')
    except Exception as e:
        print(f"Quick edit recipe error: {e}")
        traceback.print_exc()
        flash('Error updating recipe', 'danger')
        return redirect(f'/recipes/{id}')


@app.route('/recipes/<int:id>/edit', methods=['GET', 'POST'])
@login_required
def edit_recipe(id):
    """Full edit page — all recipe fields including instructions and notes."""
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        if request.method == 'POST':
            # Parse VAT rate (default 20.00 for hot food)
            try:
                vat_rate_val = float(request.form.get('vat_rate', '20'))
            except ValueError:
                vat_rate_val = 20.00
            cur.execute('''
                UPDATE recipes
                SET name = %s, category = %s, selling_price = %s,
                    portion_size = %s, servings = %s,
                    instructions = %s, notes = %s,
                    vat_rate = %s, is_takeaway_cold = %s,
                    updated_at = %s
                WHERE id = %s AND organization_id = %s
            ''', (
                request.form['name'],
                request.form.get('category', ''),
                float(request.form['selling_price']) if request.form.get('selling_price') else 0,
                request.form.get('portion_size', ''),
                int(request.form['servings']) if request.form.get('servings') else 1,
                request.form.get('instructions', ''),
                request.form.get('notes', ''),
                vat_rate_val,
                request.form.get('is_takeaway_cold') == 'on',
                datetime.now(),
                id,
                org_id
            ))
            conn.commit()
            cur.close()
            conn.close()
            flash('Recipe updated.', 'success')
            return redirect(f'/recipes/{id}')

        cur.execute('SELECT * FROM recipes WHERE id = %s AND organization_id = %s', (id, org_id))
        recipe = cur.fetchone()
        cur.close()
        conn.close()

        if not recipe:
            flash('Recipe not found', 'danger')
            return redirect('/recipes')

        return render_template('edit_recipe.html', recipe=recipe)
    except Exception as e:
        print(f"Edit recipe error: {e}")
        traceback.print_exc()
        flash('Error loading recipe', 'danger')
        return redirect(f'/recipes/{id}')


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

        # All active equipment for the "link equipment" picker
        cur.execute('SELECT id, name, equipment_type FROM haccp_equipment WHERE organization_id = %s AND is_active = true ORDER BY name', (org_id,))
        all_equipment = cur.fetchall()

        # Equipment currently linked to this recipe
        cur.execute('''
            SELECT e.id, e.name, e.equipment_type
            FROM haccp_recipe_equipment re
            JOIN haccp_equipment e ON e.id = re.equipment_id
            WHERE re.recipe_id = %s AND re.organization_id = %s
            ORDER BY e.name
        ''', (id, org_id))
        linked_equipment = cur.fetchall()
        
        cur.close()
        conn.close()

        # Allergen data
        allergen_data = get_recipe_allergens(id, org_id)
        all_allergens = get_all_allergens()
        allergen_lookup = {a['code']: a for a in all_allergens}
        
        return render_template('recipe_detail.html',
                             recipe=recipe,
                             ingredients=ingredients,
                             total_cost=total_cost,
                             food_cost_percent=food_cost_percent,
                             gross_profit=gross_profit,
                             all_items=all_items,
                             all_equipment=all_equipment,
                             linked_equipment=linked_equipment,
                             allergen_data=allergen_data,
                             all_allergens=all_allergens,
                             allergen_lookup=allergen_lookup)
    except Exception as e:
        print(f"Recipe detail error: {e}")
        traceback.print_exc()
        return redirect('/recipes')

@app.route('/recipes/<int:recipe_id>/create-and-add-ingredient', methods=['POST'])
@login_required
def create_and_add_ingredient(recipe_id):
    """Create a new inventory item and immediately add it to the recipe."""
    try:
        org_id = get_current_org_id()
        # Verify recipe belongs to this org
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT id FROM recipes WHERE id = %s AND organization_id = %s', (recipe_id, org_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            flash('Recipe not found', 'danger')
            return redirect('/recipes')

        # Create the inventory item
        allergen_codes = parse_allergen_codes(request.form)
        cur.execute('''
            INSERT INTO items (organization_id, name, category, stock, reorder_point, cost, unit, allergens, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            RETURNING id
        ''', (
            org_id,
            request.form['name'].strip(),
            request.form.get('category', '').strip() or None,
            0,  # stock starts at 0
            0,  # reorder point starts at 0
            float(request.form['cost']) if request.form.get('cost') else 0,
            request.form['unit'].strip(),
            json.dumps(allergen_codes),
            datetime.now()
        ))
        new_item_id = cur.fetchone()['id']

        # Add it to the recipe
        cur.execute('''
            INSERT INTO recipe_ingredients (recipe_id, inventory_item_id, quantity, notes)
            VALUES (%s, %s, %s, %s)
        ''', (recipe_id, new_item_id, request.form['quantity'], ''))

        conn.commit()
        cur.close()
        conn.close()
        flash(f"Created '{request.form['name'].strip()}' and added to recipe.", 'success')
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Create-and-add ingredient error: {e}")
        traceback.print_exc()
        flash('Error creating ingredient', 'danger')
        return redirect(f'/recipes/{recipe_id}')


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


# ============================================================
# MENU PLANNING — group recipes into menus, costing, GP analysis
# ============================================================

def _menu_costing(menu_id, org_id):
    """Return list of menu items with full costing breakdown.
    Each row contains: id, recipe_id, recipe_name, ingredient_cost,
    selling_price (menu override or recipe default), vat_rate,
    selling_price_ex_vat, vat_amount, gross_profit, food_cost_percent,
    gp_percent, weekly_sales, weekly_revenue_ex_vat, weekly_gp.
    """
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT mi.id, mi.recipe_id, mi.menu_selling_price, mi.estimated_weekly_sales,
               mi.notes, mi.sort_order,
               r.name as recipe_name, r.selling_price as recipe_selling_price,
               r.vat_rate, r.is_takeaway_cold,
               COALESCE(SUM(ri.quantity * i.cost), 0) as ingredient_cost
        FROM menu_items mi
        JOIN recipes r ON r.id = mi.recipe_id
        LEFT JOIN recipe_ingredients ri ON ri.recipe_id = r.id
        LEFT JOIN items i ON i.id = ri.inventory_item_id
        WHERE mi.menu_id = %s AND r.organization_id = %s
        GROUP BY mi.id, mi.recipe_id, mi.menu_selling_price, mi.estimated_weekly_sales,
                 mi.notes, mi.sort_order, r.name, r.selling_price, r.vat_rate, r.is_takeaway_cold
        ORDER BY mi.sort_order, r.name
    ''', (menu_id, org_id))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    results = []
    for r in rows:
        cost = float(r['ingredient_cost'] or 0)
        # Use menu override if set, otherwise recipe's default
        price_inc_vat = float(r['menu_selling_price']) if r['menu_selling_price'] is not None else float(r['recipe_selling_price'] or 0)
        vat_rate = float(r['vat_rate'] if r['vat_rate'] is not None else 20)
        # Strip VAT to get the ex-VAT price
        if vat_rate > 0:
            price_ex_vat = price_inc_vat / (1 + vat_rate / 100)
        else:
            price_ex_vat = price_inc_vat
        vat_amount = price_inc_vat - price_ex_vat
        gross_profit = price_ex_vat - cost
        food_cost_percent = (cost / price_ex_vat * 100) if price_ex_vat > 0 else 0
        gp_percent = (gross_profit / price_ex_vat * 100) if price_ex_vat > 0 else 0
        weekly_sales = int(r['estimated_weekly_sales'] or 0)
        weekly_revenue_ex_vat = price_ex_vat * weekly_sales
        weekly_gp = gross_profit * weekly_sales

        results.append({
            'id': r['id'],
            'recipe_id': r['recipe_id'],
            'recipe_name': r['recipe_name'],
            'ingredient_cost': cost,
            'selling_price_inc_vat': price_inc_vat,
            'selling_price_ex_vat': price_ex_vat,
            'vat_rate': vat_rate,
            'vat_amount': vat_amount,
            'is_takeaway_cold': r['is_takeaway_cold'],
            'gross_profit': gross_profit,
            'food_cost_percent': food_cost_percent,
            'gp_percent': gp_percent,
            'weekly_sales': weekly_sales,
            'weekly_revenue_ex_vat': weekly_revenue_ex_vat,
            'weekly_gp': weekly_gp,
            'notes': r['notes'],
            'sort_order': r['sort_order'],
            'is_using_override': r['menu_selling_price'] is not None,
        })
    return results


@app.route('/menus')
@login_required
def menus():
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT m.*, COUNT(mi.id) as item_count
            FROM menus m
            LEFT JOIN menu_items mi ON mi.menu_id = m.id
            WHERE m.organization_id = %s
            GROUP BY m.id
            ORDER BY m.is_active DESC, m.name
        ''', (org_id,))
        all_menus = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('menus.html', menus=all_menus)
    except Exception as e:
        print(f"Menus list error: {e}")
        traceback.print_exc()
        return f"Error loading menus: {str(e)}", 500


@app.route('/menus/new', methods=['GET', 'POST'])
@login_required
def menu_new():
    if request.method == 'GET':
        return render_template('menu_form.html', menu=None)
    try:
        org_id = get_current_org_id()
        name = request.form.get('name', '').strip()
        if not name:
            flash('Menu name is required.', 'danger')
            return redirect('/menus/new')
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO menus (organization_id, name, description, is_active)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        ''', (org_id, name,
              request.form.get('description', '').strip() or None,
              request.form.get('is_active') == 'on'))
        new_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        flash(f"Menu '{name}' created.", 'success')
        return redirect(f'/menus/{new_id}')
    except Exception as e:
        print(f"Menu new error: {e}")
        traceback.print_exc()
        flash('Error creating menu.', 'danger')
        return redirect('/menus')


@app.route('/menus/<int:menu_id>')
@login_required
def menu_detail(menu_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        menu = cur.fetchone()
        if not menu:
            cur.close()
            conn.close()
            flash('Menu not found.', 'danger')
            return redirect('/menus')

        # Recipes already on this menu
        cur.execute('SELECT recipe_id FROM menu_items WHERE menu_id = %s', (menu_id,))
        existing_ids = {r['recipe_id'] for r in cur.fetchall()}

        # Recipes available to add (not already on the menu)
        cur.execute('''
            SELECT id, name, category, selling_price
            FROM recipes
            WHERE organization_id = %s
            ORDER BY name
        ''', (org_id,))
        all_recipes = cur.fetchall()
        available = [r for r in all_recipes if r['id'] not in existing_ids]

        cur.close()
        conn.close()

        items = _menu_costing(menu_id, org_id)

        # Menu summary
        total_weekly_revenue = sum(i['weekly_revenue_ex_vat'] for i in items)
        total_weekly_gp = sum(i['weekly_gp'] for i in items)
        items_with_sales = [i for i in items if i['weekly_sales'] > 0]
        avg_gp_pct_weighted = (total_weekly_gp / total_weekly_revenue * 100) if total_weekly_revenue > 0 else 0
        avg_gp_pct_simple = (sum(i['gp_percent'] for i in items) / len(items)) if items else 0

        return render_template('menu_detail.html',
                               menu=menu,
                               items=items,
                               available_recipes=available,
                               item_count=len(items),
                               total_weekly_revenue=total_weekly_revenue,
                               total_weekly_gp=total_weekly_gp,
                               avg_gp_pct_weighted=avg_gp_pct_weighted,
                               avg_gp_pct_simple=avg_gp_pct_simple)
    except Exception as e:
        print(f"Menu detail error: {e}")
        traceback.print_exc()
        return f"Error loading menu: {str(e)}", 500


@app.route('/menus/<int:menu_id>/edit', methods=['GET', 'POST'])
@login_required
def menu_edit(menu_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        if request.method == 'POST':
            cur.execute('''
                UPDATE menus
                SET name = %s, description = %s, is_active = %s, updated_at = NOW()
                WHERE id = %s AND organization_id = %s
            ''', (
                request.form.get('name', '').strip(),
                request.form.get('description', '').strip() or None,
                request.form.get('is_active') == 'on',
                menu_id, org_id
            ))
            conn.commit()
            cur.close()
            conn.close()
            flash('Menu updated.', 'success')
            return redirect(f'/menus/{menu_id}')

        cur.execute('SELECT * FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        menu = cur.fetchone()
        cur.close()
        conn.close()
        if not menu:
            flash('Menu not found.', 'danger')
            return redirect('/menus')
        return render_template('menu_form.html', menu=menu)
    except Exception as e:
        print(f"Menu edit error: {e}")
        traceback.print_exc()
        flash('Error editing menu.', 'danger')
        return redirect('/menus')


@app.route('/menus/<int:menu_id>/delete', methods=['POST'])
@login_required
def menu_delete(menu_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Menu deleted.', 'success')
    except Exception as e:
        print(f"Menu delete error: {e}")
        flash('Error deleting menu.', 'danger')
    return redirect('/menus')


@app.route('/menus/<int:menu_id>/add-recipe', methods=['POST'])
@login_required
def menu_add_recipe(menu_id):
    try:
        org_id = get_current_org_id()
        recipe_id = request.form.get('recipe_id')
        if not recipe_id:
            flash('Pick a recipe to add.', 'danger')
            return redirect(f'/menus/{menu_id}')
        conn = get_db()
        cur = conn.cursor()
        # Verify both belong to this org
        cur.execute('SELECT id FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            flash('Menu not found.', 'danger')
            return redirect('/menus')
        cur.execute('SELECT id FROM recipes WHERE id = %s AND organization_id = %s', (recipe_id, org_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            flash('Recipe not found.', 'danger')
            return redirect(f'/menus/{menu_id}')
        # Insert (ignore duplicate)
        cur.execute('''
            INSERT INTO menu_items (menu_id, recipe_id, sort_order)
            VALUES (%s, %s, COALESCE((SELECT MAX(sort_order) + 1 FROM menu_items WHERE menu_id = %s), 0))
            ON CONFLICT (menu_id, recipe_id) DO NOTHING
        ''', (menu_id, recipe_id, menu_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Recipe added to menu.', 'success')
    except Exception as e:
        print(f"Menu add recipe error: {e}")
        traceback.print_exc()
        flash('Error adding recipe to menu.', 'danger')
    return redirect(f'/menus/{menu_id}')


@app.route('/menus/<int:menu_id>/items/<int:item_id>/update', methods=['POST'])
@login_required
def menu_item_update(menu_id, item_id):
    """Update menu-specific fields on a menu item: override price, weekly sales, notes."""
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        # Verify ownership via the menu
        cur.execute('SELECT id FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            flash('Menu not found.', 'danger')
            return redirect('/menus')

        # Parse the optional override price (blank = use recipe default)
        price_str = request.form.get('menu_selling_price', '').strip()
        if price_str:
            try:
                menu_price = float(price_str)
            except ValueError:
                menu_price = None
        else:
            menu_price = None

        weekly_sales_str = request.form.get('estimated_weekly_sales', '').strip()
        weekly_sales = int(weekly_sales_str) if weekly_sales_str.isdigit() else 0

        cur.execute('''
            UPDATE menu_items
            SET menu_selling_price = %s,
                estimated_weekly_sales = %s,
                notes = %s
            WHERE id = %s AND menu_id = %s
        ''', (menu_price, weekly_sales,
              request.form.get('notes', '').strip() or None,
              item_id, menu_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Menu item updated.', 'success')
    except Exception as e:
        print(f"Menu item update error: {e}")
        traceback.print_exc()
        flash('Error updating item.', 'danger')
    return redirect(f'/menus/{menu_id}')


@app.route('/menus/<int:menu_id>/items/<int:item_id>/remove', methods=['POST'])
@login_required
def menu_item_remove(menu_id, item_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT id FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            flash('Menu not found.', 'danger')
            return redirect('/menus')
        cur.execute('DELETE FROM menu_items WHERE id = %s AND menu_id = %s', (item_id, menu_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Recipe removed from menu.', 'success')
    except Exception as e:
        print(f"Menu item remove error: {e}")
        flash('Error removing item.', 'danger')
    return redirect(f'/menus/{menu_id}')


@app.route('/menus/<int:menu_id>/items/<int:item_id>/move', methods=['POST'])
@login_required
def menu_item_move(menu_id, item_id):
    """Move a menu item up or down in the order. direction = 'up' or 'down'."""
    try:
        org_id = get_current_org_id()
        direction = request.form.get('direction', 'up')
        conn = get_db()
        cur = conn.cursor()
        # Verify menu ownership
        cur.execute('SELECT id FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            return redirect('/menus')

        # Fetch all items ordered
        cur.execute('SELECT id, sort_order FROM menu_items WHERE menu_id = %s ORDER BY sort_order, id', (menu_id,))
        rows = cur.fetchall()
        # Find target index
        idx = next((i for i, r in enumerate(rows) if r['id'] == item_id), None)
        if idx is None:
            cur.close()
            conn.close()
            return redirect(f'/menus/{menu_id}')

        if direction == 'up' and idx > 0:
            swap_with = idx - 1
        elif direction == 'down' and idx < len(rows) - 1:
            swap_with = idx + 1
        else:
            cur.close()
            conn.close()
            return redirect(f'/menus/{menu_id}')

        # Re-number all items so sort_order is contiguous, then swap
        new_order = list(range(len(rows)))
        new_order[idx], new_order[swap_with] = new_order[swap_with], new_order[idx]
        for new_pos, original_idx in enumerate(new_order):
            cur.execute('UPDATE menu_items SET sort_order = %s WHERE id = %s',
                        (new_pos, rows[original_idx]['id']))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Menu item move error: {e}")
        traceback.print_exc()
    return redirect(f'/menus/{menu_id}')


@app.route('/menus/<int:menu_id>/report.pdf')
@login_required
def menu_report_pdf(menu_id):
    """Generate a printable menu costing report (PDF)."""
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM menus WHERE id = %s AND organization_id = %s', (menu_id, org_id))
        menu = cur.fetchone()
        if not menu:
            cur.close()
            conn.close()
            flash('Menu not found.', 'danger')
            return redirect('/menus')
        org = _get_org(cur, org_id)
        cur.close()
        conn.close()

        items = _menu_costing(menu_id, org_id)

        # Build PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4,
                                topMargin=15*mm, bottomMargin=15*mm,
                                leftMargin=15*mm, rightMargin=15*mm)
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle('title', parent=styles['Title'],
                                      fontSize=18, textColor=colors.HexColor('#0A0A0A'),
                                      alignment=TA_LEFT, spaceAfter=6)
        h2_style = ParagraphStyle('h2', parent=styles['Heading2'],
                                   fontSize=13, textColor=colors.HexColor('#0A0A0A'),
                                   spaceBefore=12, spaceAfter=8)
        body_style = ParagraphStyle('body', parent=styles['BodyText'],
                                     fontSize=10, textColor=colors.HexColor('#3A3A3A'))
        small_style = ParagraphStyle('small', parent=styles['BodyText'],
                                      fontSize=9, textColor=colors.HexColor('#6B6860'))

        story = []
        # Header
        biz_name = (org.get('business_name') if org else None) or 'Food Business'
        story.append(Paragraph(biz_name, ParagraphStyle('biz', fontSize=11,
                                                         textColor=colors.HexColor('#6B6860'),
                                                         spaceAfter=2)))
        story.append(Paragraph(f"Menu costing report — {menu['name']}", title_style))
        if menu.get('description'):
            story.append(Paragraph(menu['description'], small_style))
        story.append(Paragraph(f"Generated {datetime.now().strftime('%d %B %Y · %H:%M')}", small_style))
        story.append(Spacer(1, 6*mm))

        # Summary stats
        total_revenue = sum(i['weekly_revenue_ex_vat'] for i in items)
        total_gp = sum(i['weekly_gp'] for i in items)
        weighted_gp_pct = (total_gp / total_revenue * 100) if total_revenue > 0 else 0
        avg_simple = (sum(i['gp_percent'] for i in items) / len(items)) if items else 0

        summary_data = [
            ['Recipes on menu', str(len(items))],
            ['Average GP% (simple)', f"{avg_simple:.1f}%"],
        ]
        if total_revenue > 0:
            summary_data.extend([
                ['Forecast weekly revenue (ex-VAT)', f"£{total_revenue:.2f}"],
                ['Forecast weekly gross profit', f"£{total_gp:.2f}"],
                ['Weighted GP%', f"{weighted_gp_pct:.1f}%"],
            ])
        summary_table = Table(summary_data, colWidths=[80*mm, 60*mm])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#FAFAF7')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#0A0A0A')),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#E8E5DD')),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica-Bold'),
        ]))
        story.append(summary_table)
        story.append(Spacer(1, 4*mm))

        story.append(Paragraph("UK industry targets: healthy GP is 65–72% (food cost 28–35%). "
                                "Below 50% GP indicates loss-making dishes.", small_style))
        story.append(Spacer(1, 4*mm))

        # Per-dish costing table
        story.append(Paragraph('Per-dish costing', h2_style))
        if items:
            header = ['Recipe', 'Cost', 'Sell ex-VAT', 'VAT', 'Sell inc-VAT', 'GP £', 'GP%']
            data = [header]
            for it in items:
                gp_str = f"{it['gp_percent']:.1f}%" if it['gp_percent'] > 0 else '—'
                data.append([
                    it['recipe_name'],
                    f"£{it['ingredient_cost']:.2f}",
                    f"£{it['selling_price_ex_vat']:.2f}",
                    f"{it['vat_rate']:.0f}%",
                    f"£{it['selling_price_inc_vat']:.2f}",
                    f"£{it['gross_profit']:.2f}",
                    gp_str,
                ])
            costing_table = Table(data, colWidths=[55*mm, 18*mm, 22*mm, 14*mm, 22*mm, 18*mm, 18*mm])
            ts = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0A0A0A')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#D6D2C7')),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ])
            # Colour-code the GP% column
            for i, it in enumerate(items, start=1):
                if it['gp_percent'] >= 65:
                    ts.add('BACKGROUND', (6, i), (6, i), colors.HexColor('#4A6B45'))
                    ts.add('TEXTCOLOR', (6, i), (6, i), colors.white)
                    ts.add('FONTNAME', (6, i), (6, i), 'Helvetica-Bold')
                elif it['gp_percent'] >= 50:
                    ts.add('BACKGROUND', (6, i), (6, i), colors.HexColor('#D97706'))
                    ts.add('TEXTCOLOR', (6, i), (6, i), colors.white)
                    ts.add('FONTNAME', (6, i), (6, i), 'Helvetica-Bold')
                elif it['gp_percent'] > 0:
                    ts.add('BACKGROUND', (6, i), (6, i), colors.HexColor('#7C2D2D'))
                    ts.add('TEXTCOLOR', (6, i), (6, i), colors.white)
                    ts.add('FONTNAME', (6, i), (6, i), 'Helvetica-Bold')
            costing_table.setStyle(ts)
            story.append(costing_table)
        else:
            story.append(Paragraph('No recipes on this menu yet.', body_style))

        # Sales forecasting (only if any sales numbers entered)
        items_with_sales = [i for i in items if i['weekly_sales'] > 0]
        if items_with_sales:
            story.append(Spacer(1, 6*mm))
            story.append(Paragraph('Weekly sales forecast', h2_style))
            forecast_data = [['Recipe', 'Sold/wk', 'Revenue ex-VAT', 'GP £']]
            for it in items_with_sales:
                forecast_data.append([
                    it['recipe_name'],
                    str(it['weekly_sales']),
                    f"£{it['weekly_revenue_ex_vat']:.2f}",
                    f"£{it['weekly_gp']:.2f}",
                ])
            forecast_data.append(['TOTAL', '',
                                   f"£{total_revenue:.2f}",
                                   f"£{total_gp:.2f}"])
            forecast_table = Table(forecast_data, colWidths=[80*mm, 22*mm, 35*mm, 30*mm])
            forecast_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0A0A0A')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#EEF1ED')),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#D6D2C7')),
            ]))
            story.append(forecast_table)

        # Footer
        story.append(Spacer(1, 8*mm))
        story.append(Paragraph('All figures shown ex-VAT for proper margin analysis. Cost based on current ingredient prices in inventory.', small_style))
        if org and org.get('food_business_registration'):
            story.append(Paragraph(f"FBO: {org['food_business_registration']}", small_style))

        doc.build(story)
        buffer.seek(0)

        # Sanitise menu name for filename
        safe_name = ''.join(c if c.isalnum() else '-' for c in menu['name']).lower().strip('-')
        filename = f"menu-{safe_name}-{datetime.now().strftime('%Y-%m-%d')}.pdf"
        return _send_pdf(buffer, filename)
    except Exception as e:
        print(f"Menu PDF error: {e}")
        traceback.print_exc()
        flash('Error generating PDF.', 'danger')
        return redirect(f'/menus/{menu_id}')


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


@app.route('/recipes/<int:id>/ppds-settings', methods=['POST'])
@login_required
def save_ppds_settings(id):
    """Save PPDS-specific fields (is_ppds flag, storage instructions, use-by days)."""
    try:
        org_id = get_current_org_id()
        is_ppds = request.form.get('is_ppds') == 'on'
        storage = request.form.get('ppds_storage_instructions', '').strip() or None
        use_by_days_raw = request.form.get('ppds_use_by_days', '').strip()
        try:
            use_by_days = int(use_by_days_raw) if use_by_days_raw else None
        except ValueError:
            use_by_days = None
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE recipes
            SET is_ppds = %s, ppds_storage_instructions = %s, ppds_use_by_days = %s, updated_at = %s
            WHERE id = %s AND organization_id = %s
        ''', (is_ppds, storage, use_by_days, datetime.now(), id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('PPDS settings saved.', 'success')
        return redirect(f'/recipes/{id}')
    except Exception as e:
        print(f"Save PPDS settings error: {e}")
        traceback.print_exc()
        flash('Error saving PPDS settings', 'danger')
        return redirect(f'/recipes/{id}')


# ====== PPDS LABEL GENERATOR ======

def _generate_qr_image(url, box_size=4):
    """Returns a BytesIO containing a PNG QR code for the URL."""
    qr = qrcode.QRCode(version=None, error_correction=qrcode.constants.ERROR_CORRECT_M,
                       box_size=box_size, border=2)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color='black', back_color='white')
    buf = BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return buf


def _build_ingredient_paragraph(ingredients_with_allergens, allergen_lookup, base_font_size=8):
    """Builds an HTML-ish ingredient list string for ReportLab Paragraph.
    Statutory allergens get <b><u>bold underlined</u></b> per Natasha's Law.
    ingredients_with_allergens = list of (item_name, allergen_codes_list) tuples."""
    parts = []
    for name, allergen_codes in ingredients_with_allergens:
        # If this ingredient has any statutory allergens, emphasise the parts
        if allergen_codes:
            # Build display: "Wheat Flour (CONTAINS: gluten)" with bold/underline on the allergen names
            allergen_display_names = [allergen_lookup[c]['display_name'] for c in allergen_codes if c in allergen_lookup]
            if allergen_display_names:
                emphasised = ', '.join(f'<b><u>{n}</u></b>' for n in allergen_display_names)
                parts.append(f'{name} ({emphasised})')
            else:
                parts.append(name)
        else:
            parts.append(name)
    return ', '.join(parts)


def _build_ppds_label_story(recipe, ingredients, allergen_data, allergen_lookup, qr_url, label_size, styles):
    """Returns a list of flowables for one PPDS label.
    label_size: 'a6', 'a7', or 'thermal' — controls font sizes and layout density."""
    # Tune sizes per label format
    if label_size == 'a6':
        title_size = 14
        body_size = 9
        qr_box = 4
    elif label_size == 'a7':
        title_size = 11
        body_size = 7
        qr_box = 3
    else:  # thermal
        title_size = 11
        body_size = 8
        qr_box = 3

    # Build ingredient list
    ingredient_data = []
    for ing in ingredients:
        codes = ing.get('allergens') or []
        if isinstance(codes, str):
            try:
                codes = json.loads(codes)
            except (ValueError, TypeError):
                codes = []
        ingredient_data.append((ing['item_name'], codes))

    ingredients_html = _build_ingredient_paragraph(ingredient_data, allergen_lookup, body_size)

    # Combined allergen list
    contains_html = ''
    if allergen_data['combined']:
        names = [allergen_lookup[c]['display_name'] for c in allergen_data['combined'] if c in allergen_lookup]
        if names:
            contains_html = '<b><u>CONTAINS: ' + ', '.join(names) + '</u></b>'

    # Storage and use-by
    storage = recipe.get('ppds_storage_instructions') or 'Keep refrigerated below 5°C'
    use_by_days = recipe.get('ppds_use_by_days')
    use_by_text = f'USE BY: ____________  (within {use_by_days} day{"s" if use_by_days != 1 else ""} of production)' if use_by_days else 'USE BY: ____________'

    # Build paragraph styles for this size
    title_style = ParagraphStyle('PpdsTitle', fontSize=title_size, fontName='Helvetica-Bold', alignment=TA_LEFT, spaceAfter=4)
    section_label_style = ParagraphStyle('PpdsSectionLabel', fontSize=body_size - 1, fontName='Helvetica-Bold', textColor=colors.HexColor('#4a5568'), spaceAfter=2, spaceBefore=4)
    body_style = ParagraphStyle('PpdsBody', fontSize=body_size, fontName='Helvetica', leading=body_size + 2, spaceAfter=2)
    contains_style = ParagraphStyle('PpdsContains', fontSize=body_size, fontName='Helvetica-Bold', textColor=colors.HexColor('#7a1f1a'), leading=body_size + 2, spaceAfter=4, spaceBefore=4)
    useby_style = ParagraphStyle('PpdsUseBy', fontSize=body_size + 1, fontName='Helvetica-Bold', spaceBefore=4)

    # QR code
    qr_buf = _generate_qr_image(qr_url, box_size=qr_box)
    qr_img_size = 2.2*cm if label_size == 'a6' else 1.7*cm
    qr_img = Image(qr_buf, width=qr_img_size, height=qr_img_size)

    # Compose flowables
    flow = []
    flow.append(Paragraph(recipe['name'], title_style))
    flow.append(Paragraph('INGREDIENTS:', section_label_style))
    flow.append(Paragraph(ingredients_html or '(no ingredients listed)', body_style))
    if contains_html:
        flow.append(Paragraph(contains_html, contains_style))
    flow.append(Paragraph(f'Storage: {storage}', body_style))
    flow.append(Paragraph(use_by_text, useby_style))

    # QR code with caption
    qr_caption = ParagraphStyle('PpdsQrCap', fontSize=body_size - 2, fontName='Helvetica-Oblique', alignment=TA_CENTER, textColor=colors.HexColor('#4a5568'))
    qr_table = Table([
        [qr_img],
        [Paragraph('Scan for full allergen info', qr_caption)],
    ], colWidths=[qr_img_size])
    qr_table.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
    ]))

    # On A6 the QR sits to the right of the use-by line
    # For simplicity we put it at the bottom for all sizes
    flow.append(Spacer(1, 4))
    flow.append(qr_table)

    return flow


@app.route('/recipes/<int:id>/ppds-label.pdf')
@login_required
def ppds_label_pdf(id):
    """Generate a printable PPDS label for a recipe.
    Query param: size=a6|a7|thermal (default a6)."""
    try:
        org_id = get_current_org_id()
        size = request.args.get('size', 'a6')
        if size not in ('a6', 'a7', 'thermal'):
            size = 'a6'

        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM recipes WHERE id = %s AND organization_id = %s', (id, org_id))
        recipe = cur.fetchone()
        if not recipe:
            cur.close()
            conn.close()
            flash('Recipe not found', 'danger')
            return redirect('/recipes')

        cur.execute('''
            SELECT ri.quantity, i.name as item_name, i.unit, i.allergens
            FROM recipe_ingredients ri
            JOIN items i ON i.id = ri.inventory_item_id
            WHERE ri.recipe_id = %s
            ORDER BY ri.quantity DESC, i.name
        ''', (id,))
        ingredients = cur.fetchall()
        cur.close()
        conn.close()

        allergen_data = get_recipe_allergens(id, org_id)
        all_allergens = get_all_allergens()
        allergen_lookup = {a['code']: a for a in all_allergens}

        # Build the QR URL — points at the public allergen page Phase 5 will build
        qr_url = request.host_url.rstrip('/') + url_for('public_allergen_page', recipe_id=id)

        # Page setup per size
        if size == 'a6':
            page_size = (105*mm, 148*mm)
            margins = 8*mm
        elif size == 'a7':
            page_size = (74*mm, 105*mm)
            margins = 5*mm
        else:  # thermal — fixed 62mm wide, height auto-grows but we cap it
            page_size = (62*mm, 100*mm)
            margins = 4*mm

        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=page_size,
                              leftMargin=margins, rightMargin=margins,
                              topMargin=margins, bottomMargin=margins)
        styles = getSampleStyleSheet()
        story = _build_ppds_label_story(recipe, ingredients, allergen_data, allergen_lookup, qr_url, size, styles)
        doc.build(story)
        buffer.seek(0)

        response = make_response(buffer.read())
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = f'inline; filename="{recipe["name"].replace(" ", "-")}-PPDS-{size}.pdf"'
        return response
    except Exception as e:
        print(f"PPDS label error: {e}")
        traceback.print_exc()
        flash('Error generating label', 'danger')
        return redirect(f'/recipes/{id}')


# Public allergen page — no login required. This is what the QR code on the printed label scans to.
@app.route('/allergen/<int:recipe_id>')
def public_allergen_page(recipe_id):
    """Public-facing allergen information page — designed for someone with a serious
    allergy reading on their phone. No login required. Mobile-first."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM recipes WHERE id = %s', (recipe_id,))
        recipe = cur.fetchone()
        if not recipe:
            cur.close()
            conn.close()
            return render_template('public_allergen.html', recipe=None, org=None,
                                   ingredients=[], allergen_data=None, allergen_lookup={}), 404

        org_id = recipe['organization_id']

        cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
        org = cur.fetchone()

        cur.execute('''
            SELECT ri.quantity, i.name as item_name, i.unit, i.allergens
            FROM recipe_ingredients ri
            JOIN items i ON i.id = ri.inventory_item_id
            WHERE ri.recipe_id = %s
            ORDER BY ri.quantity DESC, i.name
        ''', (recipe_id,))
        ingredients = cur.fetchall()
        cur.close()
        conn.close()

        # Normalise the per-ingredient allergen JSON for the template
        for ing in ingredients:
            raw = ing.get('allergens')
            if raw is None:
                ing['allergens_list'] = []
            elif isinstance(raw, str):
                try:
                    ing['allergens_list'] = json.loads(raw)
                except (ValueError, TypeError):
                    ing['allergens_list'] = []
            else:
                ing['allergens_list'] = list(raw)

        allergen_data = get_recipe_allergens(recipe_id, org_id)
        all_allergens = get_all_allergens()
        allergen_lookup = {a['code']: a for a in all_allergens}

        return render_template('public_allergen.html',
                               recipe=recipe, org=org,
                               ingredients=ingredients,
                               allergen_data=allergen_data,
                               allergen_lookup=allergen_lookup)
    except Exception as e:
        print(f"Public allergen page error: {e}")
        traceback.print_exc()
        return "Error loading allergen information. Please contact the food business directly.", 500


@app.route('/recipes/<int:recipe_id>/save-manual-allergens', methods=['POST'])
@login_required
def save_recipe_manual_allergens(recipe_id):
    """Updates the list of manually-declared allergens on a recipe.
    Used for hidden allergens not on the ingredient list (e.g. 'fried in shared peanut oil')."""
    try:
        org_id = get_current_org_id()
        codes = parse_allergen_codes(request.form)
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE recipes
            SET manual_allergens = %s::jsonb, updated_at = %s
            WHERE id = %s AND organization_id = %s
        ''', (json.dumps(codes), datetime.now(), recipe_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Manual allergens updated.', 'success')
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Save manual allergens error: {e}")
        flash('Error saving allergens', 'danger')
        return redirect(f'/recipes/{recipe_id}')


@app.route('/recipes/<int:recipe_id>/link-equipment', methods=['POST'])
@login_required
def link_recipe_equipment(recipe_id):
    """Links a piece of equipment to this recipe (used to derive cross-contamination)."""
    try:
        org_id = get_current_org_id()
        equipment_id = request.form.get('equipment_id')
        if not equipment_id:
            flash('Pick an equipment item to link.', 'danger')
            return redirect(f'/recipes/{recipe_id}')
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO haccp_recipe_equipment (organization_id, recipe_id, equipment_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (recipe_id, equipment_id) DO NOTHING
        ''', (org_id, recipe_id, equipment_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Equipment linked to recipe.', 'success')
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Link equipment error: {e}")
        flash('Error linking equipment', 'danger')
        return redirect(f'/recipes/{recipe_id}')


@app.route('/recipes/<int:recipe_id>/unlink-equipment/<int:equipment_id>', methods=['POST'])
@login_required
def unlink_recipe_equipment(recipe_id, equipment_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            DELETE FROM haccp_recipe_equipment
            WHERE recipe_id = %s AND equipment_id = %s AND organization_id = %s
        ''', (recipe_id, equipment_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Equipment unlinked.', 'success')
        return redirect(f'/recipes/{recipe_id}')
    except Exception as e:
        print(f"Unlink equipment error: {e}")
        flash('Error unlinking equipment', 'danger')
        return redirect(f'/recipes/{recipe_id}')


@app.route('/haccp/equipment/<int:equipment_id>/allergens', methods=['POST'])
@login_required
def save_equipment_allergens(equipment_id):
    """Replaces the cross-contamination allergen flags on a piece of equipment.
    Form sends allergen_<code>=on for each ticked box."""
    try:
        org_id = get_current_org_id()
        codes = parse_allergen_codes(request.form)
        conn = get_db()
        cur = conn.cursor()
        # Verify equipment belongs to this org
        cur.execute('SELECT id FROM haccp_equipment WHERE id = %s AND organization_id = %s', (equipment_id, org_id))
        if not cur.fetchone():
            cur.close()
            conn.close()
            flash('Equipment not found.', 'danger')
            return redirect('/haccp/temperatures')
        # Wipe existing flags and re-insert (simpler and atomic enough for this volume)
        cur.execute('DELETE FROM haccp_equipment_allergens WHERE equipment_id = %s AND organization_id = %s', (equipment_id, org_id))
        for code in codes:
            cur.execute('''
                INSERT INTO haccp_equipment_allergens (organization_id, equipment_id, allergen_code, notes)
                VALUES (%s, %s, %s, %s)
            ''', (org_id, equipment_id, code, request.form.get(f'note_{code}', '')))
        conn.commit()
        cur.close()
        conn.close()
        flash('Cross-contamination allergens updated.', 'success')
        return redirect('/haccp/temperatures')
    except Exception as e:
        print(f"Save equipment allergens error: {e}")
        traceback.print_exc()
        flash('Error saving allergens', 'danger')
        return redirect('/haccp/temperatures')


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
            'resolvable_id': fail['id'],
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

    # 5. PPDS recipes — Natasha's Law compliance checks
    cur.execute('''
        SELECT r.id, r.name, r.ppds_storage_instructions, r.ppds_use_by_days,
               COUNT(ri.id) as ingredient_count
        FROM recipes r
        LEFT JOIN recipe_ingredients ri ON ri.recipe_id = r.id
        WHERE r.organization_id = %s AND r.is_ppds = true
        GROUP BY r.id, r.name, r.ppds_storage_instructions, r.ppds_use_by_days
    ''', (org_id,))
    ppds_recipes = cur.fetchall()

    for rec in ppds_recipes:
        # Critical: PPDS recipe with no ingredients — cannot legally print a label
        if rec['ingredient_count'] == 0:
            alerts['critical'].append({
                'type': 'ppds_no_ingredients',
                'severity': 'critical',
                'title': f"PPDS recipe '{rec['name']}' has no ingredients",
                'detail': "Cannot print a legally compliant label without an ingredient list",
                'link': f"/recipes/{rec['id']}",
            })
            continue
        # Warning: missing storage AND use-by
        if not rec['ppds_storage_instructions'] and not rec['ppds_use_by_days']:
            alerts['warning'].append({
                'type': 'ppds_missing_storage',
                'severity': 'warning',
                'title': f"'{rec['name']}' missing storage info",
                'detail': "PPDS labels need storage instructions and a use-by period",
                'link': f"/recipes/{rec['id']}",
            })

    # 6. Inventory items used in PPDS recipes that have no allergen flags set —
    # info-level prompt to verify whether allergens were missed
    cur.execute('''
        SELECT DISTINCT i.id, i.name
        FROM items i
        JOIN recipe_ingredients ri ON ri.inventory_item_id = i.id
        JOIN recipes r ON r.id = ri.recipe_id
        WHERE r.organization_id = %s
          AND r.is_ppds = true
          AND (i.allergens IS NULL OR i.allergens = '[]'::jsonb)
    ''', (org_id,))
    unflagged_items = cur.fetchall()
    if unflagged_items:
        names = ', '.join(item['name'] for item in unflagged_items[:3])
        if len(unflagged_items) > 3:
            names += f" + {len(unflagged_items) - 3} more"
        alerts['info'].append({
            'type': 'ppds_unflagged_ingredients',
            'severity': 'info',
            'title': f"{len(unflagged_items)} PPDS ingredient{'s' if len(unflagged_items) != 1 else ''} have no allergens flagged",
            'detail': f"Verify allergens on: {names}",
            'link': '/inventory',
        })

    # 7. Pest control — overdue contractor visits
    cur.execute('''
        SELECT contractor_name, last_visit_date, next_visit_due, visit_frequency_days, has_contract
        FROM haccp_pest_contract WHERE organization_id = %s
    ''', (org_id,))
    pest_contract = cur.fetchone()
    if pest_contract:
        if pest_contract.get('has_contract') and pest_contract.get('next_visit_due'):
            days_overdue = (datetime.now().date() - pest_contract['next_visit_due']).days
            if days_overdue > 0:
                severity = 'critical' if days_overdue > 14 else 'warning'
                alerts[severity].append({
                    'type': 'pest_visit_overdue',
                    'severity': severity,
                    'title': f"Pest control visit overdue by {days_overdue} day{'s' if days_overdue != 1 else ''}",
                    'detail': f"Last visit: {pest_contract['last_visit_date'].strftime('%d %b %Y') if pest_contract.get('last_visit_date') else 'unknown'}",
                    'link': '/haccp/pest-control',
                })
        elif pest_contract.get('has_contract') and not pest_contract.get('last_visit_date'):
            alerts['info'].append({
                'type': 'pest_no_visits',
                'severity': 'info',
                'title': 'Pest control contract set up but no visits logged',
                'detail': 'Log your first visit to start the audit trail.',
                'link': '/haccp/pest-control',
            })

    # 8. Pest control — unresolved sightings
    cur.execute('''
        SELECT pest_type, sighted_at, severity, location
        FROM haccp_pest_sightings
        WHERE organization_id = %s AND is_resolved = false
        ORDER BY sighted_at ASC
    ''', (org_id,))
    open_sightings = cur.fetchall()
    for s in open_sightings:
        days_open = (datetime.now() - s['sighted_at']).days if s['sighted_at'] else 0
        if s['severity'] == 'high' or days_open > 3:
            alerts['critical'].append({
                'type': 'pest_sighting_open',
                'severity': 'critical',
                'title': f"Open pest sighting: {s['pest_type']}{' (' + s['location'] + ')' if s['location'] else ''}",
                'detail': f"Reported {days_open} day{'s' if days_open != 1 else ''} ago — needs resolving.",
                'link': '/haccp/pest-control',
            })
        else:
            alerts['warning'].append({
                'type': 'pest_sighting_open',
                'severity': 'warning',
                'title': f"Open pest sighting: {s['pest_type']}{' (' + s['location'] + ')' if s['location'] else ''}",
                'detail': f"Reported {days_open} day{'s' if days_open != 1 else ''} ago.",
                'link': '/haccp/pest-control',
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

        # Annotate each equipment row with its allergen codes (for cross-contamination flagging UI)
        cur.execute('''
            SELECT equipment_id, allergen_code
            FROM haccp_equipment_allergens
            WHERE organization_id = %s
        ''', (org_id,))
        eq_allergens = {}
        for row in cur.fetchall():
            eq_allergens.setdefault(row['equipment_id'], []).append(row['allergen_code'])
        for eq in equipment:
            eq['allergen_codes'] = eq_allergens.get(eq['id'], [])

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

        all_allergens = get_all_allergens()
        allergen_lookup = {a['code']: a for a in all_allergens}

        return render_template('haccp_temperatures.html',
                             equipment=equipment, logs=logs,
                             all_allergens=all_allergens,
                             allergen_lookup=allergen_lookup)
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


@app.route('/haccp/temperature/resolve/<int:id>', methods=['POST'])
@login_required
def resolve_temperature_alert(id):
    """Adds a corrective action to a previously-failed temperature log,
    which removes it from the alerts feed and writes the note to the audit trail."""
    try:
        org_id = get_current_org_id()
        action = request.form.get('corrective_action', '').strip()
        if not action:
            flash('A corrective action note is required.', 'danger')
            return redirect(request.referrer or '/haccp')
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE haccp_temperature_logs
            SET corrective_action = %s
            WHERE id = %s AND organization_id = %s
        ''', (
            f"{action} (resolved by {session.get('user_name', 'Unknown')} on {datetime.now().strftime('%d %b %Y %H:%M')})",
            id,
            org_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Alert resolved. Corrective action recorded in audit trail.', 'success')
        return redirect(request.referrer or '/haccp')
    except Exception as e:
        print(f"Resolve alert error: {e}")
        flash('Error resolving alert', 'danger')
        return redirect(request.referrer or '/haccp')


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
                responsible_person = %s, logo_url = %s,
                show_contact_on_public_page = %s, show_fbo_on_public_page = %s
            WHERE id = %s
        ''', (
            request.form.get('business_name', '').strip() or None,
            request.form.get('business_address', '').strip() or None,
            request.form.get('business_phone', '').strip() or None,
            request.form.get('business_email', '').strip() or None,
            request.form.get('food_business_registration', '').strip() or None,
            request.form.get('responsible_person', '').strip() or None,
            request.form.get('logo_url', '').strip() or None,
            request.form.get('show_contact_on_public_page') == 'on',
            request.form.get('show_fbo_on_public_page') == 'on',
            org_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Business details saved!', 'success')
        return redirect('/settings/business')
    except Exception as e:
        print(f"Save business settings error: {e}")
        traceback.print_exc()
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


# ============================================================
# DATA EXPORT — customer-controlled backups
# Lets users download a ZIP of all their data, or email it to themselves.
# Format: 6 CSVs + 1 PDF audit summary, packaged as a single ZIP.
# Email side requires RESEND_API_KEY env var (falls back to no-op if absent).
# ============================================================

def _build_data_export_zip(org_id):
    """Generate a ZIP file containing all customer data as CSVs.
    Returns BytesIO. Used by both the download route and the email route."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
    org = cur.fetchone()

    # Build each CSV in memory
    csvs = {}

    # 1. Inventory items
    cur.execute('''
        SELECT name, category, stock, reorder_point, cost, unit, allergens, updated_at
        FROM items WHERE organization_id = %s ORDER BY name
    ''', (org_id,))
    rows = cur.fetchall()
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(['Name', 'Category', 'Stock', 'Reorder Point', 'Cost (£)', 'Unit', 'Allergens', 'Last Updated'])
    for r in rows:
        allergens = r['allergens']
        if isinstance(allergens, str):
            try: allergens = json.loads(allergens)
            except: allergens = []
        elif allergens is None:
            allergens = []
        w.writerow([
            r['name'], r['category'] or '', float(r['stock']),
            float(r['reorder_point']), float(r['cost']),
            r['unit'], ','.join(allergens) if allergens else '',
            r['updated_at'].strftime('%Y-%m-%d %H:%M') if r['updated_at'] else ''
        ])
    csvs['inventory.csv'] = sio.getvalue()

    # 2. Recipes
    cur.execute('''
        SELECT id, name, category, servings, portion_size, selling_price,
               instructions, notes, manual_allergens, is_ppds,
               ppds_storage_instructions, ppds_use_by_days, created_at
        FROM recipes WHERE organization_id = %s ORDER BY name
    ''', (org_id,))
    rows = cur.fetchall()
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(['ID', 'Name', 'Category', 'Servings', 'Portion Size', 'Selling Price (£)',
                'Instructions', 'Notes', 'Manual Allergens', 'Is PPDS',
                'PPDS Storage', 'PPDS Use-By (days)', 'Created'])
    for r in rows:
        manual = r['manual_allergens']
        if isinstance(manual, str):
            try: manual = json.loads(manual)
            except: manual = []
        elif manual is None:
            manual = []
        w.writerow([
            r['id'], r['name'], r['category'] or '', r['servings'] or '',
            r['portion_size'] or '', float(r['selling_price'] or 0),
            r['instructions'] or '', r['notes'] or '',
            ','.join(manual) if manual else '',
            'Yes' if r['is_ppds'] else 'No',
            r['ppds_storage_instructions'] or '',
            r['ppds_use_by_days'] or '',
            r['created_at'].strftime('%Y-%m-%d') if r['created_at'] else ''
        ])
    csvs['recipes.csv'] = sio.getvalue()

    # 3. Recipe ingredients (links between recipes and inventory)
    cur.execute('''
        SELECT r.name as recipe_name, i.name as ingredient_name, i.unit,
               ri.quantity, ri.notes
        FROM recipe_ingredients ri
        JOIN recipes r ON r.id = ri.recipe_id
        JOIN items i ON i.id = ri.inventory_item_id
        WHERE r.organization_id = %s
        ORDER BY r.name, i.name
    ''', (org_id,))
    rows = cur.fetchall()
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(['Recipe', 'Ingredient', 'Unit', 'Quantity', 'Notes'])
    for r in rows:
        w.writerow([r['recipe_name'], r['ingredient_name'], r['unit'],
                    float(r['quantity']), r['notes'] or ''])
    csvs['recipe_ingredients.csv'] = sio.getvalue()

    # 4. Temperature logs (full history)
    cur.execute('''
        SELECT tl.logged_at, e.name as equipment_name, e.equipment_type,
               e.min_temp, e.max_temp, tl.temperature, tl.status,
               tl.corrective_action, tl.notes, tl.is_voided, tl.void_reason,
               tl.voided_at
        FROM haccp_temperature_logs tl
        JOIN haccp_equipment e ON e.id = tl.equipment_id
        WHERE tl.organization_id = %s
        ORDER BY tl.logged_at DESC
    ''', (org_id,))
    rows = cur.fetchall()
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(['Logged At', 'Equipment', 'Type', 'Min Temp', 'Max Temp',
                'Reading (°C)', 'Status', 'Corrective Action', 'Notes',
                'Voided', 'Void Reason', 'Voided At'])
    for r in rows:
        w.writerow([
            r['logged_at'].strftime('%Y-%m-%d %H:%M') if r['logged_at'] else '',
            r['equipment_name'], r['equipment_type'] or '',
            r['min_temp'] if r['min_temp'] is not None else '',
            r['max_temp'] if r['max_temp'] is not None else '',
            float(r['temperature']),
            r['status'], r['corrective_action'] or '', r['notes'] or '',
            'Yes' if r['is_voided'] else 'No',
            r['void_reason'] or '',
            r['voided_at'].strftime('%Y-%m-%d %H:%M') if r['voided_at'] else ''
        ])
    csvs['temperature_logs.csv'] = sio.getvalue()

    # 5. Cleaning logs
    cur.execute('''
        SELECT cl.completed_at, ct.task_name, ct.area, cl.completed_by, cl.notes
        FROM haccp_cleaning_logs cl
        JOIN haccp_cleaning_tasks ct ON ct.id = cl.task_id
        WHERE cl.organization_id = %s
        ORDER BY cl.completed_at DESC
    ''', (org_id,))
    rows = cur.fetchall()
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(['Completed At', 'Task', 'Area', 'Completed By', 'Notes'])
    for r in rows:
        w.writerow([
            r['completed_at'].strftime('%Y-%m-%d %H:%M') if r['completed_at'] else '',
            r['task_name'], r['area'] or '',
            r['completed_by'] or '', r['notes'] or ''
        ])
    csvs['cleaning_logs.csv'] = sio.getvalue()

    # 6. Delivery logs
    cur.execute('''
        SELECT delivery_date, supplier_name, chilled_temp, frozen_temp,
               packaging_ok, expiry_dates_ok, quality_ok,
               accepted, notes, inspected_by
        FROM haccp_delivery_logs
        WHERE organization_id = %s
        ORDER BY delivery_date DESC
    ''', (org_id,))
    rows = cur.fetchall()
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(['Delivery Date', 'Supplier', 'Chilled Temp (°C)', 'Frozen Temp (°C)',
                'Packaging OK', 'Expiry Dates OK', 'Quality OK',
                'Accepted', 'Inspected By', 'Notes'])
    for r in rows:
        w.writerow([
            r['delivery_date'].strftime('%Y-%m-%d %H:%M') if r['delivery_date'] else '',
            r['supplier_name'] or '',
            float(r['chilled_temp']) if r['chilled_temp'] is not None else '',
            float(r['frozen_temp']) if r['frozen_temp'] is not None else '',
            'Yes' if r['packaging_ok'] else 'No',
            'Yes' if r['expiry_dates_ok'] else 'No',
            'Yes' if r['quality_ok'] else 'No',
            'Yes' if r['accepted'] else 'No',
            r['inspected_by'] or '', r['notes'] or ''
        ])
    csvs['delivery_logs.csv'] = sio.getvalue()

    cur.close()
    conn.close()

    # Generate the ZIP
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        # README explaining the contents
        readme = (
            f"YieldGuard Data Export\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"Organisation: {(org.get('business_name') if org else None) or 'Unnamed'}\n"
            f"\n"
            f"This ZIP contains all your YieldGuard data in CSV format.\n"
            f"CSVs open in Excel, Numbers, Google Sheets, and most spreadsheet apps.\n"
            f"\n"
            f"Files included:\n"
            f"- inventory.csv         All inventory items with allergen flags\n"
            f"- recipes.csv           All recipes with PPDS settings\n"
            f"- recipe_ingredients.csv  Links between recipes and inventory items\n"
            f"- temperature_logs.csv  Full temperature history (incl. voided entries)\n"
            f"- cleaning_logs.csv     Cleaning task sign-offs\n"
            f"- delivery_logs.csv     Goods-in temperature checks\n"
            f"\n"
            f"Your data is yours. Keep this archive somewhere safe.\n"
        )
        zf.writestr('README.txt', readme)
        for filename, content in csvs.items():
            zf.writestr(filename, content)

    zip_buffer.seek(0)
    return zip_buffer, org


# ============================================================
# PEST CONTROL — visit logs, sightings, contractor details
# Photos uploaded to Supabase Storage (bucket: pest-photos)
# ============================================================

def _upload_photo_to_supabase(file_storage, bucket_name):
    """Upload a photo to a Supabase Storage bucket. Returns (url, filename) or (None, None)."""
    if not file_storage or not file_storage.filename:
        return (None, None)
    supabase_url = os.environ.get('SUPABASE_URL', '').rstrip('/')
    service_key = os.environ.get('SUPABASE_SERVICE_KEY')
    if not supabase_url or not service_key:
        print("Photo upload skipped: SUPABASE_URL or SUPABASE_SERVICE_KEY not set")
        return (None, None)

    # Generate a unique filename, preserve the extension
    ext = ''
    if '.' in file_storage.filename:
        ext = '.' + file_storage.filename.rsplit('.', 1)[1].lower()
        if ext not in ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.heic', '.pdf'):
            ext = '.jpg'
    safe_name = f"{uuid.uuid4().hex}{ext}"

    file_bytes = file_storage.read()
    if not file_bytes:
        return (None, None)

    content_type = file_storage.content_type or 'image/jpeg'

    upload_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{safe_name}"
    req = urllib.request.Request(
        upload_url,
        data=file_bytes,
        method='POST',
        headers={
            'Authorization': f'Bearer {service_key}',
            'Content-Type': content_type,
            'x-upsert': 'true',
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status not in (200, 201):
                print(f"Photo upload failed status {resp.status}")
                return (None, None)
    except Exception as e:
        print(f"Photo upload error: {e}")
        return (None, None)

    public_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{safe_name}"
    return (public_url, file_storage.filename)


def _upload_pest_photo(file_storage):
    """Backwards-compat wrapper for existing pest control routes."""
    return _upload_photo_to_supabase(file_storage, 'pest-photos')


@app.route('/haccp/pest-control')
@login_required
def pest_control():
    """Main pest control page — contract, recent visits, open sightings."""
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        # Contract (one row per org, may not exist yet)
        cur.execute('SELECT * FROM haccp_pest_contract WHERE organization_id = %s', (org_id,))
        contract = cur.fetchone()

        # Recent visits (last 20)
        cur.execute('''
            SELECT * FROM haccp_pest_visits
            WHERE organization_id = %s
            ORDER BY visit_date DESC
            LIMIT 20
        ''', (org_id,))
        visits = cur.fetchall()

        # Open sightings (unresolved first, then recent resolved)
        cur.execute('''
            SELECT * FROM haccp_pest_sightings
            WHERE organization_id = %s
            ORDER BY is_resolved ASC, sighted_at DESC
            LIMIT 20
        ''', (org_id,))
        sightings = cur.fetchall()

        # Stats for the page
        cur.execute('''
            SELECT COUNT(*) as cnt FROM haccp_pest_sightings
            WHERE organization_id = %s AND is_resolved = false
        ''', (org_id,))
        unresolved_count = cur.fetchone()['cnt']

        cur.execute('''
            SELECT COUNT(*) as cnt FROM haccp_pest_visits
            WHERE organization_id = %s AND visit_date >= NOW() - INTERVAL '30 days'
        ''', (org_id,))
        visits_30d = cur.fetchone()['cnt']

        # Days since last visit (for warning display)
        days_since_last = None
        if contract and contract.get('last_visit_date'):
            days_since_last = (datetime.now().date() - contract['last_visit_date']).days

        cur.close()
        conn.close()

        return render_template('pest_control.html',
                               contract=contract,
                               visits=visits,
                               sightings=sightings,
                               unresolved_count=unresolved_count,
                               visits_30d=visits_30d,
                               days_since_last=days_since_last)
    except Exception as e:
        print(f"Pest control page error: {e}")
        traceback.print_exc()
        return f"Error loading pest control page: {str(e)}", 500


@app.route('/haccp/pest-control/contract', methods=['POST'])
@login_required
def save_pest_contract():
    """Create or update the pest control contract row."""
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        has_contract = request.form.get('has_contract') == 'on'
        contractor_name = request.form.get('contractor_name', '').strip() or None
        contractor_phone = request.form.get('contractor_phone', '').strip() or None
        contractor_email = request.form.get('contractor_email', '').strip() or None
        contract_type = request.form.get('contract_type', '').strip() or None
        visit_frequency_days = request.form.get('visit_frequency_days', '').strip()
        visit_frequency_days = int(visit_frequency_days) if visit_frequency_days.isdigit() else None
        notes = request.form.get('notes', '').strip() or None

        # Upsert (insert or update by org_id)
        cur.execute('''
            INSERT INTO haccp_pest_contract
                (organization_id, has_contract, contractor_name, contractor_phone,
                 contractor_email, contract_type, visit_frequency_days, notes, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (organization_id) DO UPDATE SET
                has_contract = EXCLUDED.has_contract,
                contractor_name = EXCLUDED.contractor_name,
                contractor_phone = EXCLUDED.contractor_phone,
                contractor_email = EXCLUDED.contractor_email,
                contract_type = EXCLUDED.contract_type,
                visit_frequency_days = EXCLUDED.visit_frequency_days,
                notes = EXCLUDED.notes,
                updated_at = NOW()
        ''', (org_id, has_contract, contractor_name, contractor_phone,
              contractor_email, contract_type, visit_frequency_days, notes))
        conn.commit()
        cur.close()
        conn.close()
        flash('Pest control contract saved.', 'success')
        return redirect('/haccp/pest-control')
    except Exception as e:
        print(f"Save pest contract error: {e}")
        traceback.print_exc()
        flash('Error saving contract details.', 'danger')
        return redirect('/haccp/pest-control')


@app.route('/haccp/pest-control/visit/new', methods=['GET', 'POST'])
@login_required
def pest_visit_new():
    if request.method == 'GET':
        return render_template('pest_visit_form.html', visit=None)
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        photo_url, photo_filename = _upload_pest_photo(request.files.get('photo'))

        visit_date = request.form.get('visit_date')
        if not visit_date:
            visit_date = datetime.now().isoformat()

        cur.execute('''
            INSERT INTO haccp_pest_visits
                (organization_id, visit_date, visit_type, inspector_name, is_contractor,
                 areas_inspected, findings, activity_found, action_taken, notes,
                 photo_url, photo_filename, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            org_id,
            visit_date,
            request.form.get('visit_type', 'scheduled'),
            request.form.get('inspector_name', '').strip() or None,
            request.form.get('is_contractor') == 'on',
            request.form.get('areas_inspected', '').strip() or None,
            request.form.get('findings', '').strip() or None,
            request.form.get('activity_found') == 'on',
            request.form.get('action_taken', '').strip() or None,
            request.form.get('notes', '').strip() or None,
            photo_url,
            photo_filename,
            session.get('user_name', 'Unknown')
        ))

        # Update contract's last_visit_date and next_visit_due
        visit_dt = datetime.fromisoformat(visit_date.replace('Z', '+00:00')) if 'T' in visit_date else datetime.strptime(visit_date, '%Y-%m-%d')
        cur.execute('SELECT visit_frequency_days FROM haccp_pest_contract WHERE organization_id = %s', (org_id,))
        contract_row = cur.fetchone()
        if contract_row:
            freq = contract_row.get('visit_frequency_days')
            next_due = (visit_dt + timedelta(days=freq)).date() if freq else None
            cur.execute('''
                UPDATE haccp_pest_contract
                SET last_visit_date = %s, next_visit_due = %s, updated_at = NOW()
                WHERE organization_id = %s
            ''', (visit_dt.date(), next_due, org_id))

        conn.commit()
        cur.close()
        conn.close()
        flash('Pest control visit logged.', 'success')
        return redirect('/haccp/pest-control')
    except Exception as e:
        print(f"Pest visit new error: {e}")
        traceback.print_exc()
        flash('Error logging visit.', 'danger')
        return redirect('/haccp/pest-control')


@app.route('/haccp/pest-control/visit/<int:visit_id>/delete', methods=['POST'])
@login_required
def pest_visit_delete(visit_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM haccp_pest_visits WHERE id = %s AND organization_id = %s',
                    (visit_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Visit deleted.', 'success')
    except Exception as e:
        print(f"Pest visit delete error: {e}")
        flash('Error deleting visit.', 'danger')
    return redirect('/haccp/pest-control')


@app.route('/haccp/pest-control/sighting/new', methods=['GET', 'POST'])
@login_required
def pest_sighting_new():
    if request.method == 'GET':
        return render_template('pest_sighting_form.html', sighting=None)
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()

        photo_url, photo_filename = _upload_pest_photo(request.files.get('photo'))

        sighted_at = request.form.get('sighted_at') or datetime.now().isoformat()

        cur.execute('''
            INSERT INTO haccp_pest_sightings
                (organization_id, sighted_at, pest_type, location, description,
                 reported_by, severity, action_taken, contractor_notified,
                 photo_url, photo_filename)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            org_id,
            sighted_at,
            request.form.get('pest_type', '').strip() or 'unknown',
            request.form.get('location', '').strip() or None,
            request.form.get('description', '').strip() or None,
            request.form.get('reported_by', '').strip() or session.get('user_name', 'Unknown'),
            request.form.get('severity', 'low'),
            request.form.get('action_taken', '').strip() or None,
            request.form.get('contractor_notified') == 'on',
            photo_url,
            photo_filename
        ))
        conn.commit()
        cur.close()
        conn.close()
        flash('Sighting logged. Make sure to action it.', 'success')
        return redirect('/haccp/pest-control')
    except Exception as e:
        print(f"Pest sighting new error: {e}")
        traceback.print_exc()
        flash('Error logging sighting.', 'danger')
        return redirect('/haccp/pest-control')


@app.route('/haccp/pest-control/sighting/<int:sighting_id>/resolve', methods=['POST'])
@login_required
def pest_sighting_resolve(sighting_id):
    try:
        org_id = get_current_org_id()
        resolution_notes = request.form.get('resolution_notes', '').strip() or None
        if not resolution_notes:
            flash('Add a resolution note before closing the sighting.', 'danger')
            return redirect('/haccp/pest-control')

        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE haccp_pest_sightings
            SET is_resolved = true,
                resolved_at = NOW(),
                resolved_by = %s,
                resolution_notes = %s
            WHERE id = %s AND organization_id = %s
        ''', (session.get('user_name', 'Unknown'), resolution_notes, sighting_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Sighting marked resolved.', 'success')
    except Exception as e:
        print(f"Pest sighting resolve error: {e}")
        flash('Error resolving sighting.', 'danger')
    return redirect('/haccp/pest-control')


@app.route('/haccp/pest-control/sighting/<int:sighting_id>/delete', methods=['POST'])
@login_required
def pest_sighting_delete(sighting_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('DELETE FROM haccp_pest_sightings WHERE id = %s AND organization_id = %s',
                    (sighting_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Sighting deleted.', 'success')
    except Exception as e:
        print(f"Pest sighting delete error: {e}")
        flash('Error deleting sighting.', 'danger')
    return redirect('/haccp/pest-control')


# ============================================================
# STAFF — staff list, certifications, internal training
# ============================================================

@app.route('/staff')
@login_required
def staff_list():
    try:
        org_id = get_current_org_id()
        show_archived = request.args.get('archived') == '1'
        conn = get_db()
        cur = conn.cursor()
        if show_archived:
            cur.execute('''
                SELECT s.*,
                    COUNT(DISTINCT c.id) as cert_count,
                    COUNT(DISTINCT t.id) as training_count
                FROM staff s
                LEFT JOIN staff_certifications c ON c.staff_id = s.id
                LEFT JOIN staff_training t ON t.staff_id = s.id
                WHERE s.organization_id = %s
                GROUP BY s.id
                ORDER BY s.is_active DESC, s.first_name, s.last_name
            ''', (org_id,))
        else:
            cur.execute('''
                SELECT s.*,
                    COUNT(DISTINCT c.id) as cert_count,
                    COUNT(DISTINCT t.id) as training_count
                FROM staff s
                LEFT JOIN staff_certifications c ON c.staff_id = s.id
                LEFT JOIN staff_training t ON t.staff_id = s.id
                WHERE s.organization_id = %s AND s.is_active = true
                GROUP BY s.id
                ORDER BY s.first_name, s.last_name
            ''', (org_id,))
        all_staff = cur.fetchall()

        # Quick stats for the page header
        cur.execute('SELECT COUNT(*) as cnt FROM staff WHERE organization_id = %s AND is_active = true', (org_id,))
        active_count = cur.fetchone()['cnt']

        cur.execute('''
            SELECT COUNT(*) as cnt FROM staff_certifications
            WHERE organization_id = %s AND expiry_date IS NOT NULL AND expiry_date < CURRENT_DATE
        ''', (org_id,))
        expired_count = cur.fetchone()['cnt']

        cur.execute('''
            SELECT COUNT(*) as cnt FROM staff_certifications
            WHERE organization_id = %s AND expiry_date IS NOT NULL
              AND expiry_date >= CURRENT_DATE
              AND expiry_date < CURRENT_DATE + INTERVAL '30 days'
        ''', (org_id,))
        expiring_soon_count = cur.fetchone()['cnt']

        cur.close()
        conn.close()
        return render_template('staff_list.html',
                               staff=all_staff,
                               show_archived=show_archived,
                               active_count=active_count,
                               expired_count=expired_count,
                               expiring_soon_count=expiring_soon_count)
    except Exception as e:
        print(f"Staff list error: {e}")
        traceback.print_exc()
        return f"Error loading staff: {str(e)}", 500


@app.route('/staff/new', methods=['GET', 'POST'])
@login_required
def staff_new():
    if request.method == 'GET':
        return render_template('staff_form.html', staff=None)
    try:
        org_id = get_current_org_id()
        first_name = request.form.get('first_name', '').strip()
        if not first_name:
            flash('First name is required.', 'danger')
            return redirect('/staff/new')
        conn = get_db()
        cur = conn.cursor()
        photo_url, _ = _upload_photo_to_supabase(request.files.get('photo'), 'staff-certs')
        start_date = request.form.get('start_date', '').strip() or None
        cur.execute('''
            INSERT INTO staff (organization_id, first_name, last_name, role,
                               email, phone, start_date, notes, photo_url, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, true)
            RETURNING id
        ''', (
            org_id,
            first_name,
            request.form.get('last_name', '').strip() or None,
            request.form.get('role', '').strip() or None,
            request.form.get('email', '').strip() or None,
            request.form.get('phone', '').strip() or None,
            start_date,
            request.form.get('notes', '').strip() or None,
            photo_url,
        ))
        new_id = cur.fetchone()['id']
        conn.commit()
        cur.close()
        conn.close()
        flash(f"{first_name} added.", 'success')
        return redirect(f'/staff/{new_id}')
    except Exception as e:
        print(f"Staff new error: {e}")
        traceback.print_exc()
        flash('Error adding staff member.', 'danger')
        return redirect('/staff')


@app.route('/staff/<int:staff_id>')
@login_required
def staff_detail(staff_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('SELECT * FROM staff WHERE id = %s AND organization_id = %s', (staff_id, org_id))
        person = cur.fetchone()
        if not person:
            cur.close()
            conn.close()
            flash('Staff member not found.', 'danger')
            return redirect('/staff')
        cur.close()
        conn.close()
        return render_template('staff_detail.html', person=person)
    except Exception as e:
        print(f"Staff detail error: {e}")
        traceback.print_exc()
        return f"Error: {str(e)}", 500


@app.route('/staff/<int:staff_id>/edit', methods=['GET', 'POST'])
@login_required
def staff_edit(staff_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        if request.method == 'POST':
            photo_url_existing = request.form.get('existing_photo_url') or None
            photo_url_new, _ = _upload_photo_to_supabase(request.files.get('photo'), 'staff-certs')
            photo_url = photo_url_new or photo_url_existing
            cur.execute('''
                UPDATE staff
                SET first_name = %s, last_name = %s, role = %s,
                    email = %s, phone = %s, start_date = %s, end_date = %s,
                    is_active = %s, notes = %s, photo_url = %s, updated_at = NOW()
                WHERE id = %s AND organization_id = %s
            ''', (
                request.form.get('first_name', '').strip(),
                request.form.get('last_name', '').strip() or None,
                request.form.get('role', '').strip() or None,
                request.form.get('email', '').strip() or None,
                request.form.get('phone', '').strip() or None,
                request.form.get('start_date', '').strip() or None,
                request.form.get('end_date', '').strip() or None,
                request.form.get('is_active') == 'on',
                request.form.get('notes', '').strip() or None,
                photo_url,
                staff_id, org_id
            ))
            conn.commit()
            cur.close()
            conn.close()
            flash('Staff member updated.', 'success')
            return redirect(f'/staff/{staff_id}')

        cur.execute('SELECT * FROM staff WHERE id = %s AND organization_id = %s', (staff_id, org_id))
        person = cur.fetchone()
        cur.close()
        conn.close()
        if not person:
            flash('Staff member not found.', 'danger')
            return redirect('/staff')
        return render_template('staff_form.html', staff=person)
    except Exception as e:
        print(f"Staff edit error: {e}")
        traceback.print_exc()
        flash('Error editing staff.', 'danger')
        return redirect('/staff')


@app.route('/staff/<int:staff_id>/archive', methods=['POST'])
@login_required
def staff_archive(staff_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE staff SET is_active = false, end_date = CURRENT_DATE, updated_at = NOW()
            WHERE id = %s AND organization_id = %s
        ''', (staff_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Staff member archived.', 'success')
    except Exception as e:
        print(f"Staff archive error: {e}")
        flash('Error archiving staff.', 'danger')
    return redirect('/staff')


@app.route('/staff/<int:staff_id>/restore', methods=['POST'])
@login_required
def staff_restore(staff_id):
    try:
        org_id = get_current_org_id()
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            UPDATE staff SET is_active = true, end_date = NULL, updated_at = NOW()
            WHERE id = %s AND organization_id = %s
        ''', (staff_id, org_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Staff member restored.', 'success')
    except Exception as e:
        print(f"Staff restore error: {e}")
        flash('Error restoring staff.', 'danger')
    return redirect(f'/staff/{staff_id}')


@app.route('/data-export')
@login_required
def data_export_page():
    """Page where customers download a backup or email one to themselves."""
    org_id = get_current_org_id()
    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM organizations WHERE id = %s', (org_id,))
    org = cur.fetchone()
    cur.close()
    conn.close()
    return render_template('data_export.html', org=org,
                           user_email=session.get('user_email', ''))


@app.route('/data-export/download')
@login_required
def data_export_download():
    """Stream a ZIP of all customer data."""
    try:
        org_id = get_current_org_id()
        zip_buffer, org = _build_data_export_zip(org_id)
        date_str = datetime.now().strftime('%Y-%m-%d')
        org_name = (org.get('business_name') if org else None) or 'yieldguard'
        # Sanitise org name for filename
        safe_name = ''.join(c if c.isalnum() else '-' for c in org_name).lower().strip('-')
        filename = f"{safe_name}-backup-{date_str}.zip"
        response = make_response(zip_buffer.getvalue())
        response.headers['Content-Type'] = 'application/zip'
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
    except Exception as e:
        print(f"Data export download error: {e}")
        traceback.print_exc()
        flash('Error generating backup. Please try again or contact support.', 'danger')
        return redirect('/data-export')


def _send_export_email(to_email, zip_buffer, org_name):
    """Send the ZIP backup as an email attachment via Resend.
    Returns (success: bool, error_message: str or None)."""
    api_key = os.environ.get('RESEND_API_KEY')
    if not api_key:
        return (False, "Email service not configured yet (no RESEND_API_KEY). "
                       "Use the download button instead, or ask Sean to set up Resend.")

    # Lazy-import resend so the app doesn't crash if package not yet installed
    try:
        import resend
    except ImportError:
        return (False, "Resend package not installed. Run: pip install resend")

    resend.api_key = api_key
    sender = os.environ.get('RESEND_FROM', 'YieldGuard <onboarding@resend.dev>')

    date_str = datetime.now().strftime('%d %B %Y')
    safe_name = ''.join(c if c.isalnum() else '-' for c in (org_name or 'yieldguard')).lower().strip('-')
    filename = f"{safe_name}-backup-{datetime.now().strftime('%Y-%m-%d')}.zip"

    import base64
    zip_b64 = base64.b64encode(zip_buffer.getvalue()).decode('ascii')

    html_body = f"""
    <div style="font-family: -apple-system, sans-serif; max-width: 600px; margin: 0 auto; padding: 24px;">
        <h2 style="color: #4A6B45; margin-bottom: 8px;">Your YieldGuard backup</h2>
        <p style="color: #555; line-height: 1.5;">Hi,</p>
        <p style="color: #555; line-height: 1.5;">
            Attached is a complete backup of your YieldGuard data as of <strong>{date_str}</strong>.
            The ZIP contains CSV files that open in Excel, Numbers, or Google Sheets.
        </p>
        <p style="color: #555; line-height: 1.5;">
            Keep this archive somewhere safe — it's your guarantee that even if anything ever
            went wrong with our systems, you'd still have your data.
        </p>
        <p style="color: #555; line-height: 1.5; margin-top: 24px;">
            — The YieldGuard team
        </p>
        <hr style="border: none; border-top: 1px solid #eee; margin: 24px 0;">
        <p style="color: #999; font-size: 12px;">
            You requested this email from your YieldGuard account.
            If you didn't request it, please contact us.
        </p>
    </div>
    """

    try:
        resend.Emails.send({
            "from": sender,
            "to": to_email,
            "subject": f"Your YieldGuard backup — {date_str}",
            "html": html_body,
            "attachments": [{
                "filename": filename,
                "content": zip_b64,
            }],
        })
        return (True, None)
    except Exception as e:
        return (False, str(e))


@app.route('/data-export/email-now', methods=['POST'])
@login_required
def data_export_email_now():
    """Email the customer their backup right now."""
    try:
        org_id = get_current_org_id()
        to_email = (request.form.get('email') or session.get('user_email') or '').strip()
        if not to_email:
            flash('No email address available. Add one in Business Settings.', 'danger')
            return redirect('/data-export')

        zip_buffer, org = _build_data_export_zip(org_id)
        org_name = (org.get('business_name') if org else None) or 'YieldGuard'

        success, err = _send_export_email(to_email, zip_buffer, org_name)
        if success:
            flash(f'Backup emailed to {to_email}. Check your inbox.', 'success')
        else:
            flash(f'Email failed: {err}', 'danger')
        return redirect('/data-export')
    except Exception as e:
        print(f"Data export email error: {e}")
        traceback.print_exc()
        flash('Error sending backup email.', 'danger')
        return redirect('/data-export')


@app.route('/data-export/monthly-cron', methods=['POST', 'GET'])
def data_export_monthly_cron():
    """Endpoint hit by Render Cron Job once per month.
    Iterates all orgs, generates backup, emails to primary user.
    Protected by a shared secret in the CRON_SECRET env var."""
    expected = os.environ.get('CRON_SECRET')
    provided = request.headers.get('X-Cron-Secret') or request.args.get('secret')
    if not expected or provided != expected:
        return ('Unauthorized', 401)

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT o.id as org_id, o.business_name, u.email
            FROM organizations o
            JOIN users u ON u.id = (
                SELECT MIN(id) FROM users
                WHERE organization_id = o.id AND is_active = true
            )
        ''')
        orgs = cur.fetchall()
        cur.close()
        conn.close()

        results = {'success': 0, 'failed': 0, 'errors': []}
        for row in orgs:
            try:
                zip_buffer, org = _build_data_export_zip(row['org_id'])
                org_name = row.get('business_name') or 'YieldGuard'
                success, err = _send_export_email(row['email'], zip_buffer, org_name)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append(f"org {row['org_id']}: {err}")
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"org {row['org_id']}: {str(e)}")

        return (json.dumps(results), 200, {'Content-Type': 'application/json'})
    except Exception as e:
        return (f'Cron error: {str(e)}', 500)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
