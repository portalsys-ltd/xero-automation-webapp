# app/routes/auth.py

from flask import Blueprint, request, render_template, redirect, url_for, session, jsonify, flash
from werkzeug.security import check_password_hash, generate_password_hash
from app.models import User
from app import db
from functools import wraps
from flask_login import login_user,logout_user, current_user

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

@auth_bp.route('/user-login', methods=['GET', 'POST'])
def user_login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()

        if user and check_password_hash(user.password, password):  # Use hashed password check
            login_user(user)  # This logs in the user
            session['user_logged_in'] = True
            session['user_id'] = user.id  # Store user ID in session
            return jsonify({"success": True, "redirect_url": url_for('main.home')})  # Send success and redirect URL
        else:
            return jsonify({"success": False, "message": "Invalid credentials. Please try again."})  # Return failure message
    return render_template('login.html')

@auth_bp.route('/user-logout')
def user_logout():
    logout_user()
    session.pop('user_logged_in', None)
    # Clear the entire session to remove all user-related data
    return redirect(url_for('auth.user_login'))

def user_login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_logged_in' not in session or not session['user_logged_in']:
            return redirect(url_for('auth.user_login'))
        return f(*args, **kwargs)
    return decorated_function
