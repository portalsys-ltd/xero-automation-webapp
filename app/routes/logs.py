# app/routes/logs.py
from flask import Blueprint, jsonify, session
from flask_login import login_required, current_user
from app.models import LogEntry
from app import db

logs_bp = Blueprint('logs', __name__)



def add_log(message, log_type, user_id=None):
    if user_id is None:
        user_id = session['user_id']  # Fallback to session if user_id not provided explicitly
    new_log = LogEntry(user_id=user_id, log_type=log_type, message=message)
    db.session.add(new_log)
    db.session.commit()

# Fetch logs (general and error logs)
@logs_bp.route('/get_logs')
@login_required
def get_logs():
    user_logs = LogEntry.query.filter_by(user_id=current_user.id).order_by(LogEntry.timestamp.desc()).all()
    general_logs = [{"message": log.message, "timestamp": log.timestamp} for log in user_logs if log.log_type == 'general']
    error_logs = [{"message": log.message, "timestamp": log.timestamp} for log in user_logs if log.log_type == 'error']
    
    return jsonify({
        "general": general_logs,
        "errors": error_logs
    })