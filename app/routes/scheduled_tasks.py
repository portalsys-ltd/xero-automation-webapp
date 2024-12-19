# app/routes/scheduled_tasks.py

from app import db 
from flask import Blueprint, render_template, session, jsonify, request, flash, redirect, url_for, send_file, Response
from io import StringIO
from sqlalchemy.orm import joinedload
from app.models import *
from app.routes.logs import add_log
from flask_login import login_required
from app.routes.auth import user_login_required
import pandas as pd
import os, zipfile, subprocess
from datetime import datetime
import calendar
import re
import zipfile
from io import BytesIO
import csv
import io
from xero_python.accounting import Invoice, LineItem, Attachment,LineItemTracking
from flask_login import current_user
from app import celery
from app.xero import get_supplier_invoices_workflow_counts
from app.celery_tasks import process_cocacola_task, process_eden_farm_task, process_textman_task
from datetime import timedelta
from datetime import datetime
from sqlalchemy.sql import func
import json
from sqlalchemy.sql import func





# Define the blueprint
scheduled_tasks_bp = Blueprint('scheduled_tasks', __name__, url_prefix='/scheduled_tasks')


@scheduled_tasks_bp.route('/workflows_counts', methods=['GET'])
@user_login_required
def get_scheduled_workflow_counts_route():
    response = get_supplier_invoices_workflow_counts(current_user)  # Call the function
    return response

@scheduled_tasks_bp.route('/trigger_coca-cola', methods=['POST'])
@user_login_required
def coca_cola_process_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object
    task = process_cocacola_task.delay(user_id)
    return jsonify({"message": "Coca-Cola invoice processing has started.", "task_id": task.id}), 202  


@scheduled_tasks_bp.route('/trigger_text-man', methods=['POST'])
@user_login_required
def text_man_process_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object
    task = process_textman_task.delay(user_id)
    return jsonify({"message": "Text Management invoice processing has started.", "task_id": task.id}), 202


@scheduled_tasks_bp.route('/trigger_eden-farm', methods=['POST'])
@user_login_required
def eden_farm_process_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object
    task = process_eden_farm_task.delay(user_id)
    return jsonify({"message": "Eden Farm invoice processing has started.", "task_id": task.id}), 202




@scheduled_tasks_bp.route('/last_run_details', methods=['GET'])
@user_login_required
def get_last_run_details():
    from datetime import timedelta, datetime

    # Fetch last run details for each invoice type
    invoice_types = ["Coca-Cola", "Eden Farm", "Text Management"]
    last_run_details = {}

    for invoice_type in invoice_types:
        # Query the latest record for the given invoice type
        last_record = (
            SupplierInvoiceRecord.query
            .filter_by(invoice_type=invoice_type)
            .order_by(SupplierInvoiceRecord.run_time.desc())
            .first()
        )

        if last_record:
            # Set next scheduled run to the next day at 11:30 PM
            next_run = (datetime.now() + timedelta(days=1)).replace(hour=23, minute=30, second=0, microsecond=0)

            # Check if there were errors in any invoices processed on the same day as the last run
            run_date = last_record.run_time.date()
            has_errors = (
                SupplierInvoiceRecord.query
                .filter_by(invoice_type=invoice_type)
                .filter(func.date(SupplierInvoiceRecord.run_time) == run_date)
                .filter(SupplierInvoiceRecord.errors.isnot(None))
                .count() > 0
            )



            last_run_details[invoice_type.lower().replace(" ", "_")] = {
                "last_run_time": last_record.run_time.strftime("%Y-%m-%d %H:%M:%S"),
                "errors": "❌" if has_errors else "✅",
                "next_scheduled_run": next_run.strftime("%Y-%m-%d %H:%M:%S")
            }
        else:
            # Default for no last record
            next_run = (datetime.now() + timedelta(days=1)).replace(hour=23, minute=30, second=0, microsecond=0)
            last_run_details[invoice_type.lower().replace(" ", "_")] = {
                "last_run_time": "N/A",
                "errors": "N/A",
                "next_scheduled_run": next_run.strftime("%Y-%m-%d %H:%M:%S")
            }



    return jsonify(last_run_details)


@scheduled_tasks_bp.route('/filter_records/<string:task_type>', methods=['POST'])
@user_login_required
def filter_records(task_type):
    
    # Parse JSON data from the request
    data = request.get_json()
    filter_date = data.get('date', None)
    errors_only = data.get('errorsOnly', False)

    # Map task_type to the appropriate invoice type
    task_type_map = {
        'coca-cola': "Coca-Cola",
        'eden-farm': "Eden Farm",
        'text-man': "Text Management"
    }

    invoice_type = task_type_map.get(task_type)
    if not invoice_type:
        return jsonify({"error": "Invalid task type"}), 400

    # Query the records
    query = SupplierInvoiceRecord.query.filter_by(invoice_type=invoice_type)

    if filter_date:
        # Filter by date
        try:
            date_object = datetime.strptime(filter_date, '%Y-%m-%d').date()
            query = query.filter(func.date(SupplierInvoiceRecord.run_time) == date_object)
        except ValueError:
            return jsonify({"error": "Invalid date format"}), 400

    if errors_only:
        # Filter records with errors
        query = query.filter(SupplierInvoiceRecord.errors.isnot(None))

    # Fetch records
    records = query.order_by(SupplierInvoiceRecord.run_time.desc()).all()

    # Format the records for JSON response
    records_data = [
        {
            "store_name": record.store_name or "Unknown",
            "invoice_type": record.invoice_type,
            "invoice_number": record.invoice_number,
            "errors": record.errors,
            "run_time": record.run_time.strftime("%Y-%m-%d %H:%M:%S") if record.run_time else "N/A",
            "triggered_by": record.triggered_by or "Unknown"
        }
        for record in records
    ]

    return jsonify({"records": records_data})




