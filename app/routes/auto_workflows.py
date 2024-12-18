# app/routes/auto_workflows.py

from flask import Blueprint, jsonify, session, request
from app.xero import xero_token_required, aggregate_auto_workflows_data, get_inbox_files_from_management_company
from app.routes.logs import add_log
from xero_python.exceptions import HTTPStatusException
from flask_login import current_user
from app.celery_tasks import pre_process_dom_purchase_invoices_task, process_dom_purchase_invoices_task, process_dom_sales_invoices_task, process_cocacola_task, process_eden_farm_task, process_textman_task, update_invoice_record_task
from app.models import TrackingCategoryModel, User, DomPurchaseInvoicesTenant, InvoiceRecord, TaskStatus
import pandas as pd
import re
import json
from app.routes.auth import user_login_required
from app import db 

auto_workflows_bp = Blueprint('auto_workflows', __name__, url_prefix='/auto_workflows')


@auto_workflows_bp.route('/check_pre_processed_invoices', methods=['GET'])
@user_login_required
def check_pre_processed_invoices():
    user_id = current_user.id
    # Retrieve the user object from the database
    user = User.query.get(user_id)
    data, management_tenant_id = get_inbox_files_from_management_company(user)


    # Step 1: Identify live tenants and get expected store numbers for each tenant
    live_tenants = DomPurchaseInvoicesTenant.query.filter_by(user_id=user_id).all()
    expected_store_numbers = []

    # Dictionary to hold tenant names and their expected store numbers
    tenant_store_numbers = {}

    for tenant in live_tenants:
        tenant_name = tenant.tenant_name
        store_numbers = TrackingCategoryModel.query.filter_by(user_id=user_id, tenant_name=tenant_name).all()
        store_numbers_list = [store.store_number for store in store_numbers if store.store_number]
        tenant_store_numbers[tenant_name] = store_numbers_list
        expected_store_numbers.extend(store_numbers_list)  # Collect all store numbers for the user

    # Dictionary to store files by store number and week
    grouped_files = {}

    for file in data[0]['files']:
        file_id = file['file_id']
        file_name = file['file_name']
        mime_type = file['mime_type']

        # Check if the file is pre-processed
        if not file_name.startswith("PRE PROCESSED"):
            continue

        # Extract the store number from the file name (format: S-XXXXX)
        store_number_match = re.search(r"S-(\d{5})", file_name)
        store_number = store_number_match.group(1) if store_number_match else None
        if not store_number:
            print(f"No store number found in file '{file_name}', skipping...")
            continue

        # Extract the statement date from the file name (assuming it's in the format `DD-MM-YYYY`)
        statement_date_match = re.search(r"(\d{2}-\d{2}-\d{4})", file_name)
        statement_date = statement_date_match.group(1) if statement_date_match else None
        if not statement_date:
            print(f"No statement date found in file '{file_name}', skipping...")
            continue

        # Convert statement date to week identifier (e.g., ISO week)
        week_identifier = pd.to_datetime(statement_date, format='%d-%m-%Y').isocalendar()[1]
        
        # Create a unique key based on week identifier
        key = week_identifier

        # Initialize the week key in the dictionary if not present
        if key not in grouped_files:
            grouped_files[key] = {}

        # Initialize the store number entry for this week
        if store_number not in grouped_files[key]:
            grouped_files[key][store_number] = {'csv': None, 'pdf': None}

        # Group the files by type under the store number for this week
        if mime_type == 'text/csv' or file_name.lower().endswith('.csv'):
            grouped_files[key][store_number]['csv'] = file
        elif mime_type == 'application/pdf' or file_name.lower().endswith('.pdf'):
            grouped_files[key][store_number]['pdf'] = file

    # Structure the result dictionary for each week
    weeks_summary = {}
    for week, stores in grouped_files.items():
        # Check for matched pairs
        matched_pairs = []
        missing_stores = []
        for store_number in expected_store_numbers:
            file_pair = stores.get(store_number)
            if file_pair and file_pair['csv'] and file_pair['pdf']:
                matched_pairs.append({
                    'store_number': store_number,
                    'statement_date': file_pair['csv']['file_name'].split('_')[-1],  # Extract date from filename
                    'csv_file_id': file_pair['csv']['file_id'],
                    'pdf_file_id': file_pair['pdf']['file_id'],
                    'csv_file_name': file_pair['csv']['file_name'],
                    'pdf_file_name': file_pair['pdf']['file_name']
                })
            else:
                # Track missing store numbers
                missing_stores.append(store_number)

        # Add to weekly summary
        weeks_summary[week] = {
            'matched_invoice_count': len(matched_pairs),
            'matched_pairs': matched_pairs,
            'expected_invoice_count': len(expected_store_numbers),
            'missing_store_numbers': missing_stores
        }


    return jsonify({
        'status': 'success',
        'message': 'Pre-processed purchase invoices checked and grouped successfully.',
        'weeks_summary': weeks_summary
    })


@auto_workflows_bp.route('/data', methods=['GET'])
@user_login_required
def auto_workflows_data():
    if not current_user:
        add_log("User ID not found in session.", log_type="errors")
        return jsonify({"error": "User not authenticated."}), 401
    
    try:
        data = aggregate_auto_workflows_data(current_user)
        return data
    except Exception as e:
        add_log(f"Error aggregating auto workflows data: {e}", log_type="errors")
        return jsonify({"error": "Failed to fetch auto workflows data."}), 500


@auto_workflows_bp.route('/pre_process_dom_purchase_invoices', methods=['GET'])
@user_login_required
def pre_process_dom_purchase_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object

    task_type = "pre_process_dom_purchase_invoices"
    task = pre_process_dom_purchase_invoices_task.apply_async(args=[user_id])
    # Save task details to the database
    try:
        new_task = TaskStatus(
            task_id=task.id,
            user_id=user_id,
            task_type=task_type,
            status='in_progress'
        )
        db.session.add(new_task)
        db.session.commit()

        print(f"TaskStatus entry created for task_id: {task.id}")
    except Exception as e:
        print(f"Error saving TaskStatus to database: {e}")
        return jsonify({"status": "error", "message": "Failed to save task to database."}), 500

  

    if task:
        return jsonify({'task_id': task.id, 'message': 'Task started!'})
    else:
        return jsonify({"error": "Task failed to start"}), 500  
    


@auto_workflows_bp.route('/process_dom_purchase_invoices', methods=['GET'])
@user_login_required
def process_dom_purchase_invoices_route():
    user_id = current_user.id
    week = request.args.get('week')  # Get the week parameter from the query string

    if not week:
        return jsonify({"error": "Week parameter is missing"}), 400

    task = process_dom_purchase_invoices_task.apply_async(args=[user_id, int(week)])
    if task:
        return jsonify({'task_id': task.id, 'message': 'Task started!'})
    else:
        return jsonify({"error": "Task failed to start"}), 500

    
@auto_workflows_bp.route('/process_dom_sales_invoices', methods=['GET'])
@user_login_required
def process_dom_sales_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object
    task = process_dom_sales_invoices_task.apply_async(args=[user_id])
    if task:
        return jsonify({'task_id': task.id, 'message': 'Task started!'})
    else:
        return jsonify({"error": "Task failed to start"}), 500
    


@auto_workflows_bp.route('/process_coca-cola_invoices', methods=['POST'])
@user_login_required
def coca_cola_process_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object
    task = process_cocacola_task.delay(user_id)
    return jsonify({"message": "Coca-Cola invoice processing has started.", "task_id": task.id}), 202  # 202 indicates the task is accepted for processing


@auto_workflows_bp.route('/process_text-man_invoices', methods=['POST'])
@user_login_required
def text_man_process_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object
    task = process_textman_task.delay(user_id)
    return jsonify({"message": "Text Management invoice processing has started.", "task_id": task.id}), 202


@auto_workflows_bp.route('/process_eden-farm_invoices', methods=['POST'])
@user_login_required
def eden_farm_process_invoices_route():
    user_id = current_user.id  # Pass the user's ID instead of the whole object
    task = process_eden_farm_task.delay(user_id)
    return jsonify({"message": "Eden Farm invoice processing has started.", "task_id": task.id}), 202
    

@auto_workflows_bp.route('/active_task_status', methods=['GET'])
@user_login_required
def active_task_status():
    user_id = current_user.id

    # Query the most recent active task for the user, restricted to specific task types
    active_task = (
        TaskStatus.query.filter(
            TaskStatus.user_id == user_id,
            TaskStatus.task_type.in_(['pre_process_dom_purchase_invoices', 'process_dom_purchase']),  # Filter by task types
            TaskStatus.status.in_(['in_progress', 'failed'])  # Only in-progress or pending tasks
        )
        .order_by(TaskStatus.created_at.desc())  # Order by most recent
        .limit(1)
        .first()
    )

    if active_task and active_task.status in ['in_progress', 'pending', 'failed']:
        # Check the real-time task status from Celery
        task = pre_process_dom_purchase_invoices_task.AsyncResult(active_task.task_id)
        if task.state in ['SUCCESS', 'FAILURE']:
            # Update task status in the database
            active_task.status = 'completed' if task.state == 'SUCCESS' else 'failed'
            db.session.commit()

        return jsonify({
            'task_id': active_task.task_id,
            'task_type': active_task.task_type,
            'status': active_task.status,
            'progress': task.info.get('progress', 0) if task.state == 'PROGRESS' else 100 if task.state == 'SUCCESS' else 0,
            'message': task.info.get('message', 'Task is in progress...') if task.state == 'PROGRESS' else 'Task completed.' if task.state == 'SUCCESS' else 'Task failed.',
            'state': task.state,
        })

    return jsonify({'status': 'no_active_task'})


@auto_workflows_bp.route('/task_status/<task_id>', methods=['GET'])
def task_status(task_id):

    task = pre_process_dom_purchase_invoices_task.AsyncResult(task_id)

    if task.state == 'PENDING':
        # If pending, try the process task instead
        task = process_dom_purchase_invoices_task.AsyncResult(task_id)
 
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'progress': 0
        }
    elif task.state == 'PROGRESS':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'progress': int((task.info.get('current', 0) / task.info.get('total', 1)) * 100)
        }
    elif task.state == 'SUCCESS':
        # Handle the case where task.result or task.info is None
        result = task.result or {}
        response = {
            'state': task.state,
            'progress': 100,  # Task is complete, so progress is 100%
            'message': result.get('message', 'Task completed successfully'),
            'errors': result.get('errors', [])  # Ensure errors are included in the response
        }
    elif task.state == 'FAILURE':
        response = {
            'state': task.state,
            'progress': 0,
            'errors': str(task.info),  # The error message from the task
            'traceback': str(task.traceback)  # You can include the traceback for debugging purposes
        }
    else:
        response = {
            'state': task.state,
            'progress': 0,
            'message': 'Unknown task state',
            'errors': []
        }

    return jsonify(response)



@auto_workflows_bp.route('/start_update_invoice_record', methods=['POST'])
@user_login_required
def start_update_invoice_record():
    try:
        print("Route triggered successfully!")  # Debugging statement
        
        # Retrieve the current user
        user_id = current_user.id
        print(f"Current User ID: {user_id}")  # Debugging

        if not user_id:
            return jsonify({"error": "User not found"}), 404

        # Trigger the Celery task
        task = update_invoice_record_task.apply_async(args=[user_id])
        print(f"Task {task.id} started for user {user_id}")  # Debugging task trigger

        return jsonify({"task_id": task.id, "message": "Task triggered successfully"}), 202

    except Exception as e:
        print(f"Error occurred: {str(e)}")  # Print error to console
        return jsonify({"error": str(e)}), 500





@auto_workflows_bp.route('/load_invoice_summary', methods=['POST'])
@user_login_required
def load_invoice_summary():
    try:
        # Get week and year from the request body
        data = request.get_json()
        week_number = data.get("week_number")
        year = data.get("year")


        # Validate inputs
        if not week_number or not year:
            return jsonify({"error": "Week and year are required"}), 400

        # Fetch all tenant names for the current user
        tenant_names = [
            tenant.tenant_name
            for tenant in DomPurchaseInvoicesTenant.query.filter_by(user_id=current_user.id).all()
        ]

    
        # Fetch all stores for those tenants from TrackingCategoryModel
        store_details = TrackingCategoryModel.query.filter(
            TrackingCategoryModel.tenant_name.in_(tenant_names)
        ).all()

        print(store_details)

        # Prepare store data for matching
        store_records = []
        for store in store_details:
            # Check for a matching record in InvoiceRecord
            existing_record = InvoiceRecord.query.filter_by(
                week_number=week_number,
                year=year,
                store_number=store.store_number
            ).first()

            # Determine status: green tick or red X
            status = "✅" if existing_record else "❌"

            if not store.store_number or not re.match(r"^\d{5}$", store.store_number):
                continue
                
            # Append store record to the response list
            store_records.append({
                "store_name": store.tracking_category_option,  # Corrected column
                "store_number": store.store_number,
                "tenant_name": store.tenant_name,  # Use this as a placeholder if needed
                "status": status
            })

        return jsonify({"data": store_records}), 200

    except Exception as e:
        print(f"Error loading invoice summary: {str(e)}")
        return jsonify({"error": "Failed to load summary"}), 500
    

@auto_workflows_bp.route('/load_invoice_summary_sales', methods=['POST'])
@user_login_required
def load_invoice_summary_sales():
    try:
        import re

        # Get week and year from the request body
        data = request.get_json()
        week_number = data.get("week_number")
        year = data.get("year")

        # Validate inputs
        if not week_number or not year:
            return jsonify({"error": "Week and year are required"}), 400

        # Fetch all tenant names for the current user
        tenant_names = [
            tenant.tenant_name
            for tenant in DomPurchaseInvoicesTenant.query.filter_by(user_id=current_user.id).all()
        ]

        # Fetch all stores for those tenants from TrackingCategoryModel
        store_details = TrackingCategoryModel.query.filter(
            TrackingCategoryModel.tenant_name.in_(tenant_names)
        ).all()

        # Prepare store data for matching
        store_records = []
        for store in store_details:
            # Ignore invalid store numbers
            if not store.store_number or not re.match(r"^\d{5}$", store.store_number):
                continue

            # Check for mileage record
            mileage_record = InvoiceRecord.query.filter_by(
                week_number=week_number,
                year=year,
                store_number=store.store_number,
                invoice_type="mileage"
            ).first()

            # Check for sales record
            sales_record = InvoiceRecord.query.filter_by(
                week_number=week_number,
                year=year,
                store_number=store.store_number,
                invoice_type="sales"
            ).first()

            # Append store record to the response list
            store_records.append({
                "store_name": store.tracking_category_option,
                "store_number": store.store_number,
                "tenant_name": store.tenant_name,
                "mileage_status": "✅" if mileage_record else "❌",
                "sales_status": "✅" if sales_record else "❌"
            })

        return jsonify({"data": store_records}), 200

    except Exception as e:
        print(f"Error loading invoice summary: {str(e)}")
        return jsonify({"error": "Failed to load summary"}), 500
