# app/celert_tasks.py
import os
from app import celery
from app.routes.logs import add_log
from flask import app
import time
from app.models import *
from xero_python.accounting import AccountingApi, ContactPerson, Contact, Contacts, Invoices, Invoice, LineItem, Contact, LineItemTracking
from collections import defaultdict
from sqlalchemy import and_
from decimal import Decimal, ROUND_HALF_UP
import re
from collections import defaultdict
import pandas as pd
import pdfplumber
import csv
from io import StringIO, BytesIO
import matplotlib.pyplot as plt


from flask import session, jsonify, current_app
from app.models import TrackingCategoryModel, User
import io
import base64


from app.xero import (
    get_tracking_categories_from_xero,
    fetch_dom_invoicing_data,
    fetch_file_content,
    bulk_create_bills,
    move_and_rename_file,
    create_folder_if_not_exists,
    extract_statement_date_from_pdf,
    extract_statement_date_from_csv,
    fetch_dom_management_company_data,
    post_dom_sales_invoice_with_attachment,
    get_all_contacts,
    get_invoices_and_credit_notes,
    extract_eden_farm_invoice_data,
    extract_coca_cola_invoice_data,
    extract_textman_invoice_data,
    convert_invoice_to_credit_memo,
    convert_credit_memo_to_invoice,
    assign_tracking_code_to_invoice,
    assign_tracking_code_to_credit_note,
    post_recharge_purchase_invoice_xero,
    post_recharge_sales_invoice_xero, 
    get_inbox_files_from_management_company,
    rename_file

)


@celery.task(bind=True, name='app.celery_tasks.pre_process_dom_purchase_invoices_task')
def pre_process_dom_purchase_invoices_task(self, user_id):
    user = User.query.get(user_id)
    data, management_tenant_id = get_inbox_files_from_management_company(user)

    
    # Initialize an error list to collect issues with nominal codes
    error_messages = []

    processed_file_names = set()  # Track processed file names

    if data:
        tenant_id = data[0]['tenant_id']

        processed_folder = create_folder_if_not_exists('Dominos Supplier Invoices - Processed', tenant_id, user)
        rejected_folder = create_folder_if_not_exists('Dominos Supplier Invoices - Rejected', tenant_id, user)

        rejected_folder_id = rejected_folder.id
        processed_folder_id = processed_folder.id

   

        for file in data[0]['files']:
            file_id = file['file_id']
            file_name = file['file_name']
            mime_type = file['mime_type']
            statement_date = None
            all_codes_exist = True

            # Check if the file name contains 'CustAccountStatementExt.Report'
            if 'CustAccountStatementExt.Report' not in file_name:
                #print(f"Skipping file '{file_name}' as it does not contain 'CustAccountStatementExt.Report'")
                continue

            # Skip files already marked as pre-processed
            if file_name.startswith("PRE PROCESSED"):
                #print(f"Skipping already pre-processed file: {file_name}")
                continue

            # Adjust MIME type based on file extension if incorrect
            if mime_type == 'application/octet-stream':
                if file_name.lower().endswith('.csv'):
                    mime_type = 'text/csv'
                elif file_name.lower().endswith('.pdf'):
                    mime_type = 'application/pdf'
            
            # Extract store number in format "S-XXXXX" from file name
            store_number_match = re.search(r"S-(\d{5})", file_name)
            
            if store_number_match:
                store_number = store_number_match.group(1)
                store_record = TrackingCategoryModel.query.filter_by(user_id=user_id, store_number=store_number).first()

                if not store_record:
                    error_messages.append(f"Store number '{store_number}' in file '{file_name}' does not exist in the database.")
                else:
                    # Check if tenant_name from TrackingCategoryModel exists in DomPurchaseInvoicesTenant
                    tenant_name = store_record.tenant_name
                    tenant_exists = DomPurchaseInvoicesTenant.query.filter_by(user_id=user_id, tenant_name=tenant_name).first()

                    if not tenant_exists:
                        error_messages.append(f"Tenant name '{tenant_name}' for store number '{store_number}' has not been set live.")
            else:
                error_messages.append(f"No valid store number found in file '{file_name}'.")
                continue


            # Process PDF files
            if mime_type == 'application/pdf' or file_name.lower().endswith('.pdf'):
                statement_date = extract_statement_date_from_pdf(tenant_id, file_id, user)
                


       
            # Process CSV files
            elif mime_type == 'text/csv' or file_name.lower().endswith('.csv'):
                statement_date = extract_statement_date_from_csv(tenant_id, file_id, user)
                csv_content = fetch_file_content(user, tenant_id, file_id)
                df = pd.read_csv(csv_content)

                # Check if 'Nominal code' column exists
                if 'Nominal code' not in df.columns:
                    error_messages.append(f"Missing 'Nominal code' column in file '{file_name}'.")
                    all_codes_exist = False
                    continue

                # Check if 'Store' column exists
                if 'Store' not in df.columns:
                    error_messages.append(f"Missing 'Store' column in file '{file_name}'.")
                    all_codes_exist = False
                    continue

                # Loop through each row in the CSV data
                for index, row in df.iterrows():
                    # Check for nominal code
                    nominal_code = row.get('Nominal code')  # Use get() to handle missing values
                    if pd.isna(nominal_code):
                        error_messages.append(f"Missing nominal code in file '{file_name}' at row {index + 1}.")
                        all_codes_exist = False
                        continue

                    # Check if the nominal code exists in the database
                    code_exists = DomNominalCodes.query.filter_by(user_id=user.id, nominal_code=nominal_code).first()
                    if not code_exists:
                        error_messages.append(f"Dominos Group Nominal code '{nominal_code}' does not exist in the database, please add to DOM invoice settings.")
                        all_codes_exist = False
                        continue

                    # Check for store code
                    store_code = str(row.get('Store')).strip()
                    if pd.isna(store_code):
                        error_messages.append(f"Missing store code in file '{file_name}' at row {index + 1}.")
                        all_codes_exist = False
                        continue

                    # Verify store code exists in the TrackingCategoryModel
                    store_exists = TrackingCategoryModel.query.filter_by(user_id=user.id, store_number=store_code).first()
                    if not store_exists:
                        all_codes_exist = False
                        error_messages.append(f"Store code '{store_code}' in file '{file_name}' at row {index + 1} does not exist in the database. Please add this store code to the DOM invoice settings.")


            not_duplicate = True

            # If store_code and statement_date exist, check for duplicates
            if store_number and statement_date:
                # Base file name without extensions
                base_file_name = file_name.replace('.csv', '').replace('.pdf', '')

                processed_log = LogEntry.query.filter(
                    and_(
                        LogEntry.message.like(f"%Store code '{store_number}'%"),
                        LogEntry.message.like(f"%Statement Date '{statement_date}'%"),
                        LogEntry.log_type == "dom_purchase_invoice",
                        LogEntry.user_id == user_id
                    )
                ).first()

                # If already processed or duplicate, move to rejected
                if processed_log or file_name in processed_file_names:

                    add_log(f"File {file_name} (Store: {store_number}, Statement Date: {statement_date}) has already been processed or is a duplicate.",
                            log_type="general", user_id=user_id)
                    rejected_filename = f"{os.path.splitext(file_name)[0]} (Rejected as duplicate or already processed){os.path.splitext(file_name)[1]}"
                    
                    move_and_rename_file(file_id, rejected_folder_id, rejected_filename, mime_type, user, tenant_id)

                    error_messages.append(f"{file_name} has already been processed or is a duplicate, file has been moved to rejected with new file name: {rejected_filename}")
                    
                    not_duplicate = False

        
                # Track the processed file to prevent duplicates in the same batch
                processed_file_names.add(file_name)


            # Rename file to include "Pre Processed" and statement date if it was extracted
            if all_codes_exist and statement_date and store_record and tenant_exists and not_duplicate:
                safe_statement_date = statement_date.replace('/', '-')
                base_name, extension = file_name.rsplit('.', 1)
                new_file_name = f"PRE PROCESSED - {base_name}_{safe_statement_date}.{extension}"

                rename_successful = rename_file(file_id, new_file_name, file_type=mime_type, user=user, tenant_id=tenant_id)
                if rename_successful:
                    print(f"File renamed to {new_file_name}")
                else:
                    error_messages.append(f"Failed to rename file {file_name} to {new_file_name}")

    return {
        'status': 'success' if not error_messages else 'error',
        'message': 'Processing complete.' if not error_messages else 'Processing completed with some errors.',
        'errors': error_messages
    }

@celery.task(bind=True, name='app.celery_tasks.process_dom_purchase_invoices_task')
def process_dom_purchase_invoices_task(self, user_id, week):

    # Load tracking codes from the database
    tracking_codes = {}
    categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()

    for category in categories:
        if category.store_number:  # Only add entries with a valid store number
            tracking_codes[category.store_number] = {
                "tracking_category_id": category.tracking_category_id,
                "tracking_option_id": category.tracking_option_id
            }
            
    add_log(f"Tracking codes loaded from the database", log_type="general", user_id=user_id)


    # Retrieve the user object from the database
    user = User.query.get(user_id)
    data, management_tenant_id = get_inbox_files_from_management_company(user)
    filtered_list = []
    processed_file_count = 0

    # Step 1: Identify live tenants and get expected store numbers for each tenant
    live_tenants = DomPurchaseInvoicesTenant.query.filter_by(user_id=user_id).all()
    expected_stores = []

    # Dictionary to hold tenant and their expected store numbers
    tenant_store_numbers = {}
    tenant_names_map = {}

    for tenant in live_tenants:
        tenant_name = tenant.tenant_name
        store_numbers = TrackingCategoryModel.query.filter_by(user_id=user_id, tenant_name=tenant_name).all()
        store_numbers_list = [store.store_number for store in store_numbers if store.store_number]
        tenant_store_numbers[tenant_name] = store_numbers_list
        expected_stores.extend(store_numbers_list)  # Add all stores for the user

        # Map store numbers to tenant name
        for store_number in store_numbers_list:
            tenant_names_map[store_number] = tenant_name

    # Fetch tenant IDs for easy lookup
    tenant_id_map = {tenant.tenant_name: tenant.tenant_id for tenant in XeroTenant.query.filter_by(user_id=user_id).all()}

    # Dictionary to store files by store number for the specified week
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
        
        # Only process files for the specified week
        if week_identifier != week:
            continue

        # Initialize the store number entry for this week in grouped_files
        if store_number not in grouped_files:
            grouped_files[store_number] = {'csv': None, 'pdf': None}

        # Group the files by type under the store number for this week
        if mime_type == 'text/csv' or file_name.lower().endswith('.csv'):
            grouped_files[store_number]['csv'] = file
        elif mime_type == 'application/pdf' or file_name.lower().endswith('.pdf'):
            grouped_files[store_number]['pdf'] = file

    # Process files for each store in the specified week
    matched_files = []
    for store_number, file_pair in grouped_files.items():
        if file_pair['csv'] and file_pair['pdf']:
            tenant_name = tenant_names_map.get(store_number)
            tenant_id = tenant_id_map.get(tenant_name)

            matched_files.append({
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'store_number': store_number,
                'week': week,
                'statement_date': file_pair['csv']['file_name'].split('_')[-1],  # Extract date from filename
                'csv_file': file_pair['csv'],
                'pdf_file': file_pair['pdf']
            })

    # Populate the filtered list with matched pairs
    for match in matched_files:
        filtered_list.append({
            'tenant_id': match['tenant_id'],
            'tenant_name': match['tenant_name'],
            'store_code': match['store_number'],
            'week': match['week'],
            'statement_date': match['statement_date'],
            'csv_file_id': match['csv_file']['file_id'],
            'pdf_file_id': match['pdf_file']['file_id'],
            'csv_file_name': match['csv_file']['file_name'],
            'pdf_file_name': match['pdf_file']['file_name'],
        })

    total_files = len(filtered_list)

    # Create a list to store the processed data
    processed_files = []

    # Initialize tracking variables
    current_month = None

    current_reference = "A"  # Start with "A"


    # Iterate over the filtered files
    for file_data in filtered_list:

        tenant_id = file_data['tenant_id']
        tenant_name = file_data['tenant_name']
        csv_file_id = file_data['csv_file_id']
        csv_file_name = file_data['csv_file_name']
        pdf_file_id = file_data['pdf_file_id']
        pdf_file_name = file_data['pdf_file_name']
        store_code = file_data['store_code']

        processed_folder = create_folder_if_not_exists('Dominos Supplier Invoices - Processed', management_tenant_id, user)
        processed_folder_id = processed_folder.id


     
    

        # Fetch the content of the CSV file
        csv_content = fetch_file_content(user, management_tenant_id, csv_file_id)
        add_log(f"Fetched CSV content for {csv_file_name}", log_type="general", user_id=user_id)

        try:
            df = pd.read_csv(csv_content)
        except Exception as e:
            add_log(f"Error reading CSV: {csv_file_name}, {str(e)}", log_type="errors", user_id=user_id)
            continue

        expected_columns = [
            'Transaction type', 'Transaction date', 'Invoice number',
            'Order reference', 'Store', 'Total net', 'Total VAT',
            'Total gross', 'Nominal code', 'Nominal amount net',
            'VAT rate', 'Nominal code name', 'Net signed', 'VAT signed'
        ]

        # Check for missing columns
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            add_log(f"Missing columns in CSV: {csv_file_name}: {missing_columns}", log_type="errors", user_id=user_id)
            continue

        # Prepare line items for this CSV file
        line_items = []
        dates = df['Transaction date'].tolist() 
        store_number = df['Store'].iloc[0]
        total_invoice_amount = 0
        current_month = None
        previous_date = None
        

        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            # Extract necessary fields
            description = f"{row['Invoice number']} {row['Nominal code name']}"
            unit_amount = float(row['Net signed']) if 'Net signed' in row and pd.notnull(row['Net signed']) else 0
            # Assign net_signed as Decimal, rounded to two decimal places
            net_signed = (
                Decimal(str(row['Net signed']))
                .quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            )

            # Assign vat_signed as Decimal, rounded to two decimal places
            vat_signed = (
                Decimal(str(row['VAT signed']))
                .quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            )
            unit_amount = unit_amount if unit_amount != 0 else float(row['Total gross']) if 'Total gross' in row and pd.notnull(row['Total gross']) else 0
            transaction_date_str = row['Transaction date']
            
            try:
                # Convert to datetime with flexible parsing
                transaction_date = pd.to_datetime(transaction_date_str, format='%Y/%m/%d', errors='coerce') # CHANGE FOR EMAIL UPLOAD FROM DOMINOS

            
                # Check if parsing failed (returns NaT if failed)
                if pd.isna(transaction_date):
                    add_log(f"Error parsing date '{transaction_date_str}' for tenant '{tenant_name}'", log_type="error", user_id=user_id)
                    continue  # Skip this row if the date is invalid

                
            except Exception as e:
                add_log(f"Error converting date: {str(e)}", log_type="error", user_id=user_id)
                continue

            statement_date = str(transaction_date.strftime('%d/%m/%Y'))

            # Extract month and year for grouping
            row_month = transaction_date.strftime('%Y-%m')  # Format: 'YYYY-MM'
            
            # Determine if we need to start a new invoice
            if current_month is None:
                current_month = row_month
                current_reference = "A"
                add_log(f"Starting new invoice for month: {current_month}", log_type="general", user_id=user_id)
            elif row_month != current_month:
                # Finalize the current invoice
                add_log(f"Finalizing invoice for month: {current_month} with {len(line_items)} line items", log_type="general", user_id=user_id)
                
                processed_files.append({
                    'tenant_id': tenant_id,
                    'tenant_name': tenant_name,
                    'line_items': line_items,
                    'csv_file_name': csv_file_name,
                    'csv_file_id': csv_file_id,
                    'pdf_file_id': pdf_file_id,
                    'pdf_file_name': pdf_file_name,
                    'dates': dates,
                    'store_number': store_number,
                    'total_invoice_amount': total_invoice_amount,
                    'processed_folder_id': processed_folder_id,
                    'store_code': store_code,
                    'statement_date': previous_date,
                    'reference': current_reference

                })
                
                # Reset for the new invoice
                line_items = []
                current_month = row_month
                current_reference = "B"  # Change to "B" for a new month
                total_invoice_amount = 0
                add_log(f"Starting new invoice for month: {current_month}", log_type="general", user_id=user_id)
            
            # Continue processing the current row
            vat_rate = float(row['VAT rate']) if 'VAT rate' in row and pd.notnull(row['VAT rate']) else 0
            tax_rate = 'ZERORATEDINPUT' if vat_rate == 0 else 'INPUT2' if vat_rate == 20 else None
            store_number = str(row['Store']).strip()
            
            # Get tracking info for the store
            tracking_info = tracking_codes.get(store_number)
            if tracking_info is None:
                add_log(f"Warning: No tracking category found for store {store_number}", log_type="error", user_id=user_id)
                continue
            
            # Retrieve the nominal code and account code for the user
            nominal_code = row['Nominal code']
            nominal_code_record = DomNominalCodes.query.filter_by(nominal_code=nominal_code, user_id=user_id).first()
            
            # Check if nominal code exists
            if nominal_code_record:
                # Check if store account code exists for the nominal code
                store_account_code = StoreAccountCodes.query.filter_by(id=nominal_code_record.store_account_code_id, user_id=user_id).first()
                if store_account_code:
                    account_code = store_account_code.account_code
                else:
                    # Log the error and raise an exception to stop the task
                    error_message = f"Store account code not found for nominal code {nominal_code} for user ID {user_id}."
                    add_log(error_message, log_type="errors", user_id=user_id)
                    raise Exception(error_message)  # Stop the task with an error
            else:
                # Log the error and raise an exception to stop the task
                error_message = f"Nominal code {nominal_code} not found in the database for user ID {user_id}."
                add_log(error_message, log_type="errors", user_id=user_id)
                raise Exception(error_message)  # Stop the task with an error
            
            # Create line item tracking
            line_item_tracking = LineItemTracking(
                tracking_category_id=tracking_info['tracking_category_id'],
                tracking_option_id=tracking_info['tracking_option_id']
            )
            
            # Create line item
            line_item = LineItem(
                description=description,
                quantity=1,
                unit_amount=unit_amount,
                account_code=str(account_code),
                tax_type=tax_rate,
                tracking=[line_item_tracking]
            )
            
            line_items.append(line_item)

            total_invoice_amount = total_invoice_amount + vat_signed + net_signed
            
            previous_date = statement_date

        
        # After iterating, finalize the last invoice if there are remaining line items
        if line_items:
            add_log(f"Finalizing invoice for month: {current_month} with {len(line_items)} line items", log_type="general", user_id=user_id)
            
            processed_files.append({
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'line_items': line_items,
                'csv_file_name': csv_file_name,
                'csv_file_id': csv_file_id,
                'pdf_file_id': pdf_file_id,
                'pdf_file_name': pdf_file_name,
                'dates': dates,
                'store_number': store_number,
                'total_invoice_amount': total_invoice_amount,
                'processed_folder_id': processed_folder_id,
                'store_code': store_code,
                'statement_date': statement_date,
                'reference': current_reference
            })
        
        add_log(f"Processed {len(processed_files)} invoices from CSV {csv_file_name}", log_type="general", user_id=user_id) 

    # Group the processed files by tenant_id
    tenant_invoices = defaultdict(list)
    for file_data in processed_files:
        tenant_invoices[file_data['tenant_id']].append(file_data)

    
    

    # Loop through each tenant and process their invoices
    for tenant_id, files in tenant_invoices.items():
        tenant_name = files[0]['tenant_name']  # All files for the same tenant will have the same tenant_name
        invoices_to_create = []

        # Loop through each file for this tenant
        for file_data in files:
            csv_file_name = file_data['csv_file_name']
            pdf_file_name = file_data['pdf_file_name']
            csv_file_id = file_data['csv_file_id']
            pdf_file_id = file_data['pdf_file_id']
            line_items = file_data['line_items']
            dates = file_data['dates']  # Dates extracted from 'Transaction date' column
            store_number = file_data['store_number']  # Extracted from 'Store' column in the CSV file
            total_invoice_amount = file_data['total_invoice_amount']
            processed_folder_id = file_data['processed_folder_id']
            statement_date = file_data['statement_date']
            reference = file_data['reference']
            
            
            # Create invoice structure for this file
            invoice = {
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'invoice_date': dates[0],  # Assuming you want to use the first date as the invoice date
                'line_items': line_items,
                'csv_file_name': csv_file_name,
                'pdf_file_name': pdf_file_name,
                'csv_file_id': csv_file_id,
                'pdf_file_id': pdf_file_id,
                'store_number': store_number,
                'total_invoice_amount': total_invoice_amount,
                'processed_folder_id': processed_folder_id,
                'store_code': store_code,
                'statement_date': statement_date,
                'reference': reference 
    
            }

            invoices_to_create.append(invoice)
        
        # Bulk import these invoices for this tenant
        add_log(f"Preparing to bulk import {len(invoices_to_create)} invoices for tenant {tenant_name} ({tenant_id})", log_type="general", user_id=user_id)
        
        # This is where you'd call the actual function to bulk import to Xero or your system
        try:
            # Assuming you have a function `bulk_create_invoices` for bulk invoice creation

            response = bulk_create_bills(user, tenant_id, invoices_to_create, management_tenant_id)

            processed_file_count = processed_file_count + len(invoices_to_create)
            self.update_state(state='PROGRESS', meta={'current': processed_file_count, 'total': total_files})
            
            if response:  # Proceed only if the bulk_create_bills call was successful
                for invoice in invoices_to_create: 
                    add_log(f"Tenant Name '{tenant_name}' Processed dom purchase invoice '{invoice['csv_file_name']}' with attached pdf '{invoice['pdf_file_name']}', Total invoice amount '{invoice['total_invoice_amount']}', Statement Date '{invoice['statement_date']}', Store code '{invoice['store_number']}'", log_type="dom_purchase_invoice", user_id=user_id)

                    # Move and rename the CSV file
                    new_csv_name = f"{os.path.splitext(invoice['csv_file_name'])[0]}_PROCESSED.csv"
                    move_and_rename_file(invoice['csv_file_id'], invoice['processed_folder_id'], new_csv_name, "CSV", user, management_tenant_id)

                    # Move and rename the PDF file
                    new_pdf_name = f"{os.path.splitext(invoice['pdf_file_name'])[0]}_PROCESSED.pdf"
                    move_and_rename_file(invoice['pdf_file_id'], invoice['processed_folder_id'], new_pdf_name, "PDF", user, management_tenant_id)
                
                add_log(f"Successfully imported {len(invoices_to_create)} invoices for tenant {tenant_name} ({tenant_id})", log_type="general", user_id=user_id)
                
        except Exception as e:
            add_log(f"Failed to bulk import invoices for tenant {tenant_name} ({tenant_id}): {str(e)}", log_type="error", user_id=user_id)

    add_log(f"Dom purchase invoice processing completed for all tenants", log_type="general", user_id=user_id)

    self.update_state(state='SUCCESS', meta={'current': total_files, 'total': total_files})
    return {'current': total_files, 'total': total_files, 'status': 'Task completed!'}




    



@celery.task(bind=True, name='app.celery_tasks.process_dom_purchase_invoices_task_OLD')
def process_dom_purchase_invoices_task_OLD(self, user_id):
     # Load tracking codes from the database
    tracking_codes = {}
    categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()

    for category in categories:
        if category.store_number:  # Only add entries with a valid store number
            tracking_codes[category.store_number] = {
                "tracking_category_id": category.tracking_category_id,
                "tracking_option_id": category.tracking_option_id
            }
            
    add_log(f"Tracking codes loaded from the database", log_type="general", user_id=user_id)

    # Retrieve the user object from the database
    user = User.query.get(user_id)
    data = fetch_dom_invoicing_data(user)
    filtered_list = []

    total_files = sum([len(tenant['files']) for tenant in data])
    processed_file_count = 0



    # Prepare a list to store extracted details from files
    extracted_files = []
    processed_file_names = set()  # Track processed file names

    # Loop over each tenant
    for tenant in data:
        tenant_id = tenant['tenant_id']
        tenant_name = tenant['tenant_name']
        files = tenant['files']
        folders = tenant['folders']

        # Identify the Inbox folder ID
        inbox_folder = next((folder for folder in folders if folder['folder_name'].lower() == 'inbox'), None)
        processed_folder = create_folder_if_not_exists('Dominos Purchase Invoices - Processed', tenant_id, user)
        rejected_folder = create_folder_if_not_exists('Dominos Purchase Invoices -  Rejected', tenant_id, user)

        rejected_folder_id = rejected_folder.id
        processed_folder_id = processed_folder.id
        inbox_folder_id = inbox_folder['folder_id']

        # Filter files to only process those from the inbox
        inbox_files = [file for file in files if file['folder_id'] == inbox_folder_id and file['file_name'].startswith('CustAccountStatementExt.Report')]


        # Process each file
        for file in inbox_files:
            
            file_name = file['file_name']


            store_code_match = re.search(r'S-(\d+)-', file_name)
            store_code = store_code_match.group(1) if store_code_match else None

            # If the MIME type is incorrect, check the file extension
            mime_type = file['mime_type']
            if mime_type == 'application/octet-stream':
                if file_name.lower().endswith('.csv'):
                    mime_type = 'text/csv'
                elif file_name.lower().endswith('.pdf'):
                    mime_type = 'application/pdf'

            

            # If it's a PDF
            if mime_type == 'application/pdf' or file_name.lower().endswith('.pdf'):
                statement_date = extract_statement_date_from_pdf(tenant_id, file['file_id'],user)
                print("pdf: %s" % statement_date)


            # If it's a CSV
            elif mime_type  == 'text/csv' or file_name.lower().endswith('.csv'):
                statement_date = extract_statement_date_from_csv(tenant_id, file['file_id'], user)
                print("csv: %s" % statement_date)
                

            # Now we need to check if this file has already been processed
            if store_code and statement_date:
                # Remove both .csv and .pdf extensions from the file name
                base_file_name = file_name.replace('.csv', '').replace('.pdf', '')

                processed_log = LogEntry.query.filter(
                    and_(
                        LogEntry.message.like(f"%Store code '{store_code}'%"),
                        LogEntry.message.like(f"%Statement Date '{statement_date}'%"),
                        LogEntry.log_type == "dom_purchase_invoice",
                        LogEntry.user_id == user_id
                    )
                ).first()

            
                # If the file has already been processed or is a duplicate, move it to rejected
                if processed_log or file_name in processed_file_names:
                    add_log(f"File {file_name} (Store: {store_code}, Statement Date: {statement_date}) has already been processed or is a duplicate.", log_type="general", user_id=user_id)
                    rejected_filename = f"{os.path.splitext(file_name)[0]} (Rejected as duplicate or already processed){os.path.splitext(file_name)[1]}"

                    # Move and rename the CSV or PDF file to the "Rejected" folder
                    move_and_rename_file(file['file_id'], rejected_folder_id, rejected_filename, mime_type .split('/')[-1].upper(), user, tenant_id)
                    continue  # Skip further processing for this file

                # If not already processed, add to extracted list
                extracted_files.append({
                    'tenant_id': tenant_id,
                    'tenant_name': tenant_name,
                    'file_id': file['file_id'],
                    'file_name': file_name,
                    'store_code': store_code,
                    'statement_date': statement_date,
                    'mime_type': mime_type,
                    'processed_folder_id': processed_folder_id,
                })

                # Add to processed file names set
                processed_file_names.add(file_name)
        

    # Now that we have all the extracted details, let's match the CSV and PDF files
    matched_store_dates = set()  # Track matched store codes and dates to identify duplicates

    for csv_file in [f for f in extracted_files if f['mime_type'] == 'text/csv']:
        # Find the matching PDF file based on store_code and statement_date
        matching_pdf = next((f for f in extracted_files if f['mime_type'] == 'application/pdf' and f['store_code'] == csv_file['store_code'] and f['statement_date'] == csv_file['statement_date']), None)

        store_date_key = (csv_file['store_code'], csv_file['statement_date'])

        if store_date_key in matched_store_dates:
            # Duplicate store number and statement date, move both CSV and PDF to rejected
            add_log(f"Duplicate files found for Store {csv_file['store_code']} on {csv_file['statement_date']}. Moving to Rejected.", log_type="general", user_id=user_id)

            # Move the CSV to rejected
            rejected_csv_filename = f"{os.path.splitext(csv_file['file_name'])[0]} (Rejected as duplicate){os.path.splitext(csv_file['file_name'])[1]}"
            move_and_rename_file(csv_file['file_id'], rejected_folder_id, rejected_csv_filename, "CSV", user, csv_file['tenant_id'])

            # Move the matching PDF to rejected if it exists
            if matching_pdf:
                rejected_pdf_filename = f"{os.path.splitext(matching_pdf['file_name'])[0]} (Rejected as duplicate){os.path.splitext(matching_pdf['file_name'])[1]}"
                move_and_rename_file(matching_pdf['file_id'], rejected_folder_id, rejected_pdf_filename, "PDF", user, matching_pdf['tenant_id'])

            continue  # Skip further processing for this file

        # If no duplicates found, add the matched files to the processed list
        matched_store_dates.add(store_date_key)

        if matching_pdf:
            # Add the CSV and matching PDF file info to the filtered list, along with statement date and store code
            filtered_list.append({
                'tenant_id': csv_file['tenant_id'],
                'tenant_name': csv_file['tenant_name'],
                'csv_file_id': csv_file['file_id'],
                'csv_file_name': csv_file['file_name'],
                'pdf_file_id': matching_pdf['file_id'],
                'pdf_file_name': matching_pdf['file_name'],
                'store_code': csv_file['store_code'],            # Add store code to the filtered list
                'statement_date': csv_file['statement_date'],    # Add statement date to the filtered list
                'processed_folder_id': csv_file['processed_folder_id'],
            })
            add_log(f"Matched CSV {csv_file['file_name']} with PDF {matching_pdf['file_name']} for Store {csv_file['store_code']} on {csv_file['statement_date']}", log_type="general", user_id=user_id)
        else:
            # Log if no matching PDF was found
            add_log(f"No matching PDF found for CSV {csv_file['file_name']} (Store {csv_file['store_code']} on {csv_file['statement_date']})", log_type="error", user_id=user_id)
    


            

    total_files = len(filtered_list)

    # Create a list to store the processed data
    processed_files = []

    # Initialize tracking variables
    current_month = None

    current_reference = "A"  # Start with "A"

    # Iterate over the filtered files
    for file_data in filtered_list:
        tenant_id = file_data['tenant_id']
        tenant_name = file_data['tenant_name']
        csv_file_id = file_data['csv_file_id']
        csv_file_name = file_data['csv_file_name']
        pdf_file_id = file_data['pdf_file_id']
        pdf_file_name = file_data['pdf_file_name']
        processed_folder_id = file_data['processed_folder_id']
        store_code = file_data['store_code']
    

        # Fetch the content of the CSV file
        csv_content = fetch_file_content(user, tenant_id, csv_file_id)
        add_log(f"Fetched CSV content for {csv_file_name}", log_type="general", user_id=user_id)

        try:
            df = pd.read_csv(csv_content)
        except Exception as e:
            add_log(f"Error reading CSV: {csv_file_name}, {str(e)}", log_type="errors", user_id=user_id)
            continue

        expected_columns = [
            'Transaction type', 'Transaction date', 'Invoice number',
            'Order reference', 'Store', 'Total net', 'Total VAT',
            'Total gross', 'Nominal code', 'Nominal amount net',
            'VAT rate', 'Nominal code name', 'Net signed', 'VAT signed'
        ]

        # Check for missing columns
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            add_log(f"Missing columns in CSV: {csv_file_name}: {missing_columns}", log_type="errors", user_id=user_id)
            continue

        # Prepare line items for this CSV file
        line_items = []
        dates = df['Transaction date'].tolist() 
        store_number = df['Store'].iloc[0]
        total_invoice_amount = 0
        current_month = None
        previous_date = None
        

        # Iterate through each row in the DataFrame
        for index, row in df.iterrows():
            # Extract necessary fields
            description = f"{row['Invoice number']} {row['Nominal code name']}"
            unit_amount = float(row['Net signed']) if 'Net signed' in row and pd.notnull(row['Net signed']) else 0
            # Assign net_signed as Decimal, rounded to two decimal places
            net_signed = (
                Decimal(str(row['Net signed']))
                .quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            )

            # Assign vat_signed as Decimal, rounded to two decimal places
            vat_signed = (
                Decimal(str(row['VAT signed']))
                .quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            )
            unit_amount = unit_amount if unit_amount != 0 else float(row['Total gross']) if 'Total gross' in row and pd.notnull(row['Total gross']) else 0
            transaction_date_str = row['Transaction date']
            
            try:
                # Convert to datetime with flexible parsing
                transaction_date = pd.to_datetime(transaction_date_str, format='%Y/%m/%d', errors='coerce') # CHANGE FOR EMAIL UPLOAD FROM DOMINOS

            
                # Check if parsing failed (returns NaT if failed)
                if pd.isna(transaction_date):
                    add_log(f"Error parsing date '{transaction_date_str}' for tenant '{tenant_name}'", log_type="error", user_id=user_id)
                    continue  # Skip this row if the date is invalid

                
            except Exception as e:
                add_log(f"Error converting date: {str(e)}", log_type="error", user_id=user_id)
                continue

            statement_date = str(transaction_date.strftime('%d/%m/%Y'))

            # Extract month and year for grouping
            row_month = transaction_date.strftime('%Y-%m')  # Format: 'YYYY-MM'
            
            # Determine if we need to start a new invoice
            if current_month is None:
                current_month = row_month
                current_reference = "A"
                add_log(f"Starting new invoice for month: {current_month}", log_type="general", user_id=user_id)
            elif row_month != current_month:
                # Finalize the current invoice
                add_log(f"Finalizing invoice for month: {current_month} with {len(line_items)} line items", log_type="general", user_id=user_id)
                
                processed_files.append({
                    'tenant_id': tenant_id,
                    'tenant_name': tenant_name,
                    'line_items': line_items,
                    'csv_file_name': csv_file_name,
                    'csv_file_id': csv_file_id,
                    'pdf_file_id': pdf_file_id,
                    'pdf_file_name': pdf_file_name,
                    'dates': dates,
                    'store_number': store_number,
                    'total_invoice_amount': total_invoice_amount,
                    'processed_folder_id': processed_folder_id,
                    'store_code': store_code,
                    'statement_date': previous_date,
                    'reference': current_reference

                })
                
                # Reset for the new invoice
                line_items = []
                current_month = row_month
                current_reference = "B"  # Change to "B" for a new month
                total_invoice_amount = 0
                add_log(f"Starting new invoice for month: {current_month}", log_type="general", user_id=user_id)
            
            # Continue processing the current row
            vat_rate = float(row['VAT rate']) if 'VAT rate' in row and pd.notnull(row['VAT rate']) else 0
            tax_rate = 'ZERORATEDINPUT' if vat_rate == 0 else 'INPUT2' if vat_rate == 20 else None
            store_number = str(row['Store']).strip()
            
            # Get tracking info for the store
            tracking_info = tracking_codes.get(store_number)
            if tracking_info is None:
                add_log(f"Warning: No tracking category found for store {store_number}", log_type="error", user_id=user_id)
                continue
            
            # Retrieve the nominal code and account code for the user
            nominal_code = row['Nominal code']
            nominal_code_record = DomNominalCodes.query.filter_by(nominal_code=nominal_code, user_id=user_id).first()
            
            # Check if nominal code exists
            if nominal_code_record:
                # Check if store account code exists for the nominal code
                store_account_code = StoreAccountCodes.query.filter_by(id=nominal_code_record.store_account_code_id, user_id=user_id).first()
                if store_account_code:
                    account_code = store_account_code.account_code
                else:
                    # Log the error and raise an exception to stop the task
                    error_message = f"Store account code not found for nominal code {nominal_code} for user ID {user_id}."
                    add_log(error_message, log_type="errors", user_id=user_id)
                    raise Exception(error_message)  # Stop the task with an error
            else:
                # Log the error and raise an exception to stop the task
                error_message = f"Nominal code {nominal_code} not found in the database for user ID {user_id}."
                add_log(error_message, log_type="errors", user_id=user_id)
                raise Exception(error_message)  # Stop the task with an error
            
            # Create line item tracking
            line_item_tracking = LineItemTracking(
                tracking_category_id=tracking_info['tracking_category_id'],
                tracking_option_id=tracking_info['tracking_option_id']
            )
            
            # Create line item
            line_item = LineItem(
                description=description,
                quantity=1,
                unit_amount=unit_amount,
                account_code=str(account_code),
                tax_type=tax_rate,
                tracking=[line_item_tracking]
            )
            
            line_items.append(line_item)

            total_invoice_amount = total_invoice_amount + vat_signed + net_signed
            
            previous_date = statement_date

        
        # After iterating, finalize the last invoice if there are remaining line items
        if line_items:
            add_log(f"Finalizing invoice for month: {current_month} with {len(line_items)} line items", log_type="general", user_id=user_id)
            
            processed_files.append({
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'line_items': line_items,
                'csv_file_name': csv_file_name,
                'csv_file_id': csv_file_id,
                'pdf_file_id': pdf_file_id,
                'pdf_file_name': pdf_file_name,
                'dates': dates,
                'store_number': store_number,
                'total_invoice_amount': total_invoice_amount,
                'processed_folder_id': processed_folder_id,
                'store_code': store_code,
                'statement_date': statement_date,
                'reference': current_reference
            })
        
        add_log(f"Processed {len(processed_files)} invoices from CSV {csv_file_name}", log_type="general", user_id=user_id) 

    # Group the processed files by tenant_id
    tenant_invoices = defaultdict(list)
    for file_data in processed_files:
        tenant_invoices[file_data['tenant_id']].append(file_data)

    # Loop through each tenant and process their invoices
    for tenant_id, files in tenant_invoices.items():
        tenant_name = files[0]['tenant_name']  # All files for the same tenant will have the same tenant_name
        invoices_to_create = []

        # Loop through each file for this tenant
        for file_data in files:
            csv_file_name = file_data['csv_file_name']
            pdf_file_name = file_data['pdf_file_name']
            csv_file_id = file_data['csv_file_id']
            pdf_file_id = file_data['pdf_file_id']
            line_items = file_data['line_items']
            dates = file_data['dates']  # Dates extracted from 'Transaction date' column
            store_number = file_data['store_number']  # Extracted from 'Store' column in the CSV file
            total_invoice_amount = file_data['total_invoice_amount']
            processed_folder_id = file_data['processed_folder_id']
            statement_date = file_data['statement_date']
            reference = file_data['reference']
            
            
            # Create invoice structure for this file
            invoice = {
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'invoice_date': dates[0],  # Assuming you want to use the first date as the invoice date
                'line_items': line_items,
                'csv_file_name': csv_file_name,
                'pdf_file_name': pdf_file_name,
                'csv_file_id': csv_file_id,
                'pdf_file_id': pdf_file_id,
                'store_number': store_number,
                'total_invoice_amount': total_invoice_amount,
                'processed_folder_id': processed_folder_id,
                'store_code': store_code,
                'statement_date': statement_date,
                'reference': reference 
    
            }

            invoices_to_create.append(invoice)
        
        # Bulk import these invoices for this tenant
        add_log(f"Preparing to bulk import {len(invoices_to_create)} invoices for tenant {tenant_name} ({tenant_id})", log_type="general", user_id=user_id)
        
        # This is where you'd call the actual function to bulk import to Xero or your system
        try:
            # Assuming you have a function `bulk_create_invoices` for bulk invoice creation

            response = bulk_create_bills(user, tenant_id, invoices_to_create)

            processed_file_count = processed_file_count + len(invoices_to_create)
            self.update_state(state='PROGRESS', meta={'current': processed_file_count, 'total': total_files})
            
            if response:  # Proceed only if the bulk_create_bills call was successful

                for invoice in invoices_to_create: 
                    add_log(f"Tenant Name '{tenant_name}' Processed dom purchase invoice '{invoice['csv_file_name']}' with attached pdf '{invoice['pdf_file_name']}', Total invoice amount '{invoice['total_invoice_amount']}', Statement Date '{invoice['statement_date']}', Store code '{invoice['store_number']}'", log_type="dom_purchase_invoice", user_id=user_id)

                    # Move and rename the CSV file
                    new_csv_name = f"{os.path.splitext(invoice['csv_file_name'])[0]}_PROCESSED.csv"
                    move_and_rename_file(invoice['csv_file_id'], invoice['processed_folder_id'], new_csv_name, "CSV", user, invoice['tenant_id'])

                    # Move and rename the PDF file
                    new_pdf_name = f"{os.path.splitext(invoice['pdf_file_name'])[0]}_PROCESSED.pdf"
                    move_and_rename_file(invoice['pdf_file_id'], invoice['processed_folder_id'], new_pdf_name, "PDF", user, invoice['tenant_id'])
                
                add_log(f"Successfully imported {len(invoices_to_create)} invoices for tenant {tenant_name} ({tenant_id})", log_type="general", user_id=user_id)
                
        except Exception as e:
            add_log(f"Failed to bulk import invoices for tenant {tenant_name} ({tenant_id}): {str(e)}", log_type="error", user_id=user_id)

    add_log(f"Dom purchase invoice processing completed for all tenants", log_type="general", user_id=user_id)

    self.update_state(state='SUCCESS', meta={'current': total_files, 'total': total_files})
    return {'current': total_files, 'total': total_files, 'status': 'Task completed!'}





@celery.task(bind=True)
def process_dom_sales_invoices_task(self, user_id):
    # Fetch the user from the database
    user = User.query.get(user_id)

    # Fetch the data for the current user's company (including tenant info)
    tenant_data = fetch_dom_management_company_data(user)

    if not tenant_data:
        add_log(f"No tenant data found for user {user_id}.", log_type="error")
        return "No tenant data found."
    
    tracking_codes = {}
    categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()

    for category in categories:
        if category.store_number:  # Only add entries with a valid store number
            tracking_codes[category.store_number] = {
                "tracking_category_id": category.tracking_category_id,
                "tracking_option_id": category.tracking_option_id
            }
            
    add_log(f"Tracking codes loaded from the database", log_type="general", user_id=user_id)

    # Since there's only one tenant, extract it directly
    tenant = tenant_data[0]
    tenant_id = tenant['tenant_id']

    # Get or create the required folders: Inbox, Processed, Rejected
    inbox_folder = create_folder_if_not_exists('Inbox', tenant_id, user)
    processed_folder = create_folder_if_not_exists('Processed', tenant_id, user)
    rejected_folder = create_folder_if_not_exists('Rejected', tenant_id, user)

    if not inbox_folder or not processed_folder or not rejected_folder:
        add_log(f"Failed to retrieve or create one of the necessary folders for tenant {tenant_id}.", log_type="error", user_id=user_id)
        return "Folder creation failed."

    # Process files in the inbox folder
    inbox_files = [file for file in tenant['files'] if file['folder_id'] == inbox_folder.id]

    tenant_contact_details = get_all_contacts(user)
    
    # Map tenant names to contact IDs
    try:
        tenant_to_contact_mapping = map_tenant_to_contact(tenant_contact_details, user_id)
    except Exception as e:
        # If there's an error in finding contacts, log the error and stop the task
        add_log(f"Error in finding contacts: {str(e)}", log_type="error", user_id=user_id)
        return f"Task unsuccessful: {str(e)}"
    

    for file in inbox_files:
        if file['file_name'].startswith('KeyIndicatorsStore'):

            # If the file is a dictionary, extract the file name
            if isinstance(file, dict) and 'file_name' in file:
                original_file_name = file['file_name']

                # Fetch the file content (assuming you are fetching it from Xero API or similar)
                temp_file_path = fetch_file_content(user, tenant_id, file['file_id'])

                # Ensure the file content is processed correctly (this depends on how you're handling the content)
                with open(temp_file_path, 'rb') as f:
                    complete_file_data = f.read()  # Read file content for attaching it later
                

                print(f"Processing file: {original_file_name}")

            else:
                print(f"Unexpected file object type: {type(file)}")

            # Read the data from A4 and A5
            file_info = pd.read_excel(temp_file_path, sheet_name=0, skiprows=2, nrows=2, usecols="A")

            # Initialize variables to hold the start and end dates
            start_date = None
            end_date = None

            # Use regular expressions to extract the dates
            for index, row in file_info.iterrows():
                cell_value = row.iloc[0]  # Get the content of the cell

                if isinstance(cell_value, str):
                    # Look for the "Start Date" pattern
                    match_start = re.search(r"Start Date\s*:\s*(\d{2}-[A-Za-z]{3}-\d{4})", cell_value)
                    if match_start:
                        start_date = match_start.group(1)

                    # Look for the "End Date" pattern
                    match_end = re.search(r"End Date\s*:\s*(\d{2}-[A-Za-z]{3}-\d{4})", cell_value)
                    if match_end:
                        end_date = match_end.group(1)


            # Check if the file has already been processed by matching file name, start date, and end date
            processed_log = LogEntry.query.filter(
                and_(
                    LogEntry.message.like(f"%Processed dom sales invoice '{file['file_name']}'%"),
                    LogEntry.message.like(f"%Start Date '{start_date}'%"),
                    LogEntry.message.like(f"%End Date '{end_date}'%"),
                    LogEntry.log_type == "dom_sales_invoice",
                    LogEntry.user_id == user_id
                )
            ).first()


            if processed_log:
                tenant_id = tenant['tenant_id']
                # Log duplicate and move to rejected folder
                add_log(f"File {file['file_name']} has already been processed.", log_type="general", user_id=user.id)
                rejected_filename = f"{os.path.splitext(file['file_name'])[0]} (Rejected as Duplicate){os.path.splitext(file['file_name'])[1]}"

                # Use the move_and_rename_file function to move and rename the file
                move_and_rename_file(
                    file_id=file['file_id'],
                    new_folder_id=rejected_folder.id,
                    new_name=rejected_filename,
                    file_type="Duplicate",
                    user=user,
                    tenant_id=tenant_id
                )
                continue


            month = start_date.split('-')[1].upper()

            # Assuming start_date is a datetime object
            mileage_description = f"Mileage for {start_date} to {end_date}"
            sales_description = f"Sales for {start_date} to {end_date}"


            # Load Excel file and process data
            df = pd.read_excel(temp_file_path, sheet_name=0, skiprows=6, usecols="A,F,G,AA")
            df['Unit price for 20% VAT'] = (df['Total\nTax'] / 0.2)
            df['Unit price for Zero Rated'] = df['Total\nSales'] - (df['Total\nTax'] / 0.2) - df['Total\nTax']
            df['Unit price for 20% VAT for Mileage'] = (df['Delivered\nCost'] * 0.9) / 1.2
            df['Unit price for Zero Rated for Mileage'] = df['Delivered\nCost'] * 0.1
            df2 = pd.read_excel(temp_file_path, sheet_name=0, skiprows=3, nrows=1, usecols="A")
            date = df2.iloc[0, 0]

            line_items_sales = []
            line_items_mileage = []

            missing_store_errors = []

            # Create sales and mileage line items
            for _, row in df.iterrows():
                store_number = str(int(row['Store\nID'])).strip()
                tracking_info = tracking_codes.get(store_number)
                if not tracking_info:
                    missing_store_errors.append(f"Missing tracking category for store number '{store_number}' in file '{original_file_name}'")
                    continue

                line_item_tracking = LineItemTracking(
                    tracking_category_id=tracking_info['tracking_category_id'],
                    tracking_option_id=tracking_info['tracking_option_id']
                )

                # Sales line item for 20% VAT
                line_items_sales.append(LineItem(
                    description=sales_description,
                    quantity=1,
                    unit_amount=str(row['Unit price for 20% VAT']),
                    account_code='4001',
                    tax_type="OUTPUT2",
                    tracking=[line_item_tracking]
                ))

                # Sales line item for Zero Rated
                line_items_sales.append(LineItem(
                    description=sales_description,
                    quantity=1,
                    unit_amount=str(row['Unit price for Zero Rated']),
                    account_code='4001',
                    tax_type="ZERORATEDOUTPUT",
                    tracking=[line_item_tracking]
                ))

                # Mileage line items for 20% VAT and Zero Rated
                line_items_mileage.append(LineItem(
                    description=mileage_description,
                    quantity=1,
                    unit_amount=str(row['Unit price for 20% VAT for Mileage']),
                    account_code='7302',
                    tax_type="INPUT2",
                    tracking=[line_item_tracking]
                ))

                line_items_mileage.append(LineItem(
                    description=mileage_description,
                    quantity=1,
                    unit_amount=str(row['Unit price for Zero Rated for Mileage']),
                    account_code='7302',
                    tax_type="ZERORATEDINPUT",
                    tracking=[line_item_tracking]
                ))

            # Check if there were any missing store numbers and raise an exception
            if missing_store_errors:
                # Join the missing store numbers into a string for logging and renaming
                error_message = "\n".join(missing_store_errors)
                missing_store_numbers_str = ', '.join([msg.split("'")[1] for msg in missing_store_errors])  # Extract store numbers

                # Log the error with missing store numbers
                add_log(f"Task failed due to missing store numbers: \n{error_message}", log_type="error", user_id=user_id)

                # Move the file to the "Rejected" folder and rename it to indicate missing store numbers
                rejected_filename = f"{os.path.splitext(original_file_name)[0]} (Rejected - Missing Store Numbers {missing_store_numbers_str}){os.path.splitext(original_file_name)[1]}"

                try:
                    move_and_rename_file(
                        file_id=file['file_id'],
                        new_folder_id=rejected_folder.id,
                        new_name=rejected_filename,
                        file_type="Rejected",
                        user=user,
                        tenant_id=tenant_id
                    )
                    add_log(f"File '{original_file_name}' moved to Rejected folder with new name '{rejected_filename}' due to missing store numbers.", log_type="general", user_id=user_id)
                except Exception as e:
                    add_log(f"Error moving file '{original_file_name}' to Rejected folder: {str(e)}", log_type="error", user_id=user_id)

                # Raise an exception to stop the task
                raise Exception(f"Missing store numbers found. Task failed. \n{error_message}")

            # Group sales and mileage line items by tracking_option_id
            
            grouped_sales_items = defaultdict(list)
            grouped_mileage_items = defaultdict(list)

            for item in line_items_sales:
                tracking_option_id = item.tracking[0].tracking_option_id
                grouped_sales_items[tracking_option_id].append(item)

            for item in line_items_mileage:
                tracking_option_id = item.tracking[0].tracking_option_id
                grouped_mileage_items[tracking_option_id].append(item)


            # Fetch all tracking categories for the user
            tracking_categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()

            # Fetch all Xero tenants for the user
            xero_tenants = XeroTenant.query.filter_by(user_id=user_id).all()

            # Create a mapping of tenant_name to tenant_id using XeroTenant model
            tenant_name_to_id = {tenant.tenant_name: tenant.tenant_id for tenant in xero_tenants}

            # Create a mapping of tenant_id to tenant_name from the XeroTenant model
            tenant_id_to_name = {tenant.tenant_id: tenant.tenant_name for tenant in XeroTenant.query.filter_by(user_id=user_id).all()}

            # Create a mapping of tracking_option_id to tenant_id using tenant_name from TrackingCategoryModel
            tracking_category_to_tenant = {}
            for category in tracking_categories:
                tenant_id = tenant_name_to_id.get(category.tenant_name)
                if tenant_id:
                    tracking_category_to_tenant[category.tracking_option_id] = tenant_id
                else:
                    add_log(f"No tenant found for tenant name '{category.tenant_name}' in XeroTenant model.", log_type="error", user_id=user_id)


            # Post grouped sales line items to corresponding tenants
            for tracking_option_id, sales_items in grouped_sales_items.items():

                # Query to get the store name (tracking_category_option) based on tracking_option_id
                store_name = db.session.query(TrackingCategoryModel.tracking_category_option).filter_by(
                    tracking_option_id=tracking_option_id
                ).scalar()  # .scalar() fetches the single column value directly
    

                # Find the tenant_id associated with this tracking_option_id
                tenant_id = tracking_category_to_tenant.get(tracking_option_id)
                tenant_name = tenant_id_to_name.get(tenant_id)
                if not tenant_id:
                    add_log(f"No tenant found for tracking category {tracking_option_id}", log_type="error", user_id=user_id)
                    continue

                # Retrieve the contact IDs from the tenant-to-contact mapping
                tenant_contacts = tenant_to_contact_mapping.get(tenant_name, {})
                sales_contact_id = tenant_contacts.get("SALES")
                
                if not sales_contact_id:
                    add_log(f"Missing SALES contact for tenant {tenant_name}.", log_type="error", user_id=user_id)
                    continue

                # Post sales invoice
                try:
                    invoice_id_sales = post_dom_sales_invoice_with_attachment(
                        tenant_id, sales_items, sales_contact_id, original_file_name, complete_file_data, user, end_date, start_date, month, store_name, invoice_type="Domino's Sales",
                    )
                    add_log(f"Posted sales invoice {invoice_id_sales} for tenant {tenant_id} with tracking category {tracking_option_id}.", log_type="general", user_id=user_id)
                    add_log(f"Tenant Name '{tenant_name}' Processed dom sales invoice '{original_file_name}', Supplier type 'DOMINOS SALES', Start Date '{start_date}', End Date '{end_date}'", log_type="dom_sales_invoice", user_id=user_id)
                except Exception as e:
                    add_log(f"Error posting sales invoice for tenant {tenant_id} with tracking category {tracking_option_id}: {str(e)}", log_type="error", user_id=user_id)

            # Post grouped mileage line items to corresponding tenants
            for tracking_option_id, mileage_items in grouped_mileage_items.items():

                # Query to get the store name (tracking_category_option) based on tracking_option_id
                store_name = db.session.query(TrackingCategoryModel.tracking_category_option).filter_by(
                    tracking_option_id=tracking_option_id
                ).scalar()  # .scalar() fetches the single column value directly
    

                # Find the tenant_id associated with this tracking_option_id
                tenant_id = tracking_category_to_tenant.get(tracking_option_id)
                tenant_name = tenant_id_to_name.get(tenant_id)
                if not tenant_id:
                    add_log(f"No tenant found for tracking category {tracking_option_id}", log_type="error", user_id=user_id)
                    continue

                # Retrieve the contact IDs from the tenant-to-contact mapping
                tenant_contacts = tenant_to_contact_mapping.get(tenant_name, {})
                mileage_contact_id = tenant_contacts.get("MILEAGE")
                
                if not mileage_contact_id:
                    add_log(f"Missing MILEAGE contact for tenant {tenant_name}.", log_type="error", user_id=user_id)
                    continue

                # Post mileage invoice
                try:
                    invoice_id_mileage = post_dom_sales_invoice_with_attachment(
                        tenant_id, mileage_items, mileage_contact_id, original_file_name, complete_file_data, user, end_date, start_date, month, store_name, invoice_type="Domino's Mileage",
                    )
                    add_log(f"Posted mileage invoice {invoice_id_mileage} for tenant {tenant_id} with tracking category {tracking_option_id}.", log_type="general", user_id=user_id)
                    add_log(f"Tenant Name '{tenant_name}' Processed dom sales invoice '{original_file_name}', Supplier type 'DOMINOS MILEAGE', Start Date '{start_date}', End Date '{end_date}'", log_type="dom_sales_invoice", user_id=user_id)
                except Exception as e:
                    add_log(f"Error posting mileage invoice for tenant {tenant_id} with tracking category {tracking_option_id}: {str(e)}", log_type="error", user_id=user_id)


            

            # Post sales and mileage invoices to Xero
            # post_invoices_to_xero(api_client, tenant_id, line_items_sales, line_items_mileage, temp_file_path, file, date, user_id)

            # Move the file to the processed folder after posting
            try:
                tenant_id = tenant['tenant_id']
                rejected_filename = f"{os.path.splitext(file['file_name'])[0]} (PROCESSED){os.path.splitext(file['file_name'])[1]}"
                move_and_rename_file(
                    file_id=file['file_id'],
                    new_folder_id=processed_folder.id,
                    new_name=rejected_filename,
                    file_type="Sales Invoice Processed File",
                    user=user,
                    tenant_id=tenant_id
                )
                add_log(f"File {file['file_name']} moved to Processed folder.", log_type="general", user_id=user_id)
            except Exception as e:
                add_log(f"Error moving file {file['file_name']} to Processed folder: {str(e)}", log_type="error", user_id=user_id)

    return "Processing complete"


def map_tenant_to_contact(tenant_contact_details, user_id):
    # Initialize a dictionary to store the mapping of tenant names to contact IDs
    tenant_to_contact_mapping = {}

    # Create a mapping of tenant names to their contact details
    tenant_name_to_contacts = {tenant['tenant_name']: tenant['contacts'] for tenant in tenant_contact_details}

    # Initialize a list to capture the error details for all missing contacts
    missing_contacts_info = []

    # Fetch tracking categories for the user, filter by tenants present in TrackingCategoryModel
    tracking_categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()

    # Get the tenant names from TrackingCategoryModel
    tracked_tenant_names = {category.tenant_name for category in tracking_categories}

    # Loop through tenant names that are in the tracking categories to map tenant to contact IDs
    for tenant_name, contacts in tenant_name_to_contacts.items():
        # Only proceed if this tenant exists in the TrackingCategoryModel
        if tenant_name not in tracked_tenant_names:
            continue
        
        # Initialize contact IDs for "DOMINOS MILEAGE" and "DOMINOS SALES"
        dominos_mileage_contact_id = None
        dominos_sales_contact_id = None

        # Loop through the contacts to find the matching contact names
        for contact in contacts:
            if contact['contact_name'] == "MILEAGE":
                dominos_mileage_contact_id = contact['contact_id']
            elif contact['contact_name'] == "SALES":
                dominos_sales_contact_id = contact['contact_id']

        # Check if both contacts were found
        if dominos_mileage_contact_id and dominos_sales_contact_id:
            # If both are found, map them to the tenant name
            tenant_to_contact_mapping[tenant_name] = {
                "MILEAGE": dominos_mileage_contact_id,
                "SALES": dominos_sales_contact_id
            }
        else:
            # If one or both contacts are missing, capture the details
            missing_contacts = []
            if not dominos_mileage_contact_id:
                missing_contacts.append("MILEAGE")
            if not dominos_sales_contact_id:
                missing_contacts.append("SALES")

            # Append the missing contact info for this tenant
            missing_contacts_info.append(
                f"Tenant '{tenant_name}' is missing: {', '.join(missing_contacts)}"
            )

    # If there are any missing contacts, log them all as one error
    if missing_contacts_info:
        # Join all missing contact messages into one error message
        error_message = "The following tenants are missing contacts:\n" + "\n".join(missing_contacts_info)
        add_log(error_message, log_type="error", user_id=user_id)
        raise Exception(error_message)

    return tenant_to_contact_mapping




def load_tracking_codes_by_store_postcodes(user_id):
    tracking_codes = {}
    
    try:
        # Ensure the user is logged in
        if user_id:
            # Query the database to get all tracking categories for the current user
            tracking_categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()

            # Loop through the results and populate the tracking_codes dictionary
            for category in tracking_categories:
                if category.store_postcode:  # Only add entries with a valid store postcode
                    tracking_codes[category.store_postcode] = {
                        "tenant_name": category.tenant_name,
                        "tracking_category_id": category.tracking_category_id,
                        "tracking_category_name": category.tracking_category_name,
                        "tracking_category_option": category.tracking_category_option,
                        "tracking_option_id": category.tracking_option_id
                    }
        else:
            add_log("User not logged in. No tracking codes can be loaded.", log_type="errors")
    except Exception as e:
        add_log(f"Error loading tracking codes from database: {str(e)}", log_type="errors")
    
    return tracking_codes



def extract_store_postcode(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()

            # Extract Ship-to & Sold-to Address
            ship_to_pattern = r"Ship-to & Sold-to\s*:\s*(.*?)\n\d{6,}"
            ship_to_match = re.search(ship_to_pattern, text, re.DOTALL)
            if ship_to_match:
                ship_to_address = ship_to_match.group(1).strip()
                
                # Extract the postcode from the address
                postcode_pattern = r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b"
                postcode_match = re.search(postcode_pattern, ship_to_address)
                
                if postcode_match:
                    postcode = postcode_match.group(0)
                else:
                    postcode = "Postcode not found"
            else:
                postcode = "Ship-to & Sold-to address not found"

            # Return only the postcode
            return {
                postcode
            }
    
    return {
        None
    }

def extract_store_number(pdf_path):
    # Updated pattern to match "S" followed by 5 digits and capture only the digits
    store_number_pattern = r"S(\d{5})"  # Captures the 5 digits after "S"
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()

            # Search for the store number pattern in the extracted text
            store_number_match = re.search(store_number_pattern, text)
            
            if store_number_match:
                # Extract only the digits (group 1) from the match
                store_number = store_number_match.group(1).strip()
                return store_number
            else:
                return "Store number not found"
    
    return None

def extract_postcode_from_delivery_address(pdf_path):
    # Pattern to capture a UK postcode (e.g., LA4 4DD)
    postcode_pattern = r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()

            # Search for "Delivery Address" and extract the text below it
            delivery_address_pattern = r"Delivery Address(.*?)(\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b)"
            delivery_address_match = re.search(delivery_address_pattern, text, re.DOTALL)
            
            if delivery_address_match:
                # Extract the postcode from the delivery address
                postcode_match = re.search(postcode_pattern, delivery_address_match.group(0))
                if postcode_match:
                    return postcode_match.group(0).strip()
                else:
                    return "Postcode not found"
    
    return None



def load_tracking_codes_by_store_number(user_id):
    tracking_codes = {}
    try:
        # Ensure the user is logged in
        if user_id:
            # Query the database to get all tracking categories for the current user based on store number
            tracking_categories = TrackingCategoryModel.query.filter_by(user_id).all()

            # Loop through the results and populate the tracking_codes dictionary
            for category in tracking_categories:
                if category.store_number:  # Only add entries with a valid store number
                    tracking_codes[category.store_number] = {
                        "tenant_name": category.tenant_name,
                        "tracking_category_id": category.tracking_category_id,
                        "tracking_category_name": category.tracking_category_name,
                        "tracking_category_option": category.tracking_category_option,
                        "tracking_option_id": category.tracking_option_id
                    }
        else:
            add_log("User not logged in. No tracking codes can be loaded.", log_type="errors")
    except Exception as e:
        add_log(f"Error loading tracking codes from database: {str(e)}", log_type="errors")
    
    return tracking_codes

def check_if_credit_memo(pdf_path):
    # Pattern to capture the phrase "CREDIT MEMO"
    credit_memo_pattern = r"CREDIT MEMO"

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()

            # Check for "CREDIT MEMO" in the text
            if re.search(credit_memo_pattern, text):
                print("Found 'CREDIT MEMO' in the PDF. Skipping processing.")
                return True  # It's a credit memo
            else:
                print("This is not a credit memo.")
    
    return False  # It's not a credit memo



@celery.task(bind=True)
def process_cocacola_task(self, user_id):
    # Fetch the user by user_id
    user = User.query.get(user_id)

    # Fetch all tenants associated with this user that are in Live Dom Tenants List
    tenants = DomPurchaseInvoicesTenant.query.filter_by(user_id=user_id).all()

    # Extract tenant names to pass into the invoice fetching function
    tenant_names = [tenant.tenant_name for tenant in tenants]

    # Fetch Coca-Cola invoices without tracking categories for these tenants
    cocacola_invoices= get_invoices_and_credit_notes(user, tenant_names, "COCACOLA")

    # Calculate total invoices to process
    total_invoices = len(cocacola_invoices)
    processed_invoices = 0

    invoices_to_process = [invoice for invoice in cocacola_invoices if invoice['xero_type'] == "invoice"]
    credit_notes_to_process = [invoice for invoice in cocacola_invoices if invoice['xero_type'] == "credit_note"]

    # Track errors
    errors = []

    # Process the invoices for each tenant
    for invoice in invoices_to_process:
        data = extract_coca_cola_invoice_data(user, invoice)
        
        # Check if there are any errors
        invoice_errors = data.get("errors", [])
        if invoice_errors:
            errors.extend(invoice_errors)
            add_log(f"Error processing invoice {invoice['invoice_id']}: {', '.join(invoice_errors)}", log_type="error", user_id=user.id)
            continue  # Skip this invoice due to errors

        # Process based on invoice type
        if data["invoice_type"] == "credit_memo":
            # Call the function to convert the credit memo to an invoice
            error = convert_invoice_to_credit_memo(data, user)
        else:
            # Call the function to assign tracking code to the invoice
            error = assign_tracking_code_to_invoice(data, user)

        # Update progress after processing each invoice
        processed_invoices += 1
        self.update_state(state='PROGRESS', meta={'current': processed_invoices, 'total': total_invoices})
    
    # Process the credit notes for each tenant
    for credit_note in credit_notes_to_process:
        data = extract_coca_cola_invoice_data(user, credit_note)

        
        # Check if there are any errors
        credit_note_errors = data.get("errors", [])
        if credit_note_errors:
            errors.extend(credit_note_errors)
            add_log(f"Error processing credit note {credit_note['invoice_id']}: {', '.join(credit_note_errors)}", log_type="error", user_id=user.id)
            continue  # Skip this credit note due to errors


        # Process based on credit note type
        if data["invoice_type"] == "invoice":
            # Call the function to convert the credit memo to an invoice
            error = convert_credit_memo_to_invoice(data, user)
        else:
            # Call the function to assign tracking code to the credit memo
            error = assign_tracking_code_to_credit_note(data, user)
            add_log(f"Credit note {credit_note['invoice_id']} is a valid credit memo.", log_type="general", user_id=user.id)

        # Update progress after processing each invoice
        processed_invoices += 1
        self.update_state(state='PROGRESS', meta={'current': processed_invoices, 'total': total_invoices})

    # Final message
    self.update_state(state='SUCCESS', meta={
        'current': total_invoices,
        'total': total_invoices,
        'message': "Coca-Cola invoice processing completed",
        'errors': errors  # Ensure errors are included in the meta response
    })
    
    # Prepare final message
    if errors:
        return {
            "message": f"Coca-Cola invoice processing completed with {len(errors)} errors.",
            "errors": errors
        }
    else:
        return {"message": "Coca-Cola invoice processing completed successfully for all tenants."}



@celery.task(bind=True)
def process_textman_task(self, user_id):
    # Fetch the user by user_id
    user = User.query.get(user_id)
    
    # Fetch all tenants associated with this user from DomPurchaseInvoicesTenant
    tenants = DomPurchaseInvoicesTenant.query.filter_by(user_id=user_id).all()

    # Extract tenant names to pass into the invoice fetching function
    tenant_names = [tenant.tenant_name for tenant in tenants]

    # Fetch Textman invoices without tracking categories for these tenants
    textman_invoices = get_invoices_and_credit_notes(user, tenant_names, "TextMan")


    # Count total invoices
    total_invoices = len(textman_invoices)
    processed_invoices = 0

    # Track errors
    errors = []

    invoices_to_process = [invoice for invoice in textman_invoices if invoice['xero_type'] == "invoice"]
    credit_notes_to_process = [invoice for invoice in textman_invoices if invoice['xero_type'] == "credit_note"]

    # Process the invoices for each tenant
    for invoice in invoices_to_process:
        data = extract_textman_invoice_data(user, invoice)

        # Check if there are any errors
        invoice_errors = data.get("errors", [])
        if invoice_errors:
            errors.extend(invoice_errors)
            add_log(f"Error processing Textman invoice {invoice['invoice_id']}: {', '.join(invoice_errors)}", log_type="error", user_id=user.id)
            continue  # Skip this invoice due to errors

        # Process based on invoice type
        if data["invoice_type"] == "credit_memo":
            # Call the function to convert the credit memo to an invoice
            error = convert_invoice_to_credit_memo(data, user)
        else:
            # Call the function to assign tracking code to the invoice
            error = assign_tracking_code_to_invoice(data, user)

        # Log progress for this invoice
        processed_invoices += 1
        self.update_state(state='PROGRESS', meta={'current': processed_invoices, 'total': total_invoices})

    # Process the credit notes for each tenant
    for credit_note in credit_notes_to_process:
        data = extract_textman_invoice_data(user, credit_note)

        # Check if there are any errors
        credit_note_errors = data.get("errors", [])
        if credit_note_errors:
            errors.extend(credit_note_errors)
            add_log(f"Error processing Textman credit note {credit_note['invoice_id']}: {', '.join(credit_note_errors)}", log_type="error", user_id=user.id)
            continue  # Skip this credit note due to errors

        # Process based on credit note type
        if data["invoice_type"] == "invoice":
            # Call the function to convert the credit memo to an invoice
            error = convert_credit_memo_to_invoice(data, user)
        else:
            # Call the function to assign tracking code to the credit note
            error = assign_tracking_code_to_credit_note(data, user)

        # Log progress for this credit note
        processed_invoices += 1
        self.update_state(state='PROGRESS', meta={'current': processed_invoices, 'total': total_invoices})

    # Final message
    self.update_state(state='SUCCESS', meta={
        'current': total_invoices,
        'total': total_invoices,
        'message': "Textman invoice processing completed",
        'errors': errors  # Ensure errors are included in the meta response
    })
    
    # Prepare final message
    if errors:
        return {
            "message": f"Textman invoice processing completed with {len(errors)} errors.",
            "errors": errors
        }
    else:
        return {"message": "Textman invoice processing completed successfully for all tenants."}



@celery.task(bind=True)
def process_eden_farm_task(self, user_id):
    # Fetch the user by user_id
    user = User.query.get(user_id)

    # Fetch all tenants associated with this user from DomPurchaseInvoicesTenant
    tenants = DomPurchaseInvoicesTenant.query.filter_by(user_id=user_id).all()

    # Extract tenant names to pass into the invoice fetching function
    tenant_names = [tenant.tenant_name for tenant in tenants]

    # Fetch Eden Farm invoices and credit notes for these tenants
    eden_farm_invoices= get_invoices_and_credit_notes(user, tenant_names, "Eden Farm")


    # Calculate total invoices to process
    total_invoices = len(eden_farm_invoices)
    processed_invoices = 0

    # Separate invoices and credit notes
    invoices_to_process = [invoice for invoice in eden_farm_invoices if invoice['xero_type'] == "invoice"]
    credit_notes_to_process = [invoice for invoice in eden_farm_invoices if invoice['xero_type'] == "credit_note"]

    # Track errors
    errors = []

    # Process the invoices for each tenant
    for invoice in invoices_to_process:
        data = extract_eden_farm_invoice_data(user, invoice)
        
        # Check if there are any errors
        invoice_errors = data.get("errors", [])
        if invoice_errors:
            errors.extend(invoice_errors)
            add_log(f"Error processing Eden Farm invoice {invoice['invoice_id']}: {', '.join(invoice_errors)}", log_type="error", user_id=user.id)
            continue  # Skip this invoice due to errors

        # Process based on invoice type
        if data["invoice_type"] == "credit_memo":
            # Call the function to convert the credit memo to an invoice
            error = convert_invoice_to_credit_memo(data, user)
        else:
            # Call the function to assign tracking code to the invoice
            error = assign_tracking_code_to_invoice(data, user)

        # Update progress after processing each invoice
        processed_invoices += 1
        self.update_state(state='PROGRESS', meta={'current': processed_invoices, 'total': total_invoices})
    
    # Process the credit notes for each tenant
    for credit_note in credit_notes_to_process:
        data = extract_eden_farm_invoice_data(user, credit_note)

        # Check if there are any errors
        credit_note_errors = data.get("errors", [])
        if credit_note_errors:
            errors.extend(credit_note_errors)
            add_log(f"Error processing Eden Farm credit note {credit_note['invoice_id']}: {', '.join(credit_note_errors)}", log_type="error", user_id=user.id)
            continue  # Skip this credit note due to errors

        # Process based on credit note type
        if data["invoice_type"] == "invoice":
            # Call the function to convert the credit memo to an invoice
            error = convert_credit_memo_to_invoice(data, user)
        else:
            # Call the function to assign tracking code to the credit memo
            error = assign_tracking_code_to_credit_note(data, user)
            add_log(f"Eden Farm credit note {credit_note['invoice_id']} is a valid credit memo.", log_type="general", user_id=user.id)

        # Update progress after processing each credit note
        processed_invoices += 1
        self.update_state(state='PROGRESS', meta={'current': processed_invoices, 'total': total_invoices})

    # Final message and update state
    self.update_state(state='SUCCESS', meta={
        'current': total_invoices,
        'total': total_invoices,
        'message': "Eden Farm invoice processing completed",
        'errors': errors  # Ensure errors are included in the meta response
    })
    
    # Prepare the final response message
    if errors:
        return {
            "message": f"Eden Farm invoice processing completed with {len(errors)} errors.",
            "errors": errors
        }
    else:
        return {"message": "Eden Farm invoice processing completed successfully for all tenants."}



# Celery task to upload invoices to Xero
@celery.task(bind=True)
def upload_recharge_invoices_xero_task(self, user_id, purchase_csv_content, breakdown_csv_content, sales_invoices_csv_content):
    try:
        # Load user from the database
        user = User.query.get(user_id)

        if not user:
            raise ValueError(f"User with id {user_id} not found.")
        
        # Retrieve the stored CSV content from the session
        purchase_csv_content = purchase_csv_content
        breakdown_csv_content = breakdown_csv_content
        sales_invoices_csv_content = sales_invoices_csv_content

        if not purchase_csv_content or not breakdown_csv_content:
            raise ValueError("CSV content is missing in session")

        # Process the purchase CSV into a DataFrame
        purchase_csv_io = io.StringIO(purchase_csv_content)
        purchase_df = pd.read_csv(purchase_csv_io)
        
        sales_csv_io = io.StringIO(sales_invoices_csv_content)
        sales_invoice_df  = pd.read_csv(sales_csv_io)

        # Check if the 'DueDate' and 'InvoiceNumber' columns exist and log errors if missing
        due_date = None
        invoice_number = None

        # Check if the 'DueDate' and 'InvoiceNumber' columns exist and log errors if missing
        due_date = purchase_df.get('DueDate', pd.Series([None])).iloc[0]
        invoice_number = purchase_df.get('InvoiceNumber', pd.Series([None])).iloc[0]

        if pd.isna(due_date):
            raise ValueError("DueDate is missing in the 'purchase_csv'.")
        if pd.isna(invoice_number):
            raise ValueError("InvoiceNumber is missing in the 'purchase_csv'.")
        
         # Process the breakdown CSV into a DataFrame
        breakdown_df = pd.read_csv(io.StringIO(breakdown_csv_content))
    

        invoices_data = {}
        sales_invoices_data = {}
        sales_invoice_numbers = {}
        invoice_numbers = {}
        company_codes = {}

        # Query all valid companies from TrackingCategoryModel
        valid_companies = db.session.query(TrackingCategoryModel.tenant_name).distinct().all()
        valid_companies = {company[0] for company in valid_companies}  # Set of valid company names

        # Lists to store the result
        successful_companies = []
        failed_companies = []
        already_processed_companies = []
        sales_successful_companies = []
        sales_failed_companies = []
        sales_already_processed_companies = []


        # Iterate over the DataFrame rows to create line items for Xero
        for _, row in purchase_df.iterrows():
            company_name = row.get('Company Name')
            invoice_number = row.get('InvoiceNumber')  # Extract the InvoiceNumber for each respective company row

            if company_name not in valid_companies:
                current_app.logger.error(f"Company '{company_name}' not found in tracking categories. Skipping row.")
                failed_companies.append(company_name)
                continue

            description = row.get('Description')
            quantity = float(row.get('Quantity', 1))  # Default to 1 if not provided
            unit_amount = float(row.get('UnitAmount'))
            account_code = row.get('AccountCode')
            tracking_option_name = row.get('TrackingOption1')

            # Query the TrackingCategoryModel to get the tracking_category_id and tracking_option_id
            tracking_record = TrackingCategoryModel.query.filter_by(tracking_category_option=tracking_option_name).first()
            if not tracking_record:
                current_app.logger.error(f"Tracking option '{tracking_option_name}' not found for company '{company_name}'.")
                failed_companies.append(company_name)
                continue

            if company_name not in invoices_data:
                invoices_data[company_name] = []
                invoice_numbers[company_name] = invoice_number

            line_item_tracking = LineItemTracking(
                tracking_category_id=tracking_record.tracking_category_id,
                tracking_option_id=tracking_record.tracking_option_id
            )

            invoices_data[company_name].append(
                LineItem(
                    description=description,
                    quantity=quantity,
                    unit_amount=unit_amount,
                    account_code=account_code,
                    tracking=[line_item_tracking]
                )
            )

        # Iterate over the sales innvoie DataFrame rows to create line items for Xero
        for _, row in sales_invoice_df.iterrows():
            company_name = row.get('ContactName')
            invoice_number = row.get('InvoiceNumber')  # Extract the InvoiceNumber for each respective company row
            description = row.get('Description')
            quantity = float(row.get('Quantity', 1))  # Default to 1 if not provided
            unit_amount = float(row.get('UnitAmount'))
            account_code = row.get('AccountCode')


            # Query the TrackingCategoryModel to get the tracking_category_id and tracking_option_id
            tracking_record = TrackingCategoryModel.query.filter_by(tracking_category_option=tracking_option_name).first()
            if not tracking_record:
                current_app.logger.error(f"Tracking option '{tracking_option_name}' not found for company '{company_name}'.")
                failed_companies.append(company_name)
                continue

            if company_name not in sales_invoices_data:
                sales_invoices_data[company_name] = []
                sales_invoice_numbers[company_name] = invoice_number

            line_item_tracking = LineItemTracking(
                tracking_category_id=tracking_record.tracking_category_id,
                tracking_option_id=tracking_record.tracking_option_id
            )

            sales_invoices_data[company_name].append(
                LineItem(
                    description=description,
                    quantity=quantity,
                    unit_amount=unit_amount,
                    account_code=account_code,
                    tax_type="OUTPUT2"
                )
            )
        

        contact_data = get_all_contacts(user)
        tenant_info = {}
        sales_tenant_info = {}


        for company in invoices_data.keys():
            company_stripped = company.strip().lower()
            tenant_contact_info = next((tenant for tenant in contact_data if tenant['tenant_name'].lower().strip() == company_stripped), None)

            if not tenant_contact_info:
                failed_companies.append(company)
                continue

            tenant_id = tenant_contact_info['tenant_id']
            contact_record = next((contact for contact in tenant_contact_info['contacts'] if user.username in contact['contact_name']), None)

            if not contact_record:
                failed_companies.append(company)
                continue

            tenant_info[company] = {
                'tenant_id': tenant_id,
                'contact_id': contact_record['contact_id']
            }

        # Second loop to match tenant names with user.company_name and store tenant ID, contact ID, and company name using the company as the key
        for company in sales_invoices_data.keys():
            company_stripped = company.strip().lower()
            # Find tenant matching the user's company name
            tenant_contact_info = next((tenant for tenant in contact_data if tenant['tenant_name'].lower().strip() == user.company_name.lower().strip()), None)

            if not tenant_contact_info:
                add_log(f"Missing [{company} company contact in management company]", log_type="error", user_id=user.id)
                raise ValueError(f"Missing [{company} company contact in management company]")
                

            # Check if any contact names in the tenant match the company name in sales_invoices_data.keys()
            contact_record = next((contact for contact in tenant_contact_info['contacts'] if contact['contact_name'].lower().strip() == company_stripped), None)

            if not contact_record:
                add_log(f"Missing [{company} company contact in management company]", log_type="error", user_id=user.id)
                raise ValueError(f"Missing [{company} company contact in management company]")
                

            # Store the data in sales_tenant_info with the company as the key
            sales_tenant_info[company] = {
                'tenant_id': tenant_contact_info['tenant_id'],
                'contact_id': contact_record['contact_id'],
                'company_name': company
            }

        # Print or use sales_tenant_info as needed
        print("Sales Tenant Info:", sales_tenant_info)

        
        
        

        # Check if all companies have valid company codes
        for company in invoices_data.keys():
            # Get the company code from the Company table for the current company
            company_record = Company.query.filter_by(company_name=company, user_id=user_id).first()
            if not company_record or not company_record.company_code:
                current_app.logger.error(f"Company record or company code not found for '{company}'.")
                raise ValueError(f"Missing company code for company [{company}]")

            # Store the company code for later use in the invoice processing
            company_codes[company] = company_record.company_code



        for company, line_items in invoices_data.items():
            invoice_number = invoice_numbers[company]
            file_name = (company + " Breakdown " + "(" + invoice_number + ")" + ".csv" )
            tenant_id = tenant_info[company]['tenant_id']
            contact_id = tenant_info[company]['contact_id']


            # Retrieve the pre-validated company code
            company_code = company_codes.get(company)

            # Filter the breakdown DataFrame for the current company code
            filtered_breakdown_df = breakdown_df[breakdown_df['Company Code'] == company_code]

            # Sort the filtered breakdown DataFrame by 'Account Code Per Business'
            filtered_breakdown_df = filtered_breakdown_df.sort_values(by='Account Code Per Business')

            # Calculate the net total per account code per business
            net_total_summary = filtered_breakdown_df.groupby(['Account Code Per Business'])['Net'].sum().reset_index()

            # Create the summary table with two columns: Account Code and Net Total
            summary_table = pd.DataFrame({
                'Account Code Per Business': net_total_summary['Account Code Per Business'],
                'Net Total': net_total_summary['Net']
            })

            # Insert a blank row for spacing
            blank_row = pd.DataFrame([[''] * len(filtered_breakdown_df.columns)], columns=filtered_breakdown_df.columns)

            # Append blank row after the filtered breakdown data
            summary_df = pd.concat([filtered_breakdown_df, blank_row], ignore_index=True)

            # Convert the filtered breakdown DataFrame to CSV
            breakdown_csv_content = summary_df.to_csv(index=False)

            # Convert the summary table to CSV
            summary_csv_content = summary_table.to_csv(index=False)

            # Combine the breakdown CSV content and the summary table
            csv_content = breakdown_csv_content + '\n\n' + summary_csv_content

            # Convert the combined content to a byte array
            byte_array_csv_content = csv_content.encode('utf-8')


            try:
                result = post_recharge_purchase_invoice_xero(
                    tenant_id=tenant_id,
                    line_items=line_items,
                    contact_id=contact_id,
                    file_name=file_name,
                    file_content=byte_array_csv_content,
                    user=user,
                    end_date=due_date,
                    invoice_number=invoice_number
                )

                if result == "ALREADY CREATED":
                    already_processed_companies.append(company)
                else:
                    successful_companies.append(company)

            except Exception as e:
                current_app.logger.error(f"Error creating invoice for company '{company}': {str(e)}")
                failed_companies.append(company)

        
        for company, line_items in sales_invoices_data.items():
            try:
                # Retrieve invoice number and tenant/contact info
                invoice_number = invoice_numbers[company]
                file_name = f"{company} Breakdown ({invoice_number}).csv"
                tenant_id = sales_tenant_info[company]['tenant_id']
                contact_id = sales_tenant_info[company]['contact_id']

                # Retrieve the pre-validated company code
                company_code = company_codes.get(company)

                # Filter the breakdown DataFrame for the current company code
                filtered_breakdown_df = breakdown_df[breakdown_df['Company Code'] == company_code]

                # Sort the filtered breakdown DataFrame by 'Account Code Per Business'
                filtered_breakdown_df = filtered_breakdown_df.sort_values(by='Account Code Per Business')

                # Calculate the net total per account code per business
                net_total_summary = filtered_breakdown_df.groupby(['Account Code Per Business'])['Net'].sum().reset_index()

                # Create the summary table with two columns: Account Code and Net Total
                summary_table = pd.DataFrame({
                    'Account Code Per Business': net_total_summary['Account Code Per Business'],
                    'Net Total': net_total_summary['Net']
                })

                # Insert a blank row for spacing
                blank_row = pd.DataFrame([[''] * len(filtered_breakdown_df.columns)], columns=filtered_breakdown_df.columns)

                # Append blank row after the filtered breakdown data
                summary_df = pd.concat([filtered_breakdown_df, blank_row], ignore_index=True)

                # Convert the filtered breakdown DataFrame to CSV
                breakdown_csv_content = summary_df.to_csv(index=False)

                # Convert the summary table to CSV
                summary_csv_content = summary_table.to_csv(index=False)

                # Combine the breakdown CSV content and the summary table
                csv_content = breakdown_csv_content + '\n\n' + summary_csv_content

                # Convert the combined content to a byte array
                byte_array_csv_content = csv_content.encode('utf-8')

                # Post the sales invoice to Xero
                result = post_recharge_sales_invoice_xero(
                    tenant_id=tenant_id,
                    line_items=line_items,
                    contact_id=contact_id,
                    file_name=file_name,
                    file_content=byte_array_csv_content,
                    user=user,
                    end_date=due_date,
                    invoice_number=invoice_number
                )

                # Track result
                if result == "ALREADY CREATED":
                    sales_already_processed_companies.append(company)
                else:
                    sales_successful_companies.append(company)

            except Exception as e:
                current_app.logger.error(f"Error creating invoice for company '{company}': {str(e)}")
                sales_failed_companies.append(company)


        # Step 4: Remove duplicates from both lists
        successful_companies = list(set(successful_companies))
        failed_companies = list(set(failed_companies))
        already_processed_companies = list(set(already_processed_companies))


        # Step 4: Log the successful and failed companies
        log_message = f"Successful companies: {', '.join(successful_companies)}; Failed companies: {', '.join(failed_companies)}"
        add_log(log_message, log_type="recharge_invoices", user_id=user.id)

        return {
            "status": "success",
            "message": "Invoices uploaded to Xero successfully.",
            "successful_companies": successful_companies,
            "failed_companies": failed_companies,
            "already_processed_companies": already_processed_companies,
            "sales_successful_companies": sales_successful_companies,
            "sales_failed_companies": sales_failed_companies,
            "sales_already_processed_companies": sales_already_processed_companies

        }

    except Exception as e:
        current_app.logger.error(f"Error in upload_recharge_invoices_xero_task: {str(e)}")
        self.update_state(state='FAILURE', meta={'error': str(e), 'exc_type': type(e).__name__})
        raise