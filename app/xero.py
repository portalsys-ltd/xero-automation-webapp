# app/xero.py

from flask import Blueprint, redirect, url_for, session, jsonify, request, current_app
import requests
from app import db
from app.routes.logs import LogEntry, add_log
from flask_oauthlib.contrib.client import OAuth
from flask_login import current_user
from xero_python.api_client import ApiClient, Configuration
from xero_python.api_client.oauth2 import OAuth2Token
import json
import time
from functools import wraps
from app.models import XeroTenant, Company, DomPurchaseInvoicesTenant, StoreAccountCodes, TrackingCategoryModel
from enum import Enum
from datetime import datetime, timedelta
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
import re
import pdfplumber
import pandas as pd
import time
import dateutil.parser
import os
from dotenv import load_dotenv


from xero_python.accounting import AccountingApi, Account, Accounts, AccountType, Allocation, Allocations, BatchPayment, BatchPayments, BankTransaction, BankTransactions, BankTransfer, BankTransfers, Contact, Contacts, ContactGroup, ContactGroups, ContactPerson, CreditNote, CreditNotes, Currency, Currencies, CurrencyCode, Employee, Employees, ExpenseClaim, ExpenseClaims, HistoryRecord, HistoryRecords, Invoice, Invoices, Item, Items, LineAmountTypes, LineItem, Payment, Payments, PaymentService, PaymentServices, Phone, Purchase, Quote, Quotes, Receipt, Receipts, RepeatingInvoice, RepeatingInvoices, Schedule, TaxComponent, TaxRate, TaxRates, TaxType, TrackingCategory, TrackingCategories, TrackingOption, TrackingOptions, User, Users, LineItemTracking
from xero_python.assets import AssetApi, Asset, AssetStatus, AssetStatusQueryParam, AssetType, BookDepreciationSetting
from xero_python.project import ProjectApi, Amount, ChargeType, Projects, ProjectCreateOrUpdate, ProjectPatch, ProjectStatus, ProjectUsers, Task, TaskCreateOrUpdate, TimeEntryCreateOrUpdate
from xero_python.payrollau import PayrollAuApi, Employees, Employee, EmployeeStatus,State, HomeAddress
from xero_python.payrolluk import PayrollUkApi, Employees, Employee, Address, Employment
from xero_python.payrollnz import PayrollNzApi, Employees, Employee, Address, Employment, EmployeeLeaveSetup
from xero_python.file import FilesApi
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.exceptions import AccountingBadRequestException, PayrollUkBadRequestException
from xero_python.identity import IdentityApi
from xero_python.utils import getvalue
from xero_python.exceptions import HTTPStatusException
from xero_python.accounting import AccountingApi, Invoice, Invoices, LineItem, Contact
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
import dateutil.parser


# Blueprint for Xero-specific routes
xero_bp = Blueprint('xero', __name__, url_prefix='/xero')

# Set up OAuth for Xero
oauth = OAuth()

load_dotenv()




def get_xero_client_for_user(user):
    # Get client_id and client_secret from Flask app config
    client_id = current_app.config.get("CLIENT_ID")
    client_secret = current_app.config.get("CLIENT_SECRET")

    # Create an OAuth2 token for the user
    xero_app = oauth.remote_app(
        name="xero",
        version="2",
        client_id=client_id,
        client_secret=client_secret,
        endpoint_url="https://api.xero.com/",
        authorization_url="https://login.xero.com/identity/connect/authorize",
        access_token_url="https://identity.xero.com/connect/token",
        refresh_token_url="https://identity.xero.com/connect/token",
        scope="accounting.attachments accounting.reports.read files.read payroll.payslip accounting.budgets.read accounting.contacts.read files offline_access accounting.transactions.read accounting.settings accounting.settings.read accounting.attachments.read assets.read payroll.employees accounting.transactions projects payroll.timesheets profile projects.read openid accounting.contacts accounting.journals.read payroll.settings assets email payroll.payruns",
        
    )

    # Create an ApiClient instance for the user
    api_client = ApiClient(
        Configuration(
            oauth2_token=OAuth2Token(
                client_id=client_id,
                client_secret=client_secret,
            ),
        ),
        pool_threads=1,
    )

    # Add the tokensaver for the specific user
    @xero_app.tokensaver
    @api_client.oauth2_token_saver
    def store_user_oauth2_token(token):
        if user:
            user.xero_token = json.dumps(token)
            db.session.commit()

    @xero_app.tokengetter
    @api_client.oauth2_token_getter
    # Define the OAuth2 token getter function
    def obtain_xero_oauth2_token():
        # If the user has a token stored, check it
        if user.xero_token:
            token = json.loads(user.xero_token)
            
            # Check if the token has expired
            if token.get('expires_at') and token.get('expires_at') < time.time():
                # Token is expired, try to refresh it
                refresh_token = token.get('refresh_token')
                if refresh_token:
                    new_token = refresh_xero_token(refresh_token, user)
                    if new_token:
                        return new_token
                    else:
                        # Refresh failed, force re-authentication
                        print(f"Token refresh failed for user {user.username}")
                        return redirect(url_for('xero.xero_login'))  # Redirect to Xero login for re-authentication
                else:
                    # No refresh token, force re-authentication
                    print(f"No refresh token available for user {user.username}")
                    return redirect(url_for('xero.xero_login'))  # Redirect to Xero login
            # Token is still valid
            return token

        return None


    # Attach the OAuth2 token getter to the ApiClient
    api_client.oauth2_token_getter = obtain_xero_oauth2_token

    return api_client, xero_app

def refresh_xero_token(refresh_token, user):
    try:
        # Make a request to the Xero token endpoint to refresh the token
        response = requests.post(
            'https://identity.xero.com/connect/token',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id':client_id,
                'client_secret':client_secret,
            }
        )

        if response.status_code == 200:
            (print("token refreshed sucessfully"))
            new_token = response.json()

            # Calculate expires_at and add it to the new token
            new_token['expires_at'] = time.time() + new_token.get('expires_in', 1800) 

            # Save the new token in the user's database record
            save_user_xero_token(user, new_token)

            return new_token
        else:
            print(f"Failed to refresh token: {response.content}")
            return None
    except Exception as e:
        print(f"Error refreshing token: {e}")
        return None





def save_user_xero_token(user, token):
    user.xero_token = json.dumps(token)  # Save the entire token as JSON
    user.refresh_token = token.get("refresh_token")  # Extract and save the refresh token

    # Convert expires_at to a UNIX timestamp (float) before saving
    expires_at = token.get("expires_at")
    if expires_at:
        user.token_expires_at = datetime.fromtimestamp(expires_at).timestamp()
    else:
        user.token_expires_at = None  # Handle cases where expires_at is missing

    db.session.commit()
    add_log(f"Xero token and expiry data saved for user {user.username}.", "general")

# Login route to initiate Xero OAuth
@xero_bp.route("/login")
def xero_login():
    user = current_user
    state = "fixed_state_value"  # Set a fixed state value
    session['oauth_state'] = state  # Store fixed state in the session 
    api_client, xero_app = get_xero_client_for_user(user)

    # Check the environment and set the redirect URL accordingly
    if os.environ.get('FLASK_ENV') == 'production':
        redirect_url = "https://xero-automation-webapp-dd8c38571179.herokuapp.com/xero_settings"
    else:
        redirect_url = url_for("xero.oauth_callback", _external=True)
        redirect_url = redirect_url.replace("127.0.0.1", "localhost")  # Ensure localhost is used


    add_log(f"User {user.username} initiated Xero login", "general")  # Log the Xero login initiation
    return xero_app.authorize(callback_uri=redirect_url)

# OAuth callback route for Xero
@xero_bp.route("/callback")
def oauth_callback():
    user = current_user 
    api_client, xero_app = get_xero_client_for_user(user)

    
    response = xero_app.authorized_response()

    if response is None or response.get("access_token") is None:
        return "Access denied: response=%s" % response

    # Store the user's Xero token in the database
    save_user_xero_token(user, response)
    session['xero_logged_in'] = True
    store_xero_tenants(user)
    return redirect(url_for("main.xero_settings"))

# Function to ensure the user is authenticated with Xero
def xero_token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        xero_token = json.loads(session.get('xero_token', '{}'))
        if not xero_token:
            return redirect(url_for("xero.xero_login"))
        return f(*args, **kwargs)
    return decorated_function


def xero_login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user_logged_in'):
            return redirect(url_for('auth.user_login'))  # Redirect to login page if not logged in
        if not session.get('xero_logged_in'):
            return redirect(url_for('xero.xero_login'))  # Redirect to Xero login if not logged in to Xero
        return f(*args, **kwargs)
    return decorated_function


@xero_bp.route("/logout", methods=['GET', 'POST'])
def xero_logout():
    # Get the current logged-in user
    user = current_user
    
    if user:
        # Clear the Xero token stored in the database
        user.xero_token = None
        user.refresh_token = None
        db.session.commit()

    # Clear Xero session data
    session.pop('xero_logged_in', None)
    session.pop('xero_token', None)
    session.pop('oauth_state', None)

    # Redirect to the Xero settings page or another appropriate page
    return redirect(url_for('main.xero_settings'))  # Redirect to the Xero settings page


def fetch_all_tenant_names(user):
    # Check if the user is logged into Xero by verifying the Xero token
    if not user.xero_token:
        return []  # Return an empty list if the user is not logged in to Xero

    # Proceed if the user has a valid Xero token
    api_client, xero_app = get_xero_client_for_user(user)
    try:
        # Use IdentityApi to fetch the user's connections
        identity_api = IdentityApi(api_client)
        # Fetch tenants from the database for the logged-in user
        tenants = XeroTenant.query.filter_by(user_id=user.id).all()


        # Extract tenant names and return them as a list
        tenant_names = [tenant.tenant_name for tenant in tenants]
        return tenant_names

    except AccountingBadRequestException as e:
        # Handle error when calling the Identity API
        print(f"Error fetching tenants: {e}")
        return []
    
from xero_python.identity import IdentityApi

@xero_bp.route("/remove_tenant/<tenant_id>", methods=["POST"])
def remove_tenant(tenant_id):
    user = current_user
    password = request.form.get("password")

    # Hardcoded admin password
    ADMIN_PASSWORD = "adminibotadmin"

    if password != ADMIN_PASSWORD:
        add_log(f"Failed removal attempt for tenant {tenant_id} by user {user.username} - Incorrect password.", "error")
        return redirect(url_for("main.xero_settings"))

    # Find the tenant in the database
    tenant = XeroTenant.query.filter_by(tenant_id=tenant_id, user_id=user.id).first()

    if tenant:
        # Initialize the Xero client for the user
        api_client, xero_app = get_xero_client_for_user(user)
        identity_api = IdentityApi(api_client)

        try:
            # Remove the connection in Xero
            identity_api.delete_connection(tenant_id)
            add_log(f"Successfully removed tenant {tenant.tenant_name} (ID: {tenant_id}) in Xero for user {user.username}.", "general")

            # Remove the tenant from the local database
            db.session.delete(tenant)
            db.session.commit()
            add_log(f"Removed tenant {tenant.tenant_name} (ID: {tenant_id}) from database for user {user.username}.", "general")
        except Exception as e:
            add_log(f"Error while removing tenant {tenant_id} for user {user.username}: {e}", "error")
            return redirect(url_for("main.xero_settings")), 500  # Internal Server Error
    else:
        add_log(f"Tenant {tenant_id} not found for user {user.username}.", "error")

    return redirect(url_for("main.xero_settings"))



def store_xero_tenants(user):
    # Unpack the api_client from the returned tuple
    api_client, xero_app = get_xero_client_for_user(user)
    identity_api = IdentityApi(api_client)

    try:
        # Fetch all tenant connections from Xero API
        tenants = identity_api.get_connections()
        add_log(f"Fetched {len(tenants)} tenants from Xero for user {user.username}.", "general")


        add_log(f"Fetched {len(tenants)} tenants for user {user.username}.", "general")

        # Fetch all existing tenants from the database for the user
        existing_tenants_in_db = XeroTenant.query.filter_by(user_id=user.id).all()
        existing_tenant_ids = {tenant.tenant_id for tenant in existing_tenants_in_db}

        # Keep track of the tenant IDs currently connected to Xero
        current_tenant_ids = {tenant.tenant_id for tenant in tenants}

        # Process each tenant fetched from Xero
        for tenant in tenants:
            add_log(f"Processing tenant {tenant.tenant_name} (ID: {tenant.tenant_id}) for user {user.username}.", "general")

            # Check if the tenant already exists in the XeroTenant database
            existing_tenant = XeroTenant.query.filter_by(tenant_id=tenant.tenant_id, user_id=user.id).first()
            if not existing_tenant:
                # Create a new tenant record
                new_tenant = XeroTenant(
                    user_id=user.id,
                    tenant_id=tenant.tenant_id,
                    tenant_name=tenant.tenant_name,
                    tenant_type=tenant.tenant_type
                )
                db.session.add(new_tenant)
                add_log(f"Added new tenant {tenant.tenant_name} (ID: {tenant.tenant_id}) for user {user.username}.", "general")

            # Check if the tenant name already exists in the Company database
            existing_company = Company.query.filter_by(company_name=tenant.tenant_name, user_id=user.id).first()
            if not existing_company:
                # Create a new company record
                new_company = Company(
                    company_name=tenant.tenant_name,
                    user_id=user.id  # Assuming a company is linked to a user
                )
                db.session.add(new_company)
                add_log(f"Added new company {tenant.tenant_name} to the Company database for user {user.username}.", "general")

        # Identify and delete tenants that are no longer connected to Xero
        tenants_to_delete = existing_tenant_ids - current_tenant_ids
        for tenant_id in tenants_to_delete:
            tenant_to_delete = XeroTenant.query.filter_by(tenant_id=tenant_id, user_id=user.id).first()
            if tenant_to_delete:
                db.session.delete(tenant_to_delete)
                add_log(f"Deleted tenant {tenant_to_delete.tenant_name} (ID: {tenant_to_delete.tenant_id}) no longer connected to Xero.", "general")

        db.session.commit()  # Commit after all tenants and companies have been added/deleted
        add_log(f"Tenants and companies successfully updated for user {user.username}.", "general")

    except Exception as e:
        add_log(f"Error while storing tenants for user {user.username}: {e}", "error")
        db.session.rollback()  # Rollback the transaction on any error
        add_log(f"Session rolled back after error for user {user.username}.", "error")



def get_tracking_categories_from_xero(user):

    api_client, xero_app = get_xero_client_for_user(user)
    identity_api = IdentityApi(api_client)
    accounting_api = AccountingApi(api_client)
    tenants = XeroTenant.query.filter_by(user_id=user.id).all()
    fetched_tracking_categories = []

    for tenant in tenants:
        if tenant.tenant_type == "ORGANISATION":
            tenant_name = tenant.tenant_name
            tenant_id = tenant.tenant_id
            try:
                tracking_categories_response = accounting_api.get_tracking_categories(tenant_id)
                for category in tracking_categories_response.tracking_categories:
                    for option in category.options:
                        if option.status == "ACTIVE":
                            fetched_tracking_categories.append({
                                "tenant_name": tenant_name,
                                "tracking_category_id": category.tracking_category_id,
                                "tracking_category_name": category.name,
                                "tracking_category_option": option.name,
                                "tracking_option_id": option.tracking_option_id,

                            })
            except AccountingBadRequestException as e:
                print(f"Exception when calling AccountingApi->getTrackingCategories for tenant {tenant_name}: {e}")
            
    return fetched_tracking_categories


def get_xero_account_codes(user):
    api_client, xero_app = get_xero_client_for_user(user)
    identity_api = IdentityApi(api_client)
    accounting_api = AccountingApi(api_client)
    
    # Get the tenant connections (assuming the Identity API setup is already in place)
    tenants = XeroTenant.query.filter_by(user_id=user.id).all()
    xero_codes = []

    for tenant in tenants:
        if tenant.tenant_type == "ORGANISATION" and tenant.tenant_name == user.company_name:
            tenant_id = tenant.tenant_id  # Extract tenant ID for user company name
            try:
                # Call Xero API to get account codes for the management company tenant
                accounts_response = accounting_api.get_accounts(tenant_id)

                # Assuming the accounts_response contains a list of accounts
                for account in accounts_response.accounts:
                    xero_codes.append({
                        'account_code': account.code,  # Extract the account code
                        'descriptor_per_dms': account.name  # Extract the account name/descriptor
                    })
                
            except Exception as e:
                print(f"Error fetching account codes for tenant {tenant.tenant_name}: {str(e)}")
                continue

    return xero_codes

def sync_store_account_codes_with_xero(user):
    import traceback
    api_client, xero_app = get_xero_client_for_user(user)
    identity_api = IdentityApi(api_client)
    accounting_api = AccountingApi(api_client)
    
    # Get DOM Purchase Invoice Tenants for the user
    dom_tenants = DomPurchaseInvoicesTenant.query.filter_by(user_id=user.id).all()
    if not dom_tenants:
        add_log(f"No DOM Purchase Invoice Tenants found for user {user.username}.", "warning")
        return {'status': 'error', 'message': 'No DOM Purchase Invoice Tenants found.'}
    
    # Get Store Account Codes from the database
    store_account_codes = StoreAccountCodes.query.filter_by(user_id=user.id).all()
    if not store_account_codes:
        add_log(f"No Store Account Codes found for user {user.username}.", "warning")
        return {'status': 'error', 'message': 'No Store Account Codes found.'}
    
    # Fetch all tenants connected to the user
    try:
        connected_tenants = XeroTenant.query.filter_by(user_id=user.id).all()
        add_log(f"Connected tenants for user {user.username}: {connected_tenants}", "debug")
    except Exception as e:
        error_message = f"Error fetching connected tenants for user {user.username}: {str(e)}"
        errors = [error_message]
        add_log(error_message, "error")
        traceback_str = traceback.format_exc()
        add_log(f"Traceback:\n{traceback_str}", "error")
        return {'status': 'error', 'message': 'Error fetching connected tenants.', 'errors': errors}
    
    # Create a mapping of tenant_name to tenant_id
    tenant_name_to_id = {tenant.tenant_name: tenant.tenant_id for tenant in connected_tenants}
    add_log(f"Tenant name to ID mapping: {tenant_name_to_id}", "debug")
    
    # Prepare results
    new_codes_in_tenants = {}
    errors = []
    
    valid_tax_types_for_account_type = {
        'EXPENSE': {
            'INPUT', 'INPUT2', 'NONE', 'ZERORATEDINPUT', 'EXEMPTINPUT',
            'RRINPUT', 'REVERSECHARGES', 'CAPEXINPUT', 'CAPEXINPUT2', 'CAPEXSRINPUT',
            'GSTONIMPORTS', 'ECACQUISITIONS'
        },
        'REVENUE': {
            'OUTPUT', 'OUTPUT2', 'NONE', 'ZERORATEDOUTPUT', 'EXEMPTOUTPUT',
            'RROUTPUT', 'CAPEXOUTPUT', 'CAPEXOUTPUT2', 'CAPEXSROUTPUT',
            'ECZROUTPUT', 'ECZROUTPUTSERVICES'
        },
        'OVERHEADS': {
            'INPUT', 'INPUT2', 'NONE', 'ZERORATEDINPUT', 'EXEMPTINPUT',
            'RRINPUT', 'REVERSECHARGES', 'CAPEXINPUT', 'CAPEXINPUT2'
        },
        'DIRECTCOSTS': {
            'INPUT', 'INPUT2', 'NONE', 'ZERORATEDINPUT', 'EXEMPTINPUT',
            'RRINPUT', 'REVERSECHARGES', 'CAPEXINPUT', 'CAPEXINPUT2'
        },
        'FIXED': {
            'CAPEXINPUT', 'CAPEXINPUT2', 'NONE', 'INPUT', 'INPUT2'
        },
        'NONCURRENT': {
            'CAPEXINPUT', 'CAPEXINPUT2', 'NONE', 'INPUT', 'INPUT2'
        },
        'CURRENT': {
            'INPUT', 'INPUT2', 'OUTPUT', 'OUTPUT2', 'NONE',
            'ZERORATEDINPUT', 'ZERORATEDOUTPUT', 'EXEMPTINPUT', 'EXEMPTOUTPUT'
        },
        'INVENTORY': {
            'INPUT', 'INPUT2', 'NONE'
        },
        'PREPAYMENT': {
            'INPUT', 'INPUT2', 'NONE', 'ZERORATEDINPUT', 'EXEMPTINPUT', 'RRINPUT'
        },
        'BANK': {'NONE'},
        'EQUITY': {'NONE'},
        'CURRLIAB': {'NONE'},
        'LIABILITY': {'NONE'},
        'TERMLIAB': {'NONE'},
        'PAYG': {'NONE'},
        'PAYGLIABILITY': {'NONE'},
        'DEPRECIATN': {'NONE'},
        'OTHERINCOME': {
            'OUTPUT', 'OUTPUT2', 'NONE', 'ZERORATEDOUTPUT', 'EXEMPTOUTPUT',
            'RROUTPUT', 'CAPEXOUTPUT', 'CAPEXOUTPUT2'
        },
        # Add any other account types as needed
    }

    
    for tenant_entry in dom_tenants:
        tenant_name = tenant_entry.tenant_name
        tenant_id = tenant_name_to_id.get(tenant_name)
        
        if not tenant_id:
            error_message = f"Tenant '{tenant_name}' not found in connected tenants for user {user.username}."
            errors.append(error_message)
            add_log(error_message, "error")
            continue  # Skip to the next tenant
        
        add_log(f"Processing tenant {tenant_name} (ID: {tenant_id}) for user {user.username}.", "general")
        try:
            # Fetch existing account codes from Xero
            existing_accounts_response = accounting_api.get_accounts(tenant_id)
            existing_account_codes = {account.code for account in existing_accounts_response.accounts}
            add_log(f"Existing account codes in tenant {tenant_name}: {existing_account_codes}", "debug")
            
            # Fetch tax rates for the tenant
            try:
                tax_rates_response = accounting_api.get_tax_rates(tenant_id)
                valid_tax_types = {tax_rate.tax_type for tax_rate in tax_rates_response.tax_rates}
                add_log(f"Valid tax types in tenant {tenant_name}: {valid_tax_types}", "debug")
            except Exception as e:
                error_message = f"Error retrieving tax rates for tenant {tenant_name}: {str(e)}"
                errors.append(error_message)
                add_log(error_message, "error")
                traceback_str = traceback.format_exc()
                add_log(f"Traceback:\n{traceback_str}", "error")
                valid_tax_types = set()  # Proceed without validation
            
            # Identify missing codes
            missing_codes = [code for code in store_account_codes if code.account_code not in existing_account_codes]
            add_log(f"Missing account codes in tenant {tenant_name}: {[code.account_code for code in missing_codes]}", "debug")
            
            codes_added = []
            for code in missing_codes:
                add_log(f"Processing account code {code.account_code}", "debug")
                # Map the account type string to the AccountType enum
                try:
                    account_type_enum = AccountType[code.account_type.upper()]
                    add_log(f"Account type enum for code {code.account_code}: {account_type_enum}", "debug")
                except KeyError:
                    error_message = f"Invalid account type '{code.account_type}' for code {code.account_code}"
                    errors.append(error_message)
                    add_log(error_message, "error")
                    continue  # Skip this code if account type is invalid

                # Retrieve tax type and description from the code object
                tax_type = code.tax_type
                description = code.description

                # Validate tax_type
                if tax_type:
                    if valid_tax_types and tax_type not in valid_tax_types:
                        error_message = f"Invalid tax type '{tax_type}' for code {code.account_code} in tenant {tenant_name}"
                        errors.append(error_message)
                        add_log(error_message, "error")
                        continue  # Skip this code if tax type is invalid
                    # Check if tax_type is valid for the account type
                    account_type_name = account_type_enum.name
                    valid_tax_types_for_account = valid_tax_types_for_account_type.get(account_type_name, set())
                    if tax_type not in valid_tax_types_for_account:
                        error_message = (
                            f"Tax type '{tax_type}' is not valid for account type '{account_type_name}' "
                            f"for code {code.account_code} in tenant {tenant_name}"
                        )
                        errors.append(error_message)
                        add_log(error_message, "error")
                        continue  # Skip this code if tax type is invalid for the account type
                else:
                    add_log(f"No tax type provided for code {code.account_code}", "debug")

                # Log account data before creation
                account_data = {
                    'code': code.account_code,
                    'name': code.account_name,
                    'type': account_type_enum.value,
                    'tax_type': tax_type,
                    'description': description
                }
                add_log(f"Creating Account with data: {account_data}", "debug")

                try:
                    # Create the Account object
                    account = Account(
                        code=code.account_code,
                        name=code.account_name,
                        type=account_type_enum,
                        description=description
                    )
                    # Set tax_type if applicable
                    if tax_type:
                        account.tax_type = tax_type

                    # Create the account in Xero
                    result = accounting_api.create_account(tenant_id, account)
                    codes_added.append(code.account_code)
                    add_log(f"Added account code {code.account_code} to tenant {tenant_name}.", "general")
                except AccountingBadRequestException as e:
                    # Extract error details
                    error_details = e.error_data.get('Elements', [])
                    if error_details and isinstance(error_details, list):
                        validation_errors = error_details[0].get('ValidationErrors', [])
                        if validation_errors:
                            error_message_text = validation_errors[0].get('Message', e.reason)
                        else:
                            error_message_text = e.reason
                    else:
                        error_message_text = e.reason

                    error_message = (
                        f"Error adding code {code.account_code} to tenant {tenant_name}: {error_message_text}"
                    )
                    errors.append(error_message)
                    add_log(error_message, "error")
                    traceback_str = traceback.format_exc()
                    add_log(f"Traceback:\n{traceback_str}", "error")
                except AccountingBadRequestException as e:
                    error_message = (
                        f"API Exception when adding code {code.account_code} to tenant {tenant_name}: "
                        f"Status Code: {e.status}, Reason: {e.reason}, Body: {e.body}"
                    )
                    errors.append(error_message)
                    add_log(error_message, "error")
                    traceback_str = traceback.format_exc()
                    add_log(f"Traceback:\n{traceback_str}", "error")
                except Exception as e:
                    error_message = (
                        f"Unexpected error adding code {code.account_code} to tenant {tenant_name}: "
                        f"Type: {type(e)}, Value: {e}, Repr: {repr(e)}"
                    )
                    errors.append(error_message)
                    add_log(error_message, "error")
                    traceback_str = traceback.format_exc()
                    add_log(f"Traceback:\n{traceback_str}", "error")
            if codes_added:
                new_codes_in_tenants[tenant_name] = codes_added
                add_log(f"Codes added to tenant {tenant_name}: {codes_added}", "debug")
        except Exception as e:
            error_type = type(e)
            error_message = (
                f"Error processing tenant {tenant_name}: Exception Type: {error_type}, "
                f"Value: {e}, Repr: {repr(e)}"
            )
            errors.append(error_message)
            add_log(error_message, "error")
            traceback_str = traceback.format_exc()
            add_log(f"Traceback for tenant {tenant_name}:\n{traceback_str}", "error")
    
    result = {
        'status': 'success' if not errors else 'error',
        'new_codes_in_tenants': new_codes_in_tenants,
        'errors': errors
    }
    return result


@xero_bp.route('/auto_workflows_data')
def aggregate_auto_workflows_data(user):
    api_client, xero_app = get_xero_client_for_user(user)
    identity_api = IdentityApi(api_client)
    files_api = FilesApi(api_client)
    accounting_api = AccountingApi(api_client)

    tenants = XeroTenant.query.filter_by(user_id=user.id).all()

    workflow_log = []
    total_files = 0
    untracked_coca_cola_invoices = 0
    purchase_invoices_count = 0
    sales_invoices_count = 0
    supplier_invoices_count = {"coca_cola": 0, "eden_farm": 0, "text_man": 0}
    
    tenant_file_data = {
        "purchase_invoices_tenants": [],
        "sales_invoices_tenants": [],
        "coca_cola_tenants": [],
        "eden_farm_tenants": [],
        "text_man_tenants": []
    }

    # Get allowed tenants for the current user session
    if 'user_id' in session:
        allowed_tenants = [tenant.tenant_name for tenant in DomPurchaseInvoicesTenant.query.filter_by(user_id=session['user_id']).all()]
        add_log(f"Allowed tenants for user {session['user_id']}: {allowed_tenants}", log_type="general")
    else:
        allowed_tenants = []
        add_log("No allowed tenants found for the user session.", log_type="errors")

    for connection in tenants:
        if connection.tenant_type == "ORGANISATION" and connection.tenant_name in allowed_tenants:
            xero_tenant_id = connection.tenant_id
            try:
                # Fetch all files from the tenant's inbox
                api_response = files_api.get_files(xero_tenant_id, sort='CreatedDateUTC DESC')
                inbox_folder_id = files_api.get_inbox(xero_tenant_id)
                filtered_files = [file for file in api_response.items if file.folder_id == inbox_folder_id.id]

                # Count total files for this tenant
                file_count = len(filtered_files)
                total_files += file_count

                # Count Dom Purchase Invoices (matching .csv and .pdf with "CustAccountStatementExt.Report")
                dom_purchase_invoice_count = 0
                dom_purchase_files = [file for file in filtered_files if file.name.startswith("CustAccountStatementExt.Report") and (file.name.endswith(".csv") or file.name.endswith(".pdf"))]

                base_names = set([file.name.rsplit('.', 1)[0] for file in dom_purchase_files])
                for base_name in base_names:
                    matching_csv = any(file.name == f"{base_name}.csv" for file in dom_purchase_files)
                    matching_pdf = any(file.name == f"{base_name}.pdf" for file in dom_purchase_files)
                    if matching_csv and matching_pdf:
                        dom_purchase_invoice_count += 1

                purchase_invoices_count += dom_purchase_invoice_count
                tenant_file_data["purchase_invoices_tenants"].append({"name": connection.tenant_name, "file_count": dom_purchase_invoice_count})

                # Count Dom Sales Invoices (matching .xls files with "KeyIndicatorsStore")
                sales_invoice_count = len([file for file in filtered_files if file.name.startswith("KeyIndicatorsStore") and file.name.endswith(".xls")])
                sales_invoices_count += sales_invoice_count
                tenant_file_data["sales_invoices_tenants"].append({"name": connection.tenant_name, "file_count": sales_invoice_count})

                # Process supplier invoices (Coca-Cola, Eden Farm, Text Man)
                invoices = accounting_api.get_invoices(
                    xero_tenant_id, 
                    where='(Contact.Name.Contains("COCACOLA") OR Contact.Name.Contains("Eden Farm") OR Contact.Name.Contains("Text Management")) AND AmountDue > 0 AND Status == "AUTHORISED"'
                )

                coca_cola_count = 0
                eden_farm_count = 0
                text_man_count = 0

                for invoice in invoices.invoices:
                    contact_name = invoice.contact.name.lower()

                    if "cocacola" in contact_name:
                        coca_cola_count += 1
                        tenant_file_data["coca_cola_tenants"].append({"name": connection.tenant_name, "file_count": 1})
                    elif "eden farm" in contact_name:
                        eden_farm_count += 1
                        tenant_file_data["eden_farm_tenants"].append({"name": connection.tenant_name, "file_count": 1})
                    elif "text management" in contact_name:
                        text_man_count += 1
                        tenant_file_data["text_man_tenants"].append({"name": connection.tenant_name, "file_count": 1})

                supplier_invoices_count["coca_cola"] += coca_cola_count
                supplier_invoices_count["eden_farm"] += eden_farm_count
                supplier_invoices_count["text_man"] += text_man_count


                # Log per tenant for files
                workflow_log.append(f"{connection.tenant_name}: {file_count} total files, {dom_purchase_invoice_count} Dom Purchase Invoices, {sales_invoice_count} Dom Sales Invoices, {coca_cola_count} Coca-Cola Invoices, {eden_farm_count} Eden Farm Invoices, {text_man_count} Text Man Invoices")

                
            except HTTPStatusException as e:
                workflow_log.append(f"Error fetching files for tenant {connection.tenant_name}: {e}")
            except Exception as e:
                workflow_log.append(f"Unexpected error fetching files for tenant {connection.tenant_name}: {e}")

    workflow_log.append(f"Total Coca-Cola invoices with untracked line items: {untracked_coca_cola_invoices}")

    return jsonify({
        "total_files": total_files,
        "workflow_log": workflow_log,
        "purchase_invoices_count": purchase_invoices_count,
        "sales_invoices_count": sales_invoices_count,
        "supplier_invoices_count": supplier_invoices_count,
        "purchase_invoices_tenants": tenant_file_data["purchase_invoices_tenants"],
        "sales_invoices_tenants": tenant_file_data["sales_invoices_tenants"],
        "coca_cola_tenants": tenant_file_data["coca_cola_tenants"],
        "eden_farm_tenants": tenant_file_data["eden_farm_tenants"],
        "text_man_tenants": tenant_file_data["text_man_tenants"]
    })

def get_inbox_files_from_management_company(user):
    api_client, xero_app = get_xero_client_for_user(user)
    # Initialize the Xero API client for the given user
    identity_api = IdentityApi(api_client)
    files_api = FilesApi(api_client)

    # Fetch the management tenant for the user
    management_tenant = user.company_name
    print(management_tenant)

    tenant_data = []  # Initialize the tenant_data list outside the loop

    tenants = XeroTenant.query.filter_by(user_id=user.id).all()

    # Loop through all tenant connections
    for connection in tenants:
        if connection.tenant_name == management_tenant and connection.tenant_type == "ORGANISATION":
            tenant_id = connection.tenant_id
            tenant_name = connection.tenant_name

            # Fetch all folders for the tenant and look for the "Inbox" folder
            folders = files_api.get_folders(tenant_id)
            inbox_folder = next((folder for folder in folders if folder.name.lower() == "inbox"), None)

            if inbox_folder:
                # Initialize list to gather all files in the "Inbox" folder
                all_inbox_files = []
                page = 1
                page_size = 100  # Maximum allowed page size
                max_pages = 5   # Maximum pages to read

                # Paginate through up to 50 pages of files for the tenant
                while page <= max_pages:
                    files = files_api.get_files(
                        tenant_id,
                        page=page,
                        pagesize=page_size,
                        sort='CreatedDateUTC DESC'
                    )

                    if not files.items:
                        break  # Exit loop if no more files on the current page

                    # Filter files that are in the "Inbox" folder
                    inbox_files = [
                        {
                            'file_id': file.id,
                            'file_name': file.name,
                            'mime_type': file.mime_type,
                            'folder_id': file.folder_id
                        }
                        for file in files.items if file.folder_id == inbox_folder.id
                    ]

                    # Append inbox files from the current page to all_inbox_files list
                    all_inbox_files.extend(inbox_files)

                    # Move to the next page
                    page += 1

                # Append tenant info with only "Inbox" folder and its files
                tenant_data.append({
                    'tenant_id': tenant_id,
                    'tenant_name': tenant_name,
                    'folders': [{'folder_id': inbox_folder.id, 'folder_name': inbox_folder.name}],
                    'files': all_inbox_files
                })

    return tenant_data, tenant_id  # Return the collected data





def fetch_dom_invoicing_data(user):
    api_client, xero_app = get_xero_client_for_user(user)
    # Initialize the Xero API client for the given user
    identity_api = IdentityApi(api_client)
    files_api = FilesApi(api_client)

    # Fetch allowed tenants for the user
    allowed_tenants = [tenant.tenant_name for tenant in DomPurchaseInvoicesTenant.query.filter_by(user_id=user.id).all()]

    tenant_data = []

    tenants = XeroTenant.query.filter_by(user_id=user.id).all()

    # Loop through all tenant connections
    for connection in tenants:
        if (connection.tenant_name in allowed_tenants or connection.tenant_id in allowed_tenants) and connection.tenant_type == "ORGANISATION":
            tenant_id = connection.tenant_id
            tenant_name = connection.tenant_name

            # Fetch all folders and their IDs for the tenant
            folders = files_api.get_folders(tenant_id)
            folder_info = [{'folder_id': folder.id, 'folder_name': folder.name} for folder in folders]

            # Fetch all files for the tenant (e.g., CSV and PDFs)
            files = files_api.get_files(tenant_id, sort='CreatedDateUTC DESC')
            file_info = [{'file_id': file.id, 'file_name': file.name, 'mime_type': file.mime_type, 'folder_id': file.folder_id} for file in files.items]

            # Append tenant info, folders, and files to the tenant_data list
            tenant_data.append({
                'tenant_id': tenant_id,
                'tenant_name': tenant_name,
                'folders': folder_info,
                'files': file_info
            })

            print(tenant_data)

    return tenant_data



def fetch_file_content(user, tenant_id, file_id):
    api_client, xero_app = get_xero_client_for_user(user)
    files_api = FilesApi(api_client)
    file_content = files_api.get_file_content(tenant_id, file_id)
    return file_content


def bulk_create_bills(user, tenant_id, invoices, management_tenant_id):
    api_client, xero_app = get_xero_client_for_user(user)
    user_id = user.id

    # Create an instance of the Accounting API
    api_instance = AccountingApi(api_client)
    files_api = FilesApi(api_client)
    
    # Prepare the list of invoices to be sent
    xero_invoices = []
    invoice_file_map = {}

    # First, check all contacts for all invoices before proceeding
    missing_contacts = False
    last_reference_map = {}  # To store the last reference number for each contact

    for invoice_data in invoices:
        store_number = invoice_data['store_number']
        tracking_category = TrackingCategoryModel.query.filter_by(store_number=store_number).first()
        store_contact_name = tracking_category.store_contact  
        search_term = str(store_contact_name)

        response = api_instance.get_contacts(tenant_id, search_term)

        # Initialize contact_id to None
        contact_id = None

        # Iterate through the contacts to find a matching name
        for contact in response.contacts:
            if contact.name == store_contact_name:
                contact_id = contact.contact_id
                # Log that the contact_id was found
                add_log(
                    f"Contact ID '{contact_id}' found for contact name '{search_term}'.",
                    log_type="general",
                    user_id=user_id
                )
                break  # Exit loop after finding the first matching contact

        # If contact_id was not found, log an error and set missing_contacts flag
        if not contact_id:
            add_log(
                f"Contact with name '{search_term}' not found in Xero for store '{store_number}'.",
                log_type="error",
                user_id=user_id
            )
            missing_contacts = True

    # If any contact is missing, abort the process
    if missing_contacts:
        add_log("One or more contacts are missing. Aborting invoice creation.", log_type="error", user_id=user_id)
        return None
    
    # Query Xero for the last used invoice number globally (across all contacts)
    try:
        # Retrieve invoices created by my app, sorted by reference in descending order, up to 100 invoices
        xero_invoices_response = api_instance.get_invoices(
            tenant_id,
            where='Status=="AUTHORISED" OR Status=="SUBMITTED"',  # Get authorised or submitted invoices
            order="Reference DESC",  # Order by reference in descending order
            created_by_my_app=True,  # Only retrieve invoices created by my app
            page=1,  # Retrieve the first page (up to 100 invoices)
            unitdp=2  # Include unit decimal places
        )

        # Check if the response contains invoices
        if xero_invoices_response and xero_invoices_response.invoices:
            # Initialize the last_invoice_number to 0
            last_invoice_number = 0

            # Loop through the retrieved invoices to find the highest invoice number
            for invoice in xero_invoices_response.invoices:

                # Extract the numeric part of the invoice number (assuming format "INV-X" or "INV-XB")
                last_invoice_number_str = invoice.invoice_number.replace("INV-", "").rstrip("B")

                # Convert the extracted part to an integer, handle non-numeric cases gracefully
                current_invoice_number = int(last_invoice_number_str) if last_invoice_number_str.isdigit() else 0

                # Update the last_invoice_number if the current one is larger
                last_invoice_number = max(last_invoice_number, current_invoice_number)

        else:
            # No invoices found, start from 0
            last_invoice_number = 0

    except Exception as e:
        add_log(f"Failed to retrieve last invoice number: {str(e)}", log_type="error", user_id=user_id)
        last_invoice_number = None

    print(f"Starting from invoice number: {last_invoice_number}")



 

    for invoice_data in invoices:
        store_number = invoice_data['store_number']
        tracking_category = TrackingCategoryModel.query.filter_by(store_number=store_number).first()
        store_contact_name = tracking_category.store_contact  
        
        invoice_reference_A_or_B = invoice_data["reference"]

        response = api_instance.get_contacts(tenant_id, search_term)

        # Initialize contact_id to None
        contact_id = None

        # Iterate through the contacts to find a matching name
        for contact in response.contacts:
            if contact.name == store_contact_name:
                contact_id = contact.contact_id
                # Log that the contact_id was found
                add_log(
                    f"Contact ID '{contact_id}' found for contact name '{search_term}'.",
                    log_type="general",
                    user_id=user_id
                )
                break  # Exit loop after finding the first matching contact

        # If contact_id was not found, log an error
        if not contact_id:
            add_log(
                f"Contact with name '{search_term}' not found in Xero.",
                log_type="error",
                user_id=user_id
            )
            continue
        
        contact = Contact(
            contact_id = contact_id)
        
        
        # Determine the new invoice number based on reference type
        if invoice_reference_A_or_B == "A":
            # Increment the invoice number for "A"
            last_invoice_number += 1
            new_invoice_number = f"INV-{last_invoice_number}"
        elif invoice_reference_A_or_B == "B":
            # Use the same invoice number but append "B"
            new_invoice_number = f"INV-{last_invoice_number}B"
        else:
            add_log(f"Unknown reference type '{invoice_reference_A_or_B}'.", log_type="error", user_id=user_id)
            continue

        print(f"New invoice number: {new_invoice_number}")

    


        statement_date_str = invoice_data['statement_date']
        
        statement_date = datetime.strptime(statement_date_str, '%d/%m/%Y')

        # Convert the statement date to ISO 8601 string format
        iso_statement_date = statement_date.isoformat() + 'Z'

        # Calculate the due date by adding 9 days
        due_date = statement_date + timedelta(days=9)
        iso_due_date = due_date.isoformat() + 'Z'


        store_number = invoice_data['store_number']
        tracking_category = TrackingCategoryModel.query.filter_by(store_number=store_number).first()
        store_contact_name = tracking_category.store_contact  
        search_term = str(store_contact_name)

    

        # Prepare line items for each invoice
        line_items = []

        # Use 'csv_file_id' as the unique 'reference' field for mapping
        reference = invoice_data['csv_file_id']

        add_log(f"Processing invoice for tenant '{tenant_id}' with invoice contact name '{contact_id}''{store_contact_name}'.", log_type="general", user_id=user_id)


        # Ensure that the correct structure is used for each line item
        for line_item_data in invoice_data['line_items']:
            # Process the line item tracking (already constructed as `LineItemTracking`)
            line_item_tracking_list = []
            for tracking_data in line_item_data.tracking:  # This is assumed to already be `LineItemTracking` objects
                line_item_tracking = LineItemTracking(
                    tracking_category_id=tracking_data.tracking_category_id,
                    tracking_option_id=tracking_data.tracking_option_id
                )
                line_item_tracking_list.append(line_item_tracking)

            # Create the LineItem with the appropriate tracking
            line_item = LineItem(
                description=line_item_data.description,
                quantity=line_item_data.quantity,
                unit_amount=line_item_data.unit_amount,
                account_code=line_item_data.account_code,
                tax_type=line_item_data.tax_type,
                tracking=line_item_tracking_list  # Assign tracking properly
            )

            line_items.append(line_item)

 
        

    
        # Prepare the Invoice object
        invoice = Invoice(
            contact = contact,
            type="ACCPAY",  
            date = dateutil.parser.parse(iso_statement_date),
            due_date = dateutil.parser.parse(iso_due_date),
            line_items=line_items,
            invoice_number = new_invoice_number,
            reference=reference,
            status="AUTHORISED"  # You can change this to "AUTHORISED" if you want to directly create authorised invoices
        )
        
        # Add the invoice to the list of invoices to be sent
        xero_invoices.append(invoice)

        # Map 'reference' to its corresponding files
        invoice_file_map[new_invoice_number] = {
            'csv_file_id': invoice_data['csv_file_id'],
            'pdf_file_id': invoice_data['pdf_file_id'],
            'csv_file_name': invoice_data['csv_file_name'],
            'pdf_file_name': invoice_data['pdf_file_name'],
            'total_invoice_amount': invoice_data['total_invoice_amount'],
        }

        
    

    # Create an Invoices object with the list of invoices
    invoices_payload = Invoices(
        invoices=xero_invoices
    )

    
    try:
        # Call the Xero API to create/update invoices
        summarize_errors = 'True'
        unitdp = 2  # The number of decimal places to use for the invoice amounts
        xero_tenant_id = tenant_id

        api_response = api_instance.create_invoices(
            xero_tenant_id, invoices_payload, summarize_errors, unitdp
        )

        add_log(f"Successfully created/updated {len(api_response.invoices)} invoices for tenant '{tenant_id}'.", log_type="general", user_id=user_id)

        
        # Iterate over the created invoices and attach files
        for created_invoice in api_response.invoices:
            reference = created_invoice.invoice_number
            if not reference:
                add_log(f"Invoice {created_invoice.invoice_number} has no 'reference' field to map files", log_type="error", user_id=user_id)
                continue
            
            if reference not in invoice_file_map:
                add_log(f"No file mapping found for invoice reference {reference}", log_type="error", user_id=user_id)
                continue
            
            files_to_attach = invoice_file_map[reference]
            csv_file_id = files_to_attach.get('csv_file_id')
            pdf_file_id = files_to_attach.get('pdf_file_id')
            csv_file_name = files_to_attach.get('csv_file_name', f"{csv_file_id}.csv")
            pdf_file_name = files_to_attach.get('pdf_file_name', f"{pdf_file_id}.pdf")
            total_csv_sum = files_to_attach.get('total_invoice_amount')


            invoice_list = api_instance.get_invoice(xero_tenant_id, created_invoice.invoice_id)
            invoice = invoice_list.invoices[0]
            invoice_total = invoice_list.invoices[0].total

            if invoice_total != total_csv_sum:
                difference = total_csv_sum - invoice_total
                zero_rated_item_found = False
                for item in invoice.line_items:
                    #print(f"Processing line item: {item}")
                    if item.tax_type == 'ZERORATEDINPUT' and not zero_rated_item_found:
                        item.unit_amount = Decimal(item.unit_amount) + Decimal(difference)
                        item.line_amount = Decimal(item.line_amount) + Decimal(difference)
                        zero_rated_item_found = True
                        #print(f"Processed and changed line item: {item}")
                        break

            
                # Prepare the invoice data for update
                updated_invoice = Invoice(
                    invoice_id=invoice.invoice_id,
                    line_items=invoice.line_items
                )
                
                invoices = Invoices(invoices=[updated_invoice])
                
                # Update the invoice
                api_instance.update_invoice(xero_tenant_id, invoice.invoice_id, invoices)
                add_log(f"Updated invoice total to match csv", log_type="general", user_id=user_id)
                
                
        
            
            
            # Attach CSV file
            if csv_file_id:
                csv_file_path = files_api.get_file_content(management_tenant_id, csv_file_id)
                with open(csv_file_path, 'rb') as file:
                        csv_file_content = file.read()
                if csv_file_content:
                    try:
                        api_instance.create_invoice_attachment_by_file_name(
                            tenant_id,
                            created_invoice.invoice_id,
                            csv_file_name,
                            csv_file_content,
                            'text/csv'
                        )
                        add_log(f"Attached CSV file {csv_file_name} to invoice {created_invoice.invoice_number}", log_type="general", user_id=user_id)
                    except Exception as e:
                        add_log(f"Failed to attach CSV file {csv_file_name} to invoice {created_invoice.invoice_number}: {str(e)}", log_type="error", user_id=user_id)
                else:
                    add_log(f"CSV content not found for file ID {csv_file_id}", log_type="error", user_id=user_id)
            
            # Attach PDF file
            if pdf_file_id:
                pdf_file_path = files_api.get_file_content(management_tenant_id, pdf_file_id)
                with open(pdf_file_path, 'rb') as pdf_file:
                    pdf_content = pdf_file.read()
                if pdf_content:
                    try:
                        api_instance.create_invoice_attachment_by_file_name(
                            tenant_id,
                            created_invoice.invoice_id,
                            pdf_file_name,
                            pdf_content,
                            'application/pdf'
                        )
                        add_log(f"Attached PDF file {pdf_file_name} to invoice {created_invoice.invoice_number}", log_type="general", user_id=user_id)
                    except Exception as e:
                        add_log(f"Failed to attach PDF file {pdf_file_name} to invoice {created_invoice.invoice_number}: {str(e)}", log_type="error", user_id=user_id)
                else:
                    add_log(f"PDF content not found for file ID {pdf_file_id}", log_type="error", user_id=user_id)
    
        # Return the API response for further handling
        return api_response
    
    except AccountingBadRequestException as e:
        # Log the error response
        add_log(f"Failed to import invoices for tenant {tenant_id}: {str(e)}", log_type="error", user_id=user_id)
        return None
    

import time

def move_and_rename_file(file_id, new_folder_id, new_name, file_type, user, tenant_id, retries=3, delay=2):
    """
    Move and rename a file in Xero with retry logic and delay.
    
    Args:
        file_id: The ID of the file to be moved.
        new_folder_id: The ID of the folder to move the file into.
        new_name: The new name for the file.
        file_type: The type of file (for logging).
        user: The user performing the operation.
        tenant_id: The ID of the Xero tenant.
        retries: Number of retry attempts if the operation fails.
        delay: Time (in seconds) to wait between retry attempts.
    """
    api_client, xero_app = get_xero_client_for_user(user)
    user_id = user.id
    files_api = FilesApi(api_client)
    attempt = 0
    
    while attempt < retries:
        try:
            # Move the file
            files_api.update_file(tenant_id, file_id, {"FolderId": new_folder_id})
            add_log(f"{file_type} file moved to Rejected folder.", log_type="general", user_id=user_id)

            # Rename the file
            files_api.update_file(tenant_id, file_id, {"Name": new_name})
            add_log(f"{file_type} file renamed to {new_name}.", log_type="general", user_id=user_id)
            return True  # Exit if successful

        except Exception as e:
            attempt += 1
            if attempt < retries:
                add_log(f"Error moving/renaming {file_type} file on attempt {attempt}: {str(e)}. Retrying...", log_type="warning", user_id=user_id)
                time.sleep(delay)  # Wait before retrying
            else:
                add_log(f"Failed to move/rename {file_type} file after {retries} attempts: {str(e)}", log_type="error", user_id=user_id)
                return False  # Return False if all retries fail


def rename_file(file_id, new_name, file_type, user, tenant_id, retries=3, delay=2):
    """
    Rename a file in Xero with retry logic and delay.
    
    Args:
        file_id (str): The ID of the file to rename.
        new_name (str): The new name for the file.
        file_type (str): The type of file (for logging purposes).
        user (User): The user performing the operation.
        tenant_id (str): The ID of the Xero tenant.
        retries (int): Number of retry attempts if the operation fails.
        delay (int): Time (in seconds) to wait between retry attempts.
        
    Returns:
        bool: True if the renaming succeeded, False if it failed after retries.
    """
    # Initialize API client and log user ID
    api_client, xero_app = get_xero_client_for_user(user)
    user_id = user.id
    files_api = FilesApi(api_client)
    attempt = 0
    
    # Retry loop
    while attempt < retries:
        try:
            # Attempt to rename the file
            files_api.update_file(tenant_id, file_id, {"Name": new_name})
            add_log(f"{file_type} file renamed to {new_name}.", log_type="general", user_id=user_id)
            return True  # Exit if successful

        except Exception as e:
            attempt += 1
            if attempt < retries:
                # Log retry attempt and wait before the next try
                add_log(f"Error renaming {file_type} file on attempt {attempt}: {str(e)}. Retrying...", log_type="warning", user_id=user_id)
                time.sleep(delay)  # Wait before retrying
            else:
                # Log final failure after all retries
                add_log(f"Failed to rename {file_type} file after {retries} attempts: {str(e)}", log_type="error", user_id=user_id)
                return False  # Return False if all retries fail

    

def create_folder_if_not_exists(folder_name, tenant_id, user):
    api_client, xero_app = get_xero_client_for_user(user)
    user_id = user.id
    files_api = FilesApi(api_client)
    try:
        # Retrieve the list of folders for the tenant
        folders = files_api.get_folders(tenant_id)

        
        # Check if the folder already exists (using dot notation if folders are objects)
        folder = next((folder for folder in folders if folder.name.lower() == folder_name.lower()), None)
        
        if not folder:
            # Create the folder if it doesn't exist
            new_folder = files_api.create_folder(tenant_id, {"name": folder_name})
            add_log(f"Folder '{folder_name}' created for tenant {tenant_id}.", log_type="general", user_id=user_id)
            return new_folder
        
        return folder
    except Exception as e:
        add_log(f"Error retrieving or creating folder '{folder_name}' for tenant {tenant_id}: {str(e)}", log_type="error",user_id=user_id)
        return None
    




def extract_store_code(file_name):
    """
    Extracts the store code from the file name.
    The pattern we're looking for is 'S-<store_number>-'.
    """
    match = re.search(r'S-(\d+)-', file_name)
    if match:
        return match.group(1)  # Return the store number as a string
    return None

def extract_statement_date_from_pdf(xero_tenant_id, file_id, user):
    """
    Extracts the statement date from the first page of the PDF.
    We're looking for the text 'Statement date' and extracting the date following it.
    The PDF file is fetched using its file ID from Xero via the API.
    """

    # Get the Xero API client for the user
    api_client, xero_app = get_xero_client_for_user(user)
    files_api = FilesApi(api_client)

    print("extracting statement date from PDF...")

    try:
        # Fetch the PDF file path from the Xero API
        api_response = files_api.get_file_content(xero_tenant_id, file_id)
        pdf_file_path = api_response  # Assuming api_response contains the file path

        # Open the PDF file using pdfplumber
        with pdfplumber.open(pdf_file_path) as pdf:
            first_page = pdf.pages[0]
            text = first_page.extract_text()

            # Search for the text 'Statement date' and extract the date after it
            match = re.search(r'Statement date[:\s]*(\d{2}/\d{2}/\d{4})', text)
            if match:
                return match.group(1)  # Return the statement date as a string in the format 'dd/mm/yyyy'
        
    except Exception as e:
        print(f"Error extracting statement date from PDF: {str(e)}")
    
    return None

def extract_statement_date_from_csv(xero_tenant_id, file_id, user):
    """
    Extracts the statement date from the CSV by taking the last value
    in the 'Transaction date' column, handling multiple date formats.
    The returned date is always in 'DD/MM/YYYY' format.
    """

    # Get the Xero API client for the user
    api_client, xero_app = get_xero_client_for_user(user)
    files_api = FilesApi(api_client)

    print("extracting statement date from CSV...")

    try:
        # Fetch the file content from Xero API
        api_response = files_api.get_file_content(xero_tenant_id, file_id)
        csv_file_path = api_response  # Assuming api_response contains the file path

        # Read the CSV content
        df = pd.read_csv(csv_file_path)

        # Check if the 'Transaction date' column exists
        if 'Transaction date' in df.columns:
            # Extract the last value in the 'Transaction date' column
            last_transaction_date = df['Transaction date'].iloc[-1]

            # Convert the extracted date to a standard format, handling specific date formats
            try:
                # Try parsing the date with specific formats
                formats = ['%Y/%m/%d', '%d/%m/%Y', '%m/%d/%Y']
                standardized_date = None
                for fmt in formats:
                    try:
                        standardized_date = pd.to_datetime(last_transaction_date, format=fmt)
                        break  # If a format works, break the loop
                    except ValueError:
                        continue  # If the format doesn't match, continue to the next one

                # Raise an error if no valid date format is found
                if pd.isnull(standardized_date):
                    raise ValueError(f"Date format not recognized: {last_transaction_date}")

                # Return the date in 'DD/MM/YYYY' format
                return standardized_date.strftime('%d/%m/%Y')

            except Exception as e:
                print(f"Error converting date: {str(e)}")
                return None
        
        else:
            print("Transaction date column not found in the CSV.")
            return None
        
    except Exception as e:
        print(f"Error extracting statement date from CSV: {str(e)}")
        return None




def fetch_dom_management_company_data(user):
    # Fetch the user's company name
    company_name = user.company_name

    # Fetch tenants from the database for the logged-in user
    tenants = XeroTenant.query.filter_by(user_id=user.id).all()

    if not tenants:
        return []  # No tenants found for the user

    api_client, xero_app = get_xero_client_for_user(user)
    files_api = FilesApi(api_client)
    tenant_data = []

    for tenant in tenants:
        # Match tenant name and type
        if tenant.tenant_name == company_name and tenant.tenant_type == "ORGANISATION":
            try:
                tenant_id = tenant.tenant_id
                tenant_name = tenant.tenant_name

                # Fetch folders for the tenant
                folders = files_api.get_folders(tenant_id)
                folder_info = [
                    {'folder_id': folder.id, 'folder_name': folder.name} for folder in folders
                ]

                # Fetch files for the tenant
                files = files_api.get_files(tenant_id, sort='CreatedDateUTC DESC')
                file_info = [
                    {
                        'file_id': file.id,
                        'file_name': file.name,
                        'mime_type': file.mime_type,
                        'folder_id': file.folder_id
                    }
                    for file in files.items
                ]

                # Append tenant info, folders, and files to the tenant_data list
                tenant_data.append({
                    'tenant_id': tenant_id,
                    'tenant_name': tenant_name,
                    'folders': folder_info,
                    'files': file_info
                })
            except Exception as e:
                print(f"Error fetching data for tenant {tenant.tenant_name}: {e}")
                continue  # Skip to the next tenant in case of an error

    return tenant_data



def post_dom_purchase_invoice_with_attachment(tenant_id, line_items, invoice_type, file_name, file_content, user):
    
    api_client, xero_app = get_xero_client_for_user(user)

    try:
        # Create and post the invoice
        invoice_data = {
            "Type": "ACCREC" if invoice_type == "Sales Invoice" else "ACCPAY",
            "Contact": {"ContactID": tenant_id},
            "Date": pd.Timestamp.now().strftime('%Y-%m-%d'),
            "DueDate": pd.Timestamp.now().strftime('%Y-%m-%d'),
            "LineItems": line_items,
            "Status": "AUTHORISED"
        }
        accounting_api = AccountingApi(api_client)
        invoice_id = accounting_api.create_invoices(tenant_id, invoices=[invoice_data]).invoices[0].invoice_id
        add_log(f"Invoice {invoice_id} posted for tenant {tenant_id}.", log_type="general", user_id=user.id)

        # Attach the original file to the invoice
        file_data = {"FileName": file_name, "MimeType": "application/csv", "Content": file_content}
        accounting_api.create_invoice_attachment_by_file_name(tenant_id, invoice_id, file_name, file_data)
        add_log(f"File '{file_name}' attached to invoice {invoice_id}.", log_type="general", user_id=user.id)
        
        return invoice_id

    except Exception as e:
        add_log(f"Error posting invoice for tenant {tenant_id}: {str(e)}", log_type="error", user_id=user.id)
        return None


def post_dom_sales_invoice_with_attachment(tenant_id, line_items, contact_id, file_name, file_content, user, end_date, start_date, month, store_name, invoice_type):
    api_client, xero_app = get_xero_client_for_user(user)

    try:
        # First, convert the string to 'YYYY-MM-DD' format using datetime
        end_date_converted = datetime.strptime(end_date, "%d-%b-%Y").strftime('%Y-%m-%d')

        # Then, format it into the full format with time and timezone
        date_value = dateutil.parser.parse(f"{end_date_converted}T00:00:00Z")

        # Calculate due date as 7 days after end date
        due_date_value = date_value + timedelta(days=9)

        # Create the contact object with the provided contact_id
        contact = Contact(
            contact_id=contact_id
        )
        
       
        invoice_type_value = "ACCREC" if invoice_type == "Domino's Sales" else "ACCPAY"

        # Set invoice number conditionally
        if invoice_type == "Domino's Mileage":
            # Create and post the invoice
            invoice = Invoice(
                type=invoice_type_value,  # This denotes an Accounts Receivable invoice (sales)
                contact=contact,
                line_items=line_items,
                date=date_value,
                due_date=due_date_value,
                status="DRAFT",  # To set the invoice to 'Awaiting Approval'
                reference=f"{store_name} - w/e {end_date}",
                invoice_number=f"{store_name} - w/e {end_date}"
            )
        else:
            # Create and post the invoice
            invoice = Invoice(
                type=invoice_type_value,  # This denotes an Accounts Receivable invoice (sales)
                contact=contact,
                line_items=line_items,
                date=date_value,
                due_date=due_date_value,
                status="DRAFT",  # To set the invoice to 'Awaiting Approval'
                reference=f"{store_name} - w/e {end_date}",
            )


       

        invoices = Invoices(invoices=[invoice])

        # Post the invoice
        accounting_api = AccountingApi(api_client)
        created_invoices = accounting_api.create_invoices(tenant_id, invoices=invoices)

        # Get the invoice ID of the created invoice
        invoice_id = created_invoices.invoices[0].invoice_id
        add_log(f"Invoice {invoice_id} posted for tenant {tenant_id}.", log_type="general", user_id=user.id)

        # Attach the original file to the invoice
        accounting_api.create_invoice_attachment_by_file_name(tenant_id, invoice_id, file_name, file_content, 'text/csv')
        add_log(f"File '{file_name}' attached to invoice {invoice_id}.", log_type="general", user_id=user.id)

        return invoice_id

    except Exception as e:
        add_log(f"Error posting invoice for tenant {tenant_id}: {str(e)}", log_type="error", user_id=user.id)
        return None


def get_all_contacts(user):
    # Initialize a list to store tenant and contact details
    tenant_contact_details = []

    try:

        allowed_tenant_names = {tenant.tenant_name for tenant in DomPurchaseInvoicesTenant.query.filter_by(user_id=user.id).all()}

        # Get the Xero API client for the user
        api_client, _ = get_xero_client_for_user(user)
        identity_api = IdentityApi(api_client)
        accounting_api = AccountingApi(api_client)

        # Fetch all tenant connections for the user
        tenants = XeroTenant.query.filter_by(user_id=user.id).all()

        # Loop through each tenant and retrieve contacts
        for tenant in tenants:
            if tenant.tenant_type == "ORGANISATION" and tenant.tenant_name in allowed_tenant_names:
                tenant_id = tenant.tenant_id
                tenant_name = tenant.tenant_name
                contacts_list = []

                try:
                    # Fetch all contacts for the tenant
                    contacts_response = accounting_api.get_contacts(tenant_id)

                    # Store the contact IDs and names for the tenant
                    for contact in contacts_response.contacts:
                        contacts_list.append({
                            "contact_id": contact.contact_id,
                            "contact_name": contact.name
                        })

                    # Append tenant details along with contacts to the list
                    tenant_contact_details.append({
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_name,
                        "contacts": contacts_list
                    })

                    add_log(f"Fetched {len(contacts_response.contacts)} contacts for tenant '{tenant_name}'", log_type="general", user_id=user.id)

                except Exception as e:
                    add_log(f"Error fetching contacts for tenant '{tenant_name}': {str(e)}", log_type="error", user_id=user.id)

    except Exception as e:
        add_log(f"Error fetching tenants for user '{user.id}': {str(e)}", log_type="error", user_id=user.id)

    return tenant_contact_details


def get_invoices_and_credit_notes(user, tenants, contact_name):
    """
    Fetches all invoices and credit notes for each tenant that don't have tracking categories in their line items,
    based on the provided contact name (e.g., Coca-Cola, Eden Farm, TextMan).

    Args:
        user (User): The user object that contains Xero API credentials.
        tenants (list): A list of tenant names for which the invoices and credit notes should be fetched.
        contact_name (str): The name of the contact (e.g., Coca-Cola, Eden Farm, TextMan).

    Returns:
        list: A list of dictionaries with tenant_id and invoice_id for each invoice or credit note without tracking.
    """
    api_client, xero_app = get_xero_client_for_user(user)  # Fetch the Xero API client for the user
    accounting_api = AccountingApi(api_client)
    identity_api = IdentityApi(api_client)

    all_invoices_and_credit_notes = []


    try:
        # Loop through each tenant connection
        for connection in identity_api.get_connections():
            if connection.tenant_name in tenants and connection.tenant_type == "ORGANISATION":
                xero_tenant_id = connection.tenant_id
                tenant_name = connection.tenant_name

                try:
                    # Fetch all invoices for the tenant with the specified contact name
                    invoices = accounting_api.get_invoices(
                        xero_tenant_id,
                        where=f'Contact.Name.Contains("{contact_name}") AND AmountDue > 0 AND Status == "AUTHORISED"'
                    )

                    add_log(f"Found {len(invoices.invoices)} {contact_name} invoices for tenant {tenant_name}.", log_type="general", user_id=user.id)

                    # Loop through each invoice
                    for invoice in invoices.invoices:
                        invoice_id = invoice.invoice_id
                        add_log(f"Checking invoice {invoice.invoice_number} for tenant {tenant_name}.", log_type="general", user_id=user.id)

                        # Fetch the detailed invoice to access the line items
                        read_invoice = accounting_api.get_invoice(xero_tenant_id, invoice_id)
                        invoice_obj = read_invoice.invoices[0]  # Get the actual invoice object

                        # Check if any line items are missing tracking categories
                        line_items_without_tracking = [line_item for line_item in invoice_obj.line_items]

                        if line_items_without_tracking:
                            # If there are line items without tracking, add to the result list
                            all_invoices_and_credit_notes.append({
                                'tenant_id': xero_tenant_id,
                                'invoice_id': invoice_id,
                                'tenant_name': tenant_name,
                                'xero_type': "invoice"
                            })

                except AccountingBadRequestException as e:
                    add_log(f"Error fetching {contact_name} invoices for tenant {tenant_name}: {e.reason}", log_type="error", user_id=user.id)
                except Exception as e:
                    add_log(f"Unexpected error fetching {contact_name} invoices for tenant {tenant_name}: {str(e)}", log_type="error", user_id=user.id)

                try:
                    # Fetch all credit notes for the tenant with the specified contact name
                    credit_notes = accounting_api.get_credit_notes(
                        xero_tenant_id,
                        where=f'Contact.Name.Contains("{contact_name}") AND Total > 0 AND Status == "AUTHORISED"'
                    )

                    add_log(f"Found {len(credit_notes.credit_notes)} {contact_name} credit notes for tenant {tenant_name}.", log_type="general", user_id=user.id)

                    # Loop through each credit note
                    for credit_note in credit_notes.credit_notes:
                        credit_note_id = credit_note.credit_note_id
                        add_log(f"Checking credit note {credit_note.credit_note_number} for tenant {tenant_name}.", log_type="general", user_id=user.id)

                        # Fetch the detailed credit note to access the line items
                        read_credit_note = accounting_api.get_credit_note(xero_tenant_id, credit_note_id)
                        credit_note_obj = read_credit_note.credit_notes[0]  # Get the actual credit note object

                        # Check if any line items are missing tracking categories
                        line_items_without_tracking = [line_item for line_item in credit_note_obj.line_items]

                        if line_items_without_tracking:
                            # If there are line items without tracking, add to the result list
                            all_invoices_and_credit_notes.append({
                                'tenant_id': xero_tenant_id,
                                'invoice_id': credit_note_id,
                                'tenant_name': tenant_name,
                                'xero_type': "credit_note"
                            })

                except AccountingBadRequestException as e:
                    add_log(f"Error fetching {contact_name} credit notes for tenant {tenant_name}: {e.reason}", log_type="error", user_id=user.id)
                except Exception as e:
                    add_log(f"Unexpected error fetching {contact_name} credit notes for tenant {tenant_name}: {str(e)}", log_type="error", user_id=user.id)

    except Exception as e:
        add_log(f"Error fetching tenant connections: {str(e)}", log_type="error", user_id=user.id)

    return all_invoices_and_credit_notes




def extract_coca_cola_invoice_data(user, invoice):
    """
    Extracts the invoice data, checks if it's a credit memo, extracts the store's postcode from the invoice's PDF attachment,
    and retrieves the tracking option ID and tracking category ID for the matching store postcode.

    Args:
        user (User): The user object with Xero API credentials.
        invoice (dict): The invoice object containing relevant details such as invoice_id and tenant_name.

    Returns:
        dict: A dictionary with invoice type, invoice_id, store postcode, tracking_category_id, tracking_option_id, 
              and file path, or error messages.
    """
    tenant_id = invoice['tenant_id']
    invoice_id = invoice['invoice_id']
    tenant_name = invoice['tenant_name']

    api_client, _ = get_xero_client_for_user(user)
    accounting_api = AccountingApi(api_client)

    # Initialize errors as an empty list
    errors = []

    try:
        # Fetch invoice attachments
        attachments = accounting_api.get_invoice_attachments(tenant_id, invoice['invoice_id']).attachments
        if not attachments:
            errors.append("No attachments found")
            return {"errors": errors}

        # Fetch the attachment PDF file path
        file_path = accounting_api.get_invoice_attachment_by_id(tenant_id, invoice['invoice_id'], attachments[0].attachment_id, 'application/pdf')
        if not os.path.exists(file_path):
            errors.append("File not found")
            return {"errors": errors}

        # Extract text and check if it's a credit memo
        with pdfplumber.open(file_path) as pdf:
            text = "".join([page.extract_text() for page in pdf.pages])
            invoice_type = "credit_memo" if "CREDIT MEMO" in text else "invoice"

            # Extract Ship-to & Sold-to Address to get the store's postcode
            ship_to_match = re.search(r"Ship-to & Sold-to\s*:\s*(.*?)\n\d{6,}", text, re.DOTALL)
            if ship_to_match:
                postcode_match = re.search(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b", ship_to_match.group(1).strip())
                store_postcode = postcode_match.group(0) if postcode_match else "Postcode not found"
            else:
                store_postcode = "Ship-to & Sold-to address not found"

        if store_postcode in ["Postcode not found", "Ship-to & Sold-to address not found"]:
            errors.append("Postcode not found in invoice")
            return {"errors": errors}

        # Fetch tracking category ID and tracking option ID based on the store postcode
        tracking_category = TrackingCategoryModel.query.filter_by(store_postcode=store_postcode, user_id=user.id).first()
        if not tracking_category:
            errors.append(f"No tracking category found for {store_postcode}")
            return {"errors": errors}

        return {
            "invoice_type": invoice_type,
            "invoice_id": invoice['invoice_id'],
            "store_postcode": store_postcode,
            "tracking_category_id": tracking_category.tracking_category_id,
            "tracking_option_id": tracking_category.tracking_option_id,
            "file_path": file_path,
            "tenant_id": tenant_id,
            "errors": errors  # Return the empty list if there are no errors
        }

    except Exception as e:
        errors.append(str(e))
        return {"errors": errors}


    
def extract_textman_invoice_data(user, invoice):
    """
    Extracts the invoice data, checks if it's a credit memo, extracts the store's number from the invoice's PDF attachment,
    and retrieves the tracking option ID and tracking category ID for the matching store number.

    Args:
        user (User): The user object with Xero API credentials.
        invoice (dict): The invoice object containing relevant details such as invoice_id and tenant_name.

    Returns:
        dict: A dictionary with invoice type, invoice_id, store number, tracking_category_id, tracking_option_id, 
              and file path, or error messages.
    """
    tenant_id = invoice['tenant_id']
    invoice_id = invoice['invoice_id']
    tenant_name = invoice['tenant_name']

    api_client, _ = get_xero_client_for_user(user)
    accounting_api = AccountingApi(api_client)

    # Initialize errors as an empty list
    errors = []

    try:
        # Fetch invoice attachments
        attachments = accounting_api.get_invoice_attachments(tenant_id, invoice['invoice_id']).attachments
        if not attachments:
            errors.append("No attachments found")
            return {"errors": errors}

        # Fetch the attachment PDF file path
        file_path = accounting_api.get_invoice_attachment_by_id(tenant_id, invoice['invoice_id'], attachments[0].attachment_id, 'application/pdf')
        if not os.path.exists(file_path):
            errors.append("File not found")
            return {"errors": errors}

        # Extract text and check if it's a credit memo
        with pdfplumber.open(file_path) as pdf:
            text = "".join([page.extract_text() for page in pdf.pages])
            invoice_type = "credit_memo" if "CREDIT MEMO" in text else "invoice"

            # Extract the store number using the pattern "S" followed by 5 digits
            store_number_match = re.search(r"S(\d{5})", text)
            store_number = store_number_match.group(1).strip() if store_number_match else "Store number not found"

        if store_number == "Store number not found":
            errors.append("Store number not found in invoice")
            return {"errors": errors}

        # Fetch tracking category ID and tracking option ID based on the store number
        tracking_category = TrackingCategoryModel.query.filter_by(store_number=store_number, user_id=user.id).first()
        if not tracking_category:
            errors.append(f"No tracking category found for store number {store_number}")
            return {"errors": errors}

        return {
            "invoice_type": invoice_type,
            "invoice_id": invoice['invoice_id'],
            "store_number": store_number,
            "tracking_category_id": tracking_category.tracking_category_id,
            "tracking_option_id": tracking_category.tracking_option_id,
            "file_path": file_path,
            "tenant_id": tenant_id,
            "errors": errors  # Return the errors list, even if it's empty
        }

    except Exception as e:
        errors.append(str(e))
        return {"errors": errors}

    

def extract_eden_farm_invoice_data(user, invoice):
    """
    Extracts the invoice data, checks if it's a credit memo, extracts the store's postcode from the invoice's PDF attachment,
    and retrieves the tracking option ID and tracking category ID for the matching store postcode.

    Args:
        user (User): The user object with Xero API credentials.
        invoice (dict): The invoice object containing relevant details such as invoice_id and tenant_name.

    Returns:
        dict: A dictionary with invoice type, invoice_id, store postcode, tracking_category_id, tracking_option_id, 
              and file path, or error messages.
    """
    tenant_id = invoice['tenant_id']
    invoice_id = invoice['invoice_id']
    tenant_name = invoice['tenant_name']

    api_client, _ = get_xero_client_for_user(user)
    accounting_api = AccountingApi(api_client)

    # Initialize errors as an empty list
    errors = []

    try:
        # Fetch invoice attachments
        attachments = accounting_api.get_invoice_attachments(tenant_id, invoice['invoice_id']).attachments
        if not attachments:
            errors.append("No attachments found")
            return {"errors": errors}

        # Fetch the attachment PDF file path
        file_path = accounting_api.get_invoice_attachment_by_id(tenant_id, invoice['invoice_id'], attachments[0].attachment_id, 'application/pdf')
        if not os.path.exists(file_path):
            errors.append("File not found")
            return {"errors": errors}

        # Extract text and check if it's a credit memo
        with pdfplumber.open(file_path) as pdf:
            text = "".join([page.extract_text() for page in pdf.pages])
            invoice_type = "credit_memo" if "CREDIT NOTE" in text else "invoice"

            # Extract the postcode from the "Delivery Address" section
            delivery_address_match = re.search(r"Delivery Address(.*?)(\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b)", text, re.DOTALL)
            postcode_match = re.search(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b", delivery_address_match.group(0)) if delivery_address_match else None
            store_postcode = postcode_match.group(0).strip() if postcode_match else "Postcode not found"

        if store_postcode == "Postcode not found":
            errors.append("Postcode not found in invoice")
            return {"errors": errors}

        # Fetch tracking category ID and tracking option ID based on the store postcode
        tracking_category = TrackingCategoryModel.query.filter_by(store_postcode=store_postcode, user_id=user.id).first()
        if not tracking_category:
            errors.append(f"No tracking category found for {store_postcode}")
            return {"errors": errors}

        return {
            "invoice_type": invoice_type,
            "invoice_id": invoice['invoice_id'],
            "store_postcode": store_postcode,
            "tracking_category_id": tracking_category.tracking_category_id,
            "tracking_option_id": tracking_category.tracking_option_id,
            "file_path": file_path,
            "tenant_id": tenant_id,
            "errors": errors  # Return the errors list, even if it's empty
        }

    except Exception as e:
        errors.append(str(e))
        return {"errors": errors}



def convert_invoice_to_credit_memo(invoice, user):
    """
    Convert an invoice to a credit memo, attach the PDF, assign tracking, and void the original invoice.

    Args:
        invoice (dict): The invoice data.
        user (User): The user object with Xero API credentials.

    Returns:
        bool: True if successful, False otherwise.
    """
    api_client, _ = get_xero_client_for_user(user)
    accounting_api = AccountingApi(api_client)

    # Fetch tenant and invoice details
    tenant_id = invoice['tenant_id']
    invoice_id = invoice['invoice_id']

    try:
        # Check if tracking_category_id and tracking_option_id exist in the invoice data
        if 'tracking_category_id' not in invoice or 'tracking_option_id' not in invoice:
            raise KeyError(f"Missing tracking_category_id or tracking_option_id in invoice data: {invoice}")

        # Log the tenant_id and invoice_id for debugging
        add_log(f"Processing tenant_id: {tenant_id}, invoice_id: {invoice_id}", log_type="debug", user_id=user.id)

        # Fetch the original invoice object
        try:
            original_invoice = accounting_api.get_invoice(tenant_id, invoice_id).invoices[0]
        except AccountingBadRequestException as e:
            add_log(f"API Exception when fetching invoice {invoice_id}: {e.body}", log_type="error", user_id=user.id)
            return False
        except Exception as e:
            add_log(f"Error fetching invoice {invoice_id}: {str(e)}", log_type="error", user_id=user.id)
            return False

        # Prepare credit memo line items by making line amounts positive
        credit_memo_line_items = []
        for line_item in original_invoice.line_items:
            credit_line_item = LineItem(
                description=line_item.description,
                quantity=line_item.quantity,
                unit_amount=abs(line_item.unit_amount),  # Positive amounts
                account_code=line_item.account_code,
                line_amount=abs(line_item.line_amount),
                tracking=[LineItemTracking(
                    tracking_category_id=invoice['tracking_category_id'],
                    tracking_option_id=invoice['tracking_option_id']
                )]  
            )
            credit_memo_line_items.append(credit_line_item)

        # Create the credit note object
        credit_memo = CreditNote(
            contact=original_invoice.contact,
            date=original_invoice.date,
            line_items=credit_memo_line_items,
            type="ACCPAYCREDIT",
            status="AUTHORISED"
        )

        # Create the credit memo in Xero
        try:
            credit_note_response = accounting_api.create_credit_notes(tenant_id, CreditNotes(credit_notes=[credit_memo]))
            created_credit_memo = credit_note_response.credit_notes[0]
        except AccountingBadRequestException as e:
            add_log(f"API Exception when creating credit memo for invoice {invoice_id}: {e.body}", log_type="error", user_id=user.id)
            return False
        except Exception as e:
            add_log(f"Error creating credit memo for invoice {invoice_id}: {str(e)}", log_type="error", user_id=user.id)
            return False


        add_log(f"Created credit memo for invoice {original_invoice.invoice_number}.", log_type="general", user_id=user.id)

        # Attach the PDF to the created credit memo
        try:
            with open(invoice['file_path'], 'rb') as pdf_file:
                file_body = pdf_file.read()

                accounting_api.create_credit_note_attachment_by_file_name(
                    tenant_id,
                    created_credit_memo.credit_note_id,
                    "Credit Memo).pdf",
                    file_body,
                    include_online=True
                )

            add_log(f"Attached PDF to credit memo {created_credit_memo.credit_note_number}.", log_type="general", user_id=user.id)
        except Exception as e:
            add_log(f"Error attaching PDF to credit memo {created_credit_memo.credit_note_number}: {str(e)}", log_type="error", user_id=user.id)
            return False

        # Void the original invoice
        try:
            accounting_api.update_invoice(tenant_id, invoice_id, Invoices(invoices=[Invoice(invoice_id=invoice_id, status="VOIDED")]))
            add_log(f"Voided original invoice {original_invoice.invoice_number}.", log_type="general", user_id=user.id)
        except Exception as e:
            add_log(f"Error voiding original invoice {invoice_id}: {str(e)}", log_type="error", user_id=user.id)
            return False

        return True

    except KeyError as e:
        add_log(f"KeyError: {str(e)}. Invoice data: {invoice}", log_type="error", user_id=user.id)
        return False
    except Exception as e:
        add_log(f"Error converting invoice {invoice_id} to credit memo: {str(e)}", log_type="error", user_id=user.id)
        return False
    
def convert_credit_memo_to_invoice(credit_memo_data, user):
    """
    Convert a credit memo to an invoice, attach the PDF, assign tracking, and void the original credit memo.

    Args:
        credit_memo_data (dict): The credit memo data.
        user (User): The user object with Xero API credentials.

    Returns:
        bool: True if successful, False otherwise.
    """
    api_client, _ = get_xero_client_for_user(user)
    accounting_api = AccountingApi(api_client)

    # Fetch tenant and credit memo details
    tenant_id = credit_memo_data['tenant_id']
    credit_note_id = credit_memo_data['invoice_id']  # Using 'invoice_id' as credit_note_id
    file_path = credit_memo_data['file_path']

    try:
        # Fetch the original credit memo object
        try:
            original_credit_memo = accounting_api.get_credit_note(tenant_id, credit_note_id).credit_notes[0]
        except AccountingBadRequestException as e:
            add_log(f"API Exception when fetching credit memo {credit_note_id}: {e.body}", log_type="error", user_id=user.id)
            return False
        except Exception as e:
            add_log(f"Error fetching credit memo {credit_note_id}: {str(e)}", log_type="error", user_id=user.id)
            return False

        # Prepare invoice line items by making amounts positive
        invoice_line_items = []
        for line_item in original_credit_memo.line_items:
            invoice_line_item = LineItem(
                description=line_item.description,
                quantity=line_item.quantity,
                unit_amount=abs(line_item.unit_amount),  # Convert to positive amounts
                account_code=line_item.account_code,
                line_amount=abs(line_item.line_amount),
                tracking=[LineItemTracking(
                    tracking_category_id=credit_memo_data['tracking_category_id'],
                    tracking_option_id=credit_memo_data['tracking_option_id']
                )]
            )
            invoice_line_items.append(invoice_line_item)

        # Create the invoice object
        invoice = Invoice(
            contact=original_credit_memo.contact,
            date=original_credit_memo.date,
            due_date=original_credit_memo.date,
            line_items=invoice_line_items,
            type="ACCPAY",  # Change this if it's a different type, such as "ACCREC"
            status="AUTHORISED"
        )

        # Create the invoice in Xero
        try:
            invoice_response = accounting_api.create_invoices(tenant_id, Invoices(invoices=[invoice]))
            created_invoice = invoice_response.invoices[0]
        except AccountingBadRequestException as e:
            add_log(f"API Exception when creating invoice from credit memo {credit_note_id}: {e.body}", log_type="error", user_id=user.id)
            return False
        except Exception as e:
            add_log(f"Error creating invoice from credit memo {credit_note_id}: {str(e)}", log_type="error", user_id=user.id)
            return False

        add_log(f"Created invoice from credit memo {original_credit_memo.credit_note_number}.", log_type="general", user_id=user.id)

        # Attach the PDF to the created invoice
        try:
            with open(file_path, 'rb') as pdf_file:
                file_body = pdf_file.read()

                accounting_api.create_invoice_attachment_by_file_name(
                    tenant_id,
                    created_invoice.invoice_id,
                    "Converted Invoice.pdf",
                    file_body,
                    include_online=True
                )

            add_log(f"Attached PDF to invoice {created_invoice.invoice_number}.", log_type="general", user_id=user.id)
        except Exception as e:
            add_log(f"Error attaching PDF to invoice {created_invoice.invoice_number}: {str(e)}", log_type="error", user_id=user.id)
            return False

        # Void the original credit memo
        try:
            accounting_api.update_credit_note(tenant_id, credit_note_id, CreditNotes(credit_notes=[CreditNote(credit_note_id=credit_note_id, status="VOIDED")]))
            add_log(f"Voided original credit memo {original_credit_memo.credit_note_number}.", log_type="general", user_id=user.id)
        except Exception as e:
            add_log(f"Error voiding original credit memo {credit_note_id}: {str(e)}", log_type="error", user_id=user.id)
            return False

        return True

    except KeyError as e:
        add_log(f"KeyError: {str(e)}. Credit memo data: {credit_memo_data}", log_type="error", user_id=user.id)
        return False
    except Exception as e:
        add_log(f"Error converting credit memo {credit_note_id} to invoice: {str(e)}", log_type="error", user_id=user.id)
        return False


def assign_tracking_code_to_credit_note(credit_note, user):
    """
    Assigns or updates a tracking code to a Coca-Cola credit note. If the existing tracking differs from the expected tracking,
    it will be overridden. If all line items have the correct tracking, the credit note will not be updated.

    Args:
        credit_note (dict): The credit note data.
        user (User): The user object with Xero API credentials.

    Returns:
        bool: True if successful, False otherwise.
    """
    api_client, _ = get_xero_client_for_user(user)
    accounting_api = AccountingApi(api_client)

    try:
        # Check if tracking_category_id and tracking_option_id exist in the credit note data
        if 'tracking_category_id' not in credit_note or 'tracking_option_id' not in credit_note:
            raise KeyError(f"Missing tracking_category_id or tracking_option_id in credit note data: {credit_note}")

        tenant_id = credit_note['tenant_id']
        credit_note_id = credit_note['invoice_id']  # In Xero, credit notes have a similar ID field as invoices

        # Fetch the original credit note object
        original_credit_note = accounting_api.get_credit_note(tenant_id, credit_note_id).credit_notes[0]

        # Flag to check if an update is required
        update_required = False

        # Assign tracking information to line items
        line_items_with_tracking = []
        for line_item in original_credit_note.line_items:
            if not line_item.tracking:
                # No tracking assigned, so assign the new tracking
                tracking = LineItemTracking(
                    tracking_category_id=credit_note['tracking_category_id'],
                    tracking_option_id=credit_note['tracking_option_id']
                )
                line_item.tracking = [tracking]
                update_required = True  # An update is required because tracking was missing
            else:
                # Tracking exists, check if it matches the expected tracking
                existing_tracking = line_item.tracking[0]  # Assuming only one tracking per line item
                if (existing_tracking.tracking_category_id != credit_note['tracking_category_id'] or
                        existing_tracking.tracking_option_id != credit_note['tracking_option_id']):
                    # If existing tracking differs, override it
                    tracking = LineItemTracking(
                        tracking_category_id=credit_note['tracking_category_id'],
                        tracking_option_id=credit_note['tracking_option_id']
                    )
                    line_item.tracking = [tracking]
                    update_required = True  # An update is required because tracking was incorrect

            line_items_with_tracking.append(line_item)

        if update_required:
            # Update the credit note only if tracking changes were made
            updated_credit_note = CreditNote(
                credit_note_id=original_credit_note.credit_note_id,
                line_items=line_items_with_tracking
            )
            accounting_api.update_credit_note(tenant_id, credit_note_id, CreditNotes(credit_notes=[updated_credit_note]))
            add_log(f"Updated credit note {original_credit_note.credit_note_number} with new tracking.", log_type="general", user_id=user.id)
        else:
            # Log that no update was needed
            add_log(f"Credit note {original_credit_note.credit_note_number} already has the correct tracking.", log_type="general", user_id=user.id)

        return True

    except KeyError as e:
        add_log(f"KeyError: {str(e)}. Credit note data: {credit_note}", log_type="error", user_id=user.id)
        return False
    except Exception as e:
        add_log(f"Error assigning tracking code to credit note {original_credit_note.credit_note_number}: {str(e)}", log_type="error", user_id=user.id)
        return False
    

def assign_tracking_code_to_invoice(invoice, user):
    """
    Assigns or updates a tracking code to a Coca-Cola invoice. If the existing tracking differs from the expected tracking,
    it will be overridden. If all line items have the correct tracking, the invoice will not be updated.

    Args:
        invoice (dict): The invoice data.
        user (User): The user object with Xero API credentials.

    Returns:
        bool: True if successful, False otherwise.
    """
    api_client, _ = get_xero_client_for_user(user)
    accounting_api = AccountingApi(api_client)

    try:
        # Check if tracking_category_id and tracking_option_id exist in the invoice data
        if 'tracking_category_id' not in invoice or 'tracking_option_id' not in invoice:
            raise KeyError(f"Missing tracking_category_id or tracking_option_id in invoice data: {invoice}")

        tenant_id = invoice['tenant_id']
        invoice_id = invoice['invoice_id']

        # Fetch the original invoice object
        original_invoice = accounting_api.get_invoice(tenant_id, invoice_id).invoices[0]

        # Flag to check if an update is required
        update_required = False

        # Assign tracking information to line items
        line_items_with_tracking = []
        for line_item in original_invoice.line_items:
            if not line_item.tracking:
                # No tracking assigned, so assign the new tracking
                tracking = LineItemTracking(
                    tracking_category_id=invoice['tracking_category_id'],
                    tracking_option_id=invoice['tracking_option_id']
                )
                line_item.tracking = [tracking]
                update_required = True  # An update is required because tracking was missing
            else:
                # Tracking exists, check if it matches the expected tracking
                existing_tracking = line_item.tracking[0]  # Assuming only one tracking per line item
                if (existing_tracking.tracking_category_id != invoice['tracking_category_id'] or
                        existing_tracking.tracking_option_id != invoice['tracking_option_id']):
                    # If existing tracking differs, override it
                    tracking = LineItemTracking(
                        tracking_category_id=invoice['tracking_category_id'],
                        tracking_option_id=invoice['tracking_option_id']
                    )
                    line_item.tracking = [tracking]
                    update_required = True  # An update is required because tracking was incorrect

            line_items_with_tracking.append(line_item)

        if update_required:
            # Update the invoice only if tracking changes were made
            updated_invoice = Invoice(
                invoice_id=original_invoice.invoice_id,
                line_items=line_items_with_tracking
            )
            accounting_api.update_invoice(tenant_id, invoice_id, Invoices(invoices=[updated_invoice]))
            add_log(f"Updated invoice {original_invoice.invoice_number} with new tracking.", log_type="general", user_id=user.id)
        else:
            # Log that no update was needed
            add_log(f"Invoice {original_invoice.invoice_number} already has the correct tracking.", log_type="general", user_id=user.id)

        return True

    except KeyError as e:
        add_log(f"KeyError: {str(e)}. Invoice data: {invoice}", log_type="error", user_id=user.id)
        return False
    except Exception as e:
        add_log(f"Error assigning tracking code to invoice {original_invoice.invoice_number}: {str(e)}", log_type="error", user_id=user.id)
        return False


def post_recharge_purchase_invoice_xero(tenant_id, line_items, contact_id, file_name, file_content, user, end_date, invoice_number):
    api_client, xero_app = get_xero_client_for_user(user)

    try:
        # First, convert the string from 'DD-MMM-YYYY' to 'YYYY-MM-DD'
        end_date_converted = datetime.strptime(end_date, "%d/%m/%Y").strftime('%Y-%m-%d')

        # Then, parse it into full ISO 8601 format with time and timezone
        date_value = dateutil.parser.parse(f"{end_date_converted}T00:00:00Z")
        

        # Create the contact object with the provided contact_id
        contact = Contact(
            contact_id=contact_id
        )
        
       
        # Check if an Accounts Payable invoice with the same invoice number already exists and is not voided
        accounting_api = AccountingApi(api_client)
        existing_invoices = accounting_api.get_invoices(
            tenant_id,
            where=f"Type==\"ACCPAY\" AND InvoiceNumber==\"{invoice_number}\" AND Status!=\"VOIDED\""
        )

        if existing_invoices and existing_invoices.invoices:
            add_log(f"Invoice with number {invoice_number} already exists for tenant {tenant_id}.", log_type="general", user_id=user.id)
            return "ALREADY CREATED"  # Return message indicating the invoice is already created


        # Create and post the invoice
        invoice = Invoice(
            type="ACCPAY", # This denotes an Accounts Receivable invoice (sales)
            contact=contact,
            line_items=line_items,
            date=date_value,
            due_date=date_value,
            status="AUTHORISED",  # To set the invoice to 'Awaiting Approval'
            invoice_number=invoice_number
        )

        invoices = Invoices(invoices=[invoice])

        # Post the invoice
        accounting_api = AccountingApi(api_client)
        created_invoices = accounting_api.create_invoices(tenant_id, invoices=invoices)

        # Get the invoice ID of the created invoice
        invoice_id = created_invoices.invoices[0].invoice_id
        add_log(f"Invoice {invoice_id} posted for tenant {tenant_id}.", log_type="general", user_id=user.id)

        # Attach the original file to the invoice
        accounting_api.create_invoice_attachment_by_file_name(tenant_id, invoice_id, file_name, file_content, 'text/csv')
        add_log(f"File '{file_name}' attached to invoice {invoice_id}.", log_type="general", user_id=user.id)

        return invoice_id

    except Exception as e:
        add_log(f"Error posting invoice for tenant {tenant_id}: {str(e)}", log_type="error", user_id=user.id)
        return None
    

def post_recharge_sales_invoice_xero(tenant_id, line_items, contact_id, file_name, file_content, user, end_date, invoice_number):
    api_client, xero_app = get_xero_client_for_user(user)

    try:
        # First, convert the string from 'DD-MMM-YYYY' to 'YYYY-MM-DD'
        end_date_converted = datetime.strptime(end_date, "%d/%m/%Y").strftime('%Y-%m-%d')

        # Then, parse it into full ISO 8601 format with time and timezone
        date_value = dateutil.parser.parse(f"{end_date_converted}T00:00:00Z")
        

        # Create the contact object with the provided contact_id
        contact = Contact(
            contact_id=contact_id
        )
        
       
        # Check if an Accounts Payable invoice with the same invoice number already exists and is not voided
        accounting_api = AccountingApi(api_client)
        existing_invoices = accounting_api.get_invoices(
            tenant_id,
            where=f"Type==\"ACCREC\" AND InvoiceNumber==\"{invoice_number}\" AND Status!=\"VOIDED\""
        )

        if existing_invoices and existing_invoices.invoices:
            add_log(f"Invoice with number {invoice_number} already exists for tenant {tenant_id}.", log_type="general", user_id=user.id)
            return "ALREADY CREATED"  # Return message indicating the invoice is already created


        # Create and post the invoice
        invoice = Invoice(
            type="ACCREC", # This denotes an Accounts Receivable invoice (sales)
            contact=contact,
            line_items=line_items,
            date=date_value,
            due_date=date_value,
            status="AUTHORISED",  # To set the invoice to 'Awaiting Approval'
            invoice_number=invoice_number
        )

        invoices = Invoices(invoices=[invoice])

        # Post the invoice
        accounting_api = AccountingApi(api_client)
        created_invoices = accounting_api.create_invoices(tenant_id, invoices=invoices)

        # Get the invoice ID of the created invoice
        invoice_id = created_invoices.invoices[0].invoice_id
        add_log(f"Invoice {invoice_id} posted for tenant {tenant_id}.", log_type="general", user_id=user.id)

        # Attach the original file to the invoice
        accounting_api.create_invoice_attachment_by_file_name(tenant_id, invoice_id, file_name, file_content, 'text/csv')
        add_log(f"File '{file_name}' attached to invoice {invoice_id}.", log_type="general", user_id=user.id)

        return invoice_id

    except Exception as e:
        add_log(f"Error posting invoice for tenant {tenant_id}: {str(e)}", log_type="error", user_id=user.id)
        return None
    



def get_last_invoice_number(user, selected_month, selected_year):
    try:
        # Get Xero client and API instance
        api_client, xero_app = get_xero_client_for_user(user)
        accounting_api = AccountingApi(api_client)

        # Use IdentityApi to fetch the user's connections (tenants)
        identity_api = IdentityApi(api_client)

        tenants = XeroTenant.query.filter_by(user_id=user.id).all()

        tenant_id = None

        # Loop through tenants to find the one matching user.company_name
        for tenant in tenants:
            if tenant.tenant_name.strip().lower() == user.company_name.strip().lower():
                tenant_id = tenant.tenant_id
                # Log the tenant ID found
                add_log(f"Tenant found for company '{user.company_name}' with tenant ID: {tenant_id}.", log_type="general", user_id=user.id)
                break

        if tenant_id is None:
            add_log(f"Tenant not found for company '{user.company_name}'.", log_type="error", user_id=user.id)
            return {"status": "error", "message": "Tenant not found for the company."}  # No tenant found
        
        current_date = datetime.now()
        
        statuses = ["AUTHORISED", "PAID", "SUBMITTED"]

        # Calculate the date 3 months ago from today
        three_months_ago = current_date - timedelta(days=90)
        three_months_ago_str = three_months_ago.strftime('%Y, %m, %d')

        # Dynamically use the username in the where clause and filter by date range
        where_clause = f'InvoiceNumber.Contains("{user.username}") AND Date >= DateTime({three_months_ago_str})'


        # Fetch invoices for the tenant from Xero
        invoices_response = accounting_api.get_invoices(tenant_id, where=where_clause, statuses=statuses)
        invoices = invoices_response.invoices  # This is the correct field to access the list of invoices


        # Initialize variables
        company_name_prefix = f"{user.username.strip()} - "
        highest_invoice_number = 1  # Default initial value

        # Filter invoices by the selected month and year
        filtered_invoices = [
            invoice for invoice in invoices
            if invoice.date.month == selected_month and invoice.date.year == selected_year
        ]

        # Check if there are any invoices for the selected month and year with the correct format
        matching_invoices = [
            invoice for invoice in filtered_invoices
            if invoice.invoice_number.startswith(company_name_prefix)
        ]

        if matching_invoices:
            # If matching invoices are found, log and return a message that the month has already been processed
            add_log(f"Invoices for {user.company_name} in {selected_month}/{selected_year} have already been processed.", log_type="error", user_id=user.id)
            #return {"status": "error", "message": f"Month {selected_month}/{selected_year} has already been processed."}

        # Log that no invoices were found for the selected month/year
        add_log(f"No matching invoices found for {user.company_name} in {selected_month}/{selected_year}.", log_type="general", user_id=user.id)

        # If no invoices for the selected month, check all invoices with the correct prefix and find the highest
        for invoice in invoices:
            if invoice.invoice_number.startswith(company_name_prefix):
                invoice_number_parts = invoice.invoice_number.split(' - ')
                if len(invoice_number_parts) == 2:
                    try:
                        number = int(invoice_number_parts[1])
                        if number > highest_invoice_number:
                            highest_invoice_number = number
                    except ValueError:
                        # Log invalid invoice format
                        add_log(f"Invalid invoice number format for invoice '{invoice.invoice_number}' in {user.company_name}.", log_type="error", user_id=user.id)
                        continue

        # Log the highest invoice number found
        add_log(f"Highest invoice number found for {user.company_name}: {highest_invoice_number}.", log_type="general", user_id=user.id)

        # Return the highest invoice number found or 0 if no matching invoices
        return {"status": "success", "highest_invoice_number": highest_invoice_number}

    except Exception as e:
        # Log the error with details
        add_log(f"Error fetching last invoice number for tenant {tenant_id}: {str(e)}", log_type="error", user_id=user.id)
        return {"status": "error", "message": f"Error fetching invoice data: {str(e)}"}




