# app/models.py

from app import db 
from datetime import datetime
from enum import Enum
from werkzeug.security import generate_password_hash
from flask_login import UserMixin


#---------------tables----------------#


from app import db
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin


# In app/models.py
class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=True)
    password = db.Column(db.String(120), nullable=False)
    xero_token = db.Column(db.Text, nullable=True)
    refresh_token = db.Column(db.Text, nullable=True)
    token_expires_at = db.Column(db.Float, nullable=True)  
    company_name = db.Column(db.String(255), nullable=False)


    def set_password(self, password):
        """Hash and set the user's password."""
        self.password = generate_password_hash(password)

    def check_password(self, password):
        """Check the user's password hash against the provided password."""
        return check_password_hash(self.password, password)

    def get_id(self):
        """Return the user ID as a unique identifier."""
        return str(self.id)  # Flask-Login expects this to be a string

    def is_active(self):
        """Return True if the user is active."""
        return True  # By default, assume all users are active

    def is_authenticated(self):
        """Return True if the user is authenticated."""
        return True  # Flask-Login uses this to verify authentication

    def is_anonymous(self):
        """Return False for registered users."""
        return False  # Only anonymous users should return True

    def __repr__(self):
        return f'<User {self.username}>'


class XeroTenant(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    tenant_id = db.Column(db.String(255), nullable=False)
    tenant_name = db.Column(db.String(255), nullable=False)
    tenant_type = db.Column(db.String(50), nullable=False)
    user = db.relationship('User', backref=db.backref('xero_tenants', lazy=True))




    
class AccountTransaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    source = db.Column(db.String, nullable=False)
    contact = db.Column(db.String, nullable=True)
    description = db.Column(db.String, nullable=True)
    reference = db.Column(db.String, nullable=True)
    debit = db.Column(db.Float, nullable=True)
    credit = db.Column(db.Float, nullable=True)
    gross = db.Column(db.Float, nullable=True)
    net = db.Column(db.Float, nullable=True)
    vat = db.Column(db.Float, nullable=True)
    account_code = db.Column(db.Integer, nullable=True)  # Change to Integer
    account = db.Column(db.String, nullable=True)
    tracking_group1 = db.Column(db.String, nullable=True)
    tracking_group2 = db.Column(db.String, nullable=True)
    upload_time = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<AccountTransaction {self.id}>'
    
class GroupTrackingCode(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    group_code = db.Column(db.String(255), nullable=False)  # e.g., 'KDGFS'
    upload_time = db.Column(db.DateTime, default=datetime.utcnow)

    # Define relationship to the association table
    tracking_codes = db.relationship('TrackingCode', secondary='group_tracking_code_mapping', back_populates='groups')

class TrackingCode(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    tracking_code = db.Column(db.String(255), nullable=False)  # e.g., 'GRAVESEND'
    upload_time = db.Column(db.DateTime, default=datetime.utcnow)

    # Define relationship to the association table
    groups = db.relationship('GroupTrackingCode', secondary='group_tracking_code_mapping', back_populates='tracking_codes')

    
group_tracking_code_mapping = db.Table('group_tracking_code_mapping',
    db.Column('group_id', db.Integer, db.ForeignKey('group_tracking_code.id'), primary_key=True),
    db.Column('tracking_code_id', db.Integer, db.ForeignKey('tracking_code.id'), primary_key=True)
)

# Table for companies
class Company(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    company_name = db.Column(db.String(255), nullable=False)
    company_code = db.Column(db.String(50), nullable=True)
    upload_time = db.Column(db.DateTime, default=datetime.utcnow)

# Business-specific account codes
class AccountCodesPerBusiness(db.Model):
    __tablename__ = 'account_codes_per_business'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)  # User ID to track who owns these records
    account_code_per_business = db.Column(db.String(255), nullable=False)
    descriptor_per_business = db.Column(db.String(255), nullable=False)

    # Relationship with AccountCodesPerDMS
    dms_accounts = db.relationship('AccountCodesPerDMS', backref='business', lazy=True)

# DMS-specific account codes
class AccountCodesPerDMS(db.Model):
    __tablename__ = 'account_codes_per_dms'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)  # User ID to track who owns these records
    account_code_per_dms = db.Column(db.String(255), nullable=False)
    descriptor_per_dms = db.Column(db.String(255), nullable=False)
    
    # Foreign key to the business account codes table
    business_id = db.Column(db.Integer, db.ForeignKey('account_codes_per_business.id'), nullable=False)

class DomPurchaseInvoicesTenant(db.Model):
    __tablename__ = 'dom_purchase_invoices_tenants'

    id = db.Column(db.Integer, primary_key=True)
    tenant_name = db.Column(db.String(255), nullable=False)
    user_id = db.Column(db.Integer, nullable=False)

class LogEntry(db.Model):
    __tablename__ = 'log_entries'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)  # Link to the logged-in user
    log_type = db.Column(db.String(50), nullable=False)  # 'general' or 'error'
    message = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

class TrackingCategoryModel(db.Model):
    __tablename__ = 'tracking_categories'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    tenant_name = db.Column(db.String(255), nullable=False)
    tracking_category_id = db.Column(db.String(255), nullable=False)
    tracking_category_name = db.Column(db.String(255), nullable=False)
    tracking_category_option = db.Column(db.String(255), nullable=False)
    tracking_option_id = db.Column(db.String(255), nullable=False, unique=True)
    store_number = db.Column(db.String(255), nullable=True)
    store_postcode = db.Column(db.String(255), nullable=True)
    store_contact = db.Column(db.String(255), nullable=True)

    
    def to_dict(self):
        return {
            "tenant_name": self.tenant_name,
            "tracking_category_id": self.tracking_category_id,
            "tracking_category_name": self.tracking_category_name,
            "tracking_category_option": self.tracking_category_option,
            "tracking_option_id": self.tracking_option_id,
            "store_number": self.store_number,
            "store_postcode": self.store_postcode,
            "store_contact": self.store_contact,
        }

# Transaction model that stores user ID, nominal code, supplier description, and links to StoreAccountCodes
class DomNominalCodes(db.Model):
    __tablename__ = 'dom_nominal_codes'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)  # User ID to track which user created this transaction
    nominal_code = db.Column(db.String(255), nullable=False)  # Nominal code
    supplier_description = db.Column(db.String(255), nullable=False)  # Supplier description
    
    # Foreign key to link to store-specific account codes
    store_account_code_id = db.Column(db.Integer, db.ForeignKey('store_account_codes.id'), nullable=False)

    # Relationship to StoreAccountCodes
    store_account_code = db.relationship('StoreAccountCodes', backref='dom_nominal_codes', lazy=True)

    def __repr__(self):
        return f'<DomNominalCodes {self.nominal_code}>'


class StoreAccountCodes(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    account_code = db.Column(db.String(50), nullable=False)
    account_name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.String(255), nullable=True)
    account_type = db.Column(db.String(50), nullable=False)
    tax_type = db.Column(db.String(50), nullable=True)  # New column for Tax Type
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)

    def __init__(self, account_code, account_name, description, account_type, user_id,tax_type):
        self.account_code = account_code
        self.account_name = account_name
        self.description = description
        self.tax_type = tax_type
        self.account_type = account_type.name if isinstance(account_type, Enum) else account_type  # Convert enum to string if needed
        self.user_id = user_id


class TaskStatus(db.Model):
    __tablename__ = 'task_status'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.String(255), nullable=False, unique=True)
    user_id = db.Column(db.Integer, nullable=False)  # Associate with user
    task_type = db.Column(db.String(50), nullable=False)  # Type of task (e.g., 'recharging', 'uploading')
    status = db.Column(db.String(50), default='pending')  # Status: pending, in_progress, completed, failed
    result = db.Column(db.Text, nullable=True)  # Result or error message
    progress = db.Column(db.Integer, default=0)  # New column for progress
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class InvoiceRecord(db.Model):
    __tablename__ = 'invoice_record'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    invoice_type = db.Column(db.String(50), nullable=False)  # Invoice type (e.g., purchase_invoice)
    week_number = db.Column(db.Integer, nullable=False)      # Week number (1-52)
    year = db.Column(db.Integer, nullable=False)             # Year (e.g., 2024)
    store_number = db.Column(db.String(20), nullable=False)  # Store number (e.g., S11111)
    store_name = db.Column(db.String(100), nullable=False)   # Store name
    tenant_name = db.Column(db.String(100), nullable=False)  # Tenant name


class SupplierInvoiceRecord(db.Model):
    __tablename__ = 'supplier_invoice_record'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.Integer, nullable=True)  # Allow NULL
    store_name = db.Column(db.String(255), nullable=True)  # Allow NULL
    invoice_type = db.Column(db.String(100), nullable=True)  # Allow NULL
    invoice_number = db.Column(db.String(100), nullable=False)  # Keep NOT NULL
    invoice_id = db.Column(db.String(100), nullable=True)  # Allow NULL
    errors = db.Column(db.Text, nullable=True)  # Allow NULL
    run_time = db.Column(db.DateTime, default=datetime.utcnow, nullable=True)  # Allow NULL
    triggered_by = db.Column(db.String(50), nullable=True)  # Allow NULL
    date_of_invoice = db.Column(db.Date, nullable=False)  # Keep NOT NULL



class TaskSchedule(db.Model):
    __tablename__ = 'task_schedules'

    id = db.Column(db.Integer, primary_key=True)
    task_name = db.Column(db.String(100), unique=True, nullable=False)
    schedule_type = db.Column(db.String(20), nullable=False, default='interval')  # 'interval' or 'crontab'
    interval_minutes = db.Column(db.Integer, nullable=True)  # For interval scheduling
    specific_time = db.Column(db.Time, nullable=True)  # For crontab-like scheduling
    last_run = db.Column(db.DateTime, default=None)  # Last run time
    next_run = db.Column(db.DateTime, default=None)  # Next run time
    is_active = db.Column(db.Boolean, default=True)  # Enable/disable tasks
    arguments = db.Column(db.String, nullable=True)  # Store arguments as a string




#---------------------------------------------------------------------------tables----------------------------------------------------------------------------#
