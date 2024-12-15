# app/admin_views.py

from flask_admin.contrib.sqla import ModelView
from werkzeug.security import generate_password_hash
from flask import flash





class UserAdmin(ModelView):
        """Custom admin view for managing users."""
        
        # Fields to display in the list view
        column_list = ['username', 'company_name']
        
        # Fields to include in the form when editing/creating users
        form_columns = ['username', 'password', 'company_name']
        
        # Fields to exclude from the form view (sensitive tokens)
        form_excluded_columns = ['xero_token', 'refresh_token']
        
        # Exclude the password column from the list view (for security reasons)
        column_exclude_list = ['password']
        
        # Customize the form display for the password field
        form_widget_args = {
            'password': {'type': 'password'},  # Hide password input in the form

        }
        
        # Perform custom logic when a user is created or updated
        def on_model_change(self, form, model, is_created):
            # If the password field is populated, hash it before storing it
            if form.password.data:
                model.password = generate_password_hash(form.password.data)

            # Ensure that Xero Client ID and Secret are provided
            if not form.xero_client_id.data or not form.xero_client_secret.data:
                flash('Warning: Xero Client ID and Secret are required for Xero API access', 'warning')

            # Call the parent method to handle other model changes
            return super(UserAdmin, self).on_model_change(form, model, is_created)

        # Customize how the username and company name are displayed in the list view
        column_labels = {
            'username': 'User Name',
            'company_name': 'Company Name',

        }

        # Add filters for easier management of users
        column_filters = ['company_name', 'username']

        # Optional: Add search functionality to make finding users easier
        column_searchable_list = ['username', 'company_name']
        
        # Enable exporting the user list to CSV, Excel, etc.
        can_export = True