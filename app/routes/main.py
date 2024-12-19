# app/routes/main.py (updated)

from flask import Blueprint, render_template, request, jsonify
from celery.result import AsyncResult
from app import celery
from flask import Blueprint, render_template, session
from flask_login import current_user
from app.models import *
from app.routes.auth import user_login_required
from app.xero import *


main_bp = Blueprint('main', __name__)

# routes.py or main.py
ALLOWED_ACCOUNT_TYPES = [
    'BANK', 'CURRENT', 'CURRLIAB', 'DEPRECIATN', 'DIRECTCOSTS', 'EQUITY',
    'EXPENSE', 'FIXED', 'INVENTORY', 'LIABILITY', 'NONCURRENT', 'OTHERINCOME',
    'OVERHEADS', 'PREPAYMENT', 'REVENUE', 'SALES', 'TERMLIAB'
]

ALLOWED_TAX_TYPES = [
    'CAPEXINPUT',
    'CAPEXINPUT2',
    'CAPEXOUTPUT',
    'CAPEXOUTPUT2',
    'CAPEXSRINPUT',
    'CAPEXSROUTPUT',
    'ECACQUISITIONS',
    'ECZRINPUT',
    'ECZROUTPUT',
    'ECZROUTPUTSERVICES',
    'EXEMPTINPUT',
    'EXEMPTOUTPUT',
    'GSTONIMPORTS',
    'INPUT2',
    'NONE',
    'OUTPUT2',
    'REVERSECHARGES',
    'RRINPUT',
    'RROUTPUT',
    'SRINPUT',
    'SROUTPUT',
    'ZERORATEDINPUT',
    'ZERORATEDOUTPUT'
]




@main_bp.route('/')
@main_bp.route('/home')
@user_login_required  # Protect the home route
def home():
    user_name = current_user.company_name 
    return render_template('home.html', username=user_name)



@main_bp.route('/settings')
@user_login_required  # Protect the home route
def settings():
    if 'user_id' not in session:
        return redirect(url_for('user_login'))

    # Get the current user's ID and company name
    user_id = session['user_id']  # Get the current user's ID from session
    user_company_name = current_user.company_name  # Get the current user's company name

    # Fetch all tenants for the current user, excluding those with tenant_name matching the user's company name
    connected_tenants = [tenant.tenant_name for tenant in XeroTenant.query.filter_by(user_id=user_id).filter(XeroTenant.tenant_name != user_company_name).all()]


    tracking_codes = TrackingCode.query.filter_by(user_id=session['user_id']).all()
    group_tracking_codes = GroupTrackingCode.query.filter_by(user_id=session['user_id']).all()
    dom_tenants = DomPurchaseInvoicesTenant.query.filter_by(user_id=session['user_id']).all()
    business_account_codes = AccountCodesPerBusiness.query.filter_by(user_id=current_user.id).all()
    

    # Load nominal codes for the current user
    nominal_codes = DomNominalCodes.query.filter_by(user_id=session['user_id']).all()

    # Load store account codes to populate the dropdown
    store_account_codes = StoreAccountCodes.query.filter_by(user_id=session['user_id']).order_by(StoreAccountCodes.account_code.asc()).all()


    return render_template('settings.html', business_account_codes=business_account_codes, allowed_tax_types=ALLOWED_TAX_TYPES, allowed_account_types=ALLOWED_ACCOUNT_TYPES, tracking_codes=tracking_codes, group_tracking_codes=group_tracking_codes, dom_tenants=dom_tenants,connected_tenants=connected_tenants, nominal_codes=nominal_codes, store_account_codes=store_account_codes)




@main_bp.route("/xero_settings", methods=["GET"])
@user_login_required  # Protect the xero_settings route
def xero_settings():
    if current_user.is_authenticated:
        # Fetch connected tenants for the current user
        tenants = XeroTenant.query.filter_by(user_id=current_user.id).all()

        # Determine token storage status
        access_token_stored = bool(current_user.xero_token)
        refresh_token_stored = bool(current_user.refresh_token)

        # Extract the full access and refresh tokens
        access_token = None
        refresh_token = None
        if current_user.xero_token:
            token_data = json.loads(current_user.xero_token)
            access_token = token_data
            refresh_token = token_data.get("refresh_token")

        # Format token expiry
        token_expires_at = (
            datetime.fromtimestamp(current_user.token_expires_at)
            if current_user.token_expires_at else None
        )
    else:
        tenants = []
        access_token_stored = False
        refresh_token_stored = False
        token_expires_at = None
        access_token = None
        refresh_token = None

    return render_template(
        "xero_settings.html",
        tenants=tenants,
        title="Xero Settings",
        access_token_stored=access_token_stored,
        refresh_token_stored=refresh_token_stored,
        token_expires_at=token_expires_at,
        access_token=access_token,
        refresh_token=refresh_token,
    )




@main_bp.route('/auto_workflows')
@user_login_required  # Protect the home route
def auto_workflows():
    return render_template('auto_workflows.html')


@main_bp.route('/scheduled_tasks')
@user_login_required  # Protect the home route
def scheduled_tasks():
    return render_template('scheduled_tasks.html')


# =======================================================================================
#                                    DATA SETTINGS
# =======================================================================================

@main_bp.route('/sync_with_xero', methods=['POST'])
@user_login_required
def sync_with_xero():
    try:
        # Step 1: Get the current user's company name
        user_company_name = current_user.company_name
        
        # Step 2: Call the function to get tracking codes from Xero for the current user
        xero_tracking_codes = get_tracking_categories_from_xero(current_user)
        
        # Step 3: Filter tracking codes to only those that match the user's company name (tenant_name)
        xero_tracking_codes = [code for code in xero_tracking_codes if code['tenant_name'] == user_company_name]

        # Step 4: Retrieve tracking codes and group tracking codes from the local database
        local_tracking_codes = [code.tracking_code for code in TrackingCode.query.filter_by(user_id=session['user_id']).all()]
        local_group_codes = [group.group_code for group in GroupTrackingCode.query.filter_by(user_id=session['user_id']).all()]

        # Step 5: Find codes that are in Xero but not in the database
        codes_to_add = [code['tracking_category_option'] for code in xero_tracking_codes if code['tracking_category_option'] not in local_tracking_codes and code['tracking_category_option'] not in local_group_codes]

        # Step 6: Find codes that are in the database but not in Xero
        codes_not_in_xero = [code for code in local_tracking_codes + local_group_codes if code not in [x['tracking_category_option'] for x in xero_tracking_codes]]

        # Step 7: Return the lists of codes to be added and codes that are no longer in Xero
        return jsonify({
            'codes_to_add': codes_to_add,
            'codes_not_in_xero': codes_not_in_xero
        })

    except Exception as e:
        print(f"Error syncing tracking codes: {e}")  # Log the error
        return jsonify({'status': 'error', 'message': str(e)}), 500
    

@main_bp.route('/add_tracking_code_from_xero', methods=['POST'])
@user_login_required
def add_tracking_code_from_xero():
    tracking_code = request.form.get('code')
    
    # Add the tracking code to the database if it doesn't already exist
    if not TrackingCode.query.filter_by(user_id=session['user_id'], tracking_code=tracking_code).first():
        new_code = TrackingCode(user_id=session['user_id'], tracking_code=tracking_code)
        db.session.add(new_code)
        db.session.commit()

    return jsonify({'status': 'success'})

@main_bp.route('/add_group_tracking_code_from_xero', methods=['POST'])
@user_login_required
def add_group_tracking_code_from_xero():
    group_code = request.form.get('code')
    
    # Add the group tracking code to the database if it doesn't already exist
    if not GroupTrackingCode.query.filter_by(user_id=session['user_id'], group_code=group_code).first():
        new_group_code = GroupTrackingCode(user_id=session['user_id'], group_code=group_code)
        db.session.add(new_group_code)
        db.session.commit()

    return jsonify({'status': 'success'})



@main_bp.route('/add_tracking_code', methods=['POST'])
def add_tracking_code():
    if 'user_id' not in session:
        return redirect(url_for('user_login'))

    tracking_code = request.form['tracking_code']
    new_tracking_code = TrackingCode(user_id=session['user_id'], tracking_code=tracking_code)
    db.session.add(new_tracking_code)
    db.session.commit()
    return jsonify({'status': 'success', 'tracking_code': tracking_code})

@main_bp.route('/add_group_tracking_code', methods=['POST'])
def add_group_tracking_code():
    if 'user_id' not in session:
        return redirect(url_for('user_login'))

    group_code = request.form['group_code']
    selected_tracking_codes = request.form.getlist('selected_tracking_codes')

    new_group = GroupTrackingCode(user_id=session['user_id'], group_code=group_code)
    db.session.add(new_group)
    db.session.commit()

    tracking_codes = TrackingCode.query.filter(TrackingCode.tracking_code.in_(selected_tracking_codes)).all()
    new_group.tracking_codes.extend(tracking_codes)
    db.session.commit()

    return jsonify({'status': 'success', 'group_code': group_code})

@main_bp.route('/delete_tracking_code', methods=['POST'])
def delete_tracking_code():
    if 'user_id' not in session:
        return redirect(url_for('user_login'))

    tracking_code_id = request.form['tracking_code_id']
    tracking_code = TrackingCode.query.get(tracking_code_id)
    if tracking_code:
        db.session.delete(tracking_code)
        db.session.commit()
        return jsonify({'status': 'success'})
    return jsonify({'status': 'error'}), 404

@main_bp.route('/delete_group_tracking_code', methods=['POST'])
def delete_group_tracking_code():
    if 'user_id' not in session:
        return redirect(url_for('user_login'))

    group_code_id = request.form['group_code_id']
    group_code = GroupTrackingCode.query.get(group_code_id)
    if group_code:
        db.session.delete(group_code)
        db.session.commit()
        return jsonify({'status': 'success'})
    return jsonify({'status': 'error'}), 404

@main_bp.route('/update_group_tracking_codes', methods=['POST'])
def update_group_tracking_codes():
    if 'user_id' not in session:
        return redirect(url_for('user_login'))
    
    user_id = session['user_id']  # Get the current user ID from the session

    group_code = request.form['group_code']
    tracking_codes = request.form.getlist('tracking_codes[]')

    # Find the group by its code
    group = GroupTrackingCode.query.filter_by(group_code=group_code, user_id=user_id).first()


    # Clear the existing tracking codes for the group
    group.tracking_codes = []

    # Add the new tracking codes to the group
    # Add the new tracking codes to the group
    for code in tracking_codes:
        tracking_code = TrackingCode.query.filter_by(tracking_code=code, user_id=user_id).first()  # Ensure the tracking code belongs to the user
        if tracking_code:
            group.tracking_codes.append(tracking_code)

    db.session.commit()

    return jsonify({"status": "success"})




@main_bp.route('/get_tracking_codes', methods=['GET'])
def get_tracking_codes():
    user_id = session['user_id']  # Get the current user's ID
    tracking_codes = TrackingCode.query.filter_by(user_id=user_id).all()  # Filter by user_id
    tracking_codes_list = [{"id": code.id, "tracking_code": code.tracking_code} for code in tracking_codes]
    return jsonify({"tracking_codes": tracking_codes_list})

@main_bp.route('/get_group_tracking_codes', methods=['GET'])
def get_group_tracking_codes():
    user_id = session['user_id']  # Get the current user's ID
    group_tracking_codes = GroupTrackingCode.query.filter_by(user_id=user_id).all()  # Filter by user_id
    group_tracking_codes_list = [{
        "id": group.id,
        "group_code": group.group_code,
        "tracking_codes": [{"tracking_code": code.tracking_code} for code in group.tracking_codes]
    } for group in group_tracking_codes]
    return jsonify({"group_tracking_codes": group_tracking_codes_list})

@main_bp.route('/get_companies', methods=['GET'])
def get_companies():
    user_id = session['user_id']  # Get the current user's ID
    companies = Company.query.filter_by(user_id=user_id).all()  # Filter by user_id
    companies_data = [{"id": company.id, "company_name": company.company_name, "company_code": company.company_code} for company in companies]
    return jsonify({"companies": companies_data})

@main_bp.route('/update_company_codes', methods=['POST'])
def update_company_codes():
    try:
        updated_companies = request.get_json().get('companies', [])
        add_log(f"Received data for updating company codes: {updated_companies}", "general")

        # Iterate over each updated company code
        for company_data in updated_companies:
            company_id = company_data['company_id']
            company_code = company_data['company_code']
            add_log(f"Updating company {company_id} with code {company_code}", "general")

            # Find the company by ID and update the company code
            company = Company.query.get(company_id)
            if company:
                company.company_code = company_code if company_code != '' else None  # Allow blank company codes
                db.session.add(company)

        db.session.commit()  # Commit all changes
        add_log(f"Company codes updated successfully for companies: {updated_companies}", "general")
        return jsonify({'status': 'success'})
    except Exception as e:
        db.session.rollback()
        add_log(f"Error updating company codes: {str(e)}", "error")
        return jsonify({'status': 'error', 'message': str(e)})


# Flask route to sync Xero account codes (filtered by user_id)
@main_bp.route('/sync_xero_account_codes', methods=['POST'])
def sync_xero_account_codes():
    # Get the logged-in user's ID from session
    user_id = session.get('user_id')

    # Fetch account codes from Xero (for demonstration purposes, you replace this with an actual API call)
    xero_account_codes = get_xero_account_codes(current_user)
    
    # Get existing account codes from the database, filtered by user_id
    existing_account_codes = [code.account_code_per_dms for code in AccountCodesPerDMS.query.filter_by(user_id=user_id).all()]


    # Find the new account codes that are not yet in the database
    codes_to_add = [code for code in xero_account_codes if code not in existing_account_codes]
    
    # Return the list of new codes to add to the frontend
    return jsonify({
        'codes_to_add': codes_to_add
    })


@main_bp.route('/add_account_code_from_xero', methods=['POST'])
def add_account_code_from_xero():
    account_code = request.form.get('code')
    business_id = request.form.get('business_id')
    descriptor_per_dms = request.form.get('descriptor_per_dms')  # Get the descriptor

    if not account_code or not business_id or not descriptor_per_dms:
        return jsonify({'status': 'error', 'message': 'Missing account code, business ID, or descriptor'}), 400

    try:
        # Insert the account code, business ID, and descriptor into the database
        new_account_code = AccountCodesPerDMS(
            account_code_per_dms=account_code,
            business_id=business_id,
            descriptor_per_dms=descriptor_per_dms,
            user_id=session['user_id']
        )
        db.session.add(new_account_code)
        db.session.commit()

        return jsonify({'status': 'success'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500





@main_bp.route('/get_business_account_codes', methods=['GET'])
def get_business_account_codes():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"error": "User not logged in"}), 403
    
    business_accounts = AccountCodesPerBusiness.query.filter_by(user_id=user_id).all()


    business_account_data = [
        {
            "id": account.id,
            "account_code_per_business": account.account_code_per_business,
            "descriptor_per_business": account.descriptor_per_business
        }
        for account in business_accounts
    ]
    return jsonify({"business_account_codes": business_account_data})


@main_bp.route('/add_business_account_code', methods=['POST'])
@user_login_required
def add_business_account_code():
    business_account_code = request.form.get('business_account_code')
    business_descriptor = request.form.get('business_descriptor')

    if not business_account_code or not business_descriptor:
        return jsonify({'success': False, 'message': 'Missing required fields'}), 400

    # Check for duplicates
    existing_code = AccountCodesPerBusiness.query.filter_by(
        account_code_per_business=business_account_code,
        user_id=current_user.id
    ).first()
    if existing_code:
        return jsonify({'success': False, 'message': 'Business account code already exists'}), 400

    # Create a new AccountCodesPerBusiness object
    new_business_account_code = AccountCodesPerBusiness(
        account_code_per_business=business_account_code,
        descriptor_per_business=business_descriptor,
        user_id=current_user.id
    )
    db.session.add(new_business_account_code)
    db.session.commit()

    return jsonify({
        'success': True,
        'business_account_code': {
            'id': new_business_account_code.id,
            'business_account_code': new_business_account_code.account_code_per_business,
            'business_descriptor': new_business_account_code.descriptor_per_business
        }
    })


@main_bp.route('/edit_business_account_code', methods=['POST'])
@user_login_required
def edit_business_account_code():
    business_account_code_id = request.form.get('business_account_code_id')
    business_account_code = request.form.get('business_account_code')
    business_descriptor = request.form.get('business_descriptor')

    if not business_account_code_id or not business_account_code or not business_descriptor:
        return jsonify({'success': False, 'message': 'Missing required fields'}), 400

    # Fetch the existing record
    code = AccountCodesPerBusiness.query.filter_by(
        id=business_account_code_id,
        user_id=current_user.id
    ).first()
    if not code:
        return jsonify({'success': False, 'message': 'Business Account Code not found'}), 404

    # Update the record
    code.account_code_per_business = business_account_code
    code.descriptor_per_business = business_descriptor
    db.session.commit()

    return jsonify({'success': True})

@main_bp.route('/delete_business_account_code/<int:code_id>', methods=['POST'])
@user_login_required
def delete_business_account_code(code_id):
    # Find the business account code for the current user
    code = AccountCodesPerBusiness.query.filter_by(
        id=code_id,
        user_id=current_user.id
    ).first()

    if not code:
        return jsonify({'success': False, 'message': 'Business Account Code not found'}), 404

    # Check if there are any dependent records in account_codes_per_dms
    dependent_records = AccountCodesPerDMS.query.filter_by(business_id=code_id).all()

    if dependent_records:
        return jsonify({
            'success': False,
            'message': 'Cannot delete this Business Account Code as it is associated with other records.'
        }), 400

    # Proceed with deletion if no dependencies
    db.session.delete(code)
    db.session.commit()

    return jsonify({'success': True})



@main_bp.route('/get_dms_account_codes', methods=['GET'])
def get_dms_account_codes():
    user_id = session['user_id']  # Get the current user's ID
    dms_accounts = AccountCodesPerDMS.query.filter_by(user_id=user_id).all()  # Filter by user_id
    
    # Assuming there's a relationship between AccountCodesPerDMS and the business account codes
    dms_account_data = []
    for account in dms_accounts:
        dms_account_data.append({
            "id": account.id,
            "account_code_per_dms": account.account_code_per_dms,
            "descriptor_per_dms": account.descriptor_per_dms,
            "account_code_per_business": account.business.account_code_per_business if account.business else "N/A",
            "descriptor_per_business": account.business.descriptor_per_business if account.business else "N/A"

        })

    return jsonify({"dms_account_codes": dms_account_data})




@main_bp.route('/add_company', methods=['POST'])
def add_company():
    company_name = request.form.get('company_name')
    company_code = request.form.get('company_code')

    new_company = Company(company_name=company_name, company_code=company_code, user_id=session['user_id'])  # Assuming you have user tracking
    db.session.add(new_company)
    db.session.commit()
    return jsonify({"status": "success"})



@main_bp.route('/add_dms_account_code', methods=['POST'])
def add_dms_account_code():
    account_code = request.form.get('dms_account_code')
    descriptor = request.form.get('dms_descriptor')
    business_id = request.form.get('business_id')  # ID of the associated business account code

    new_dms_account = AccountCodesPerDMS(account_code_per_dms=account_code, descriptor_per_dms=descriptor, business_id=business_id, user_id=session['user_id'])
    db.session.add(new_dms_account)
    db.session.commit()
    return jsonify({"status": "success"})

@main_bp.route('/delete_company', methods=['POST'])
def delete_company():
    company_id = request.form.get('company_id')
    company = Company.query.get(company_id)
    if company:
        db.session.delete(company)
        db.session.commit()
        return jsonify({"status": "success"})
    return jsonify({"status": "failure"})


@main_bp.route('/delete_dms_account_code', methods=['POST'])
def delete_dms_account_code():
    dms_account_id = request.form.get('dms_account_id')
    dms_account = AccountCodesPerDMS.query.get(dms_account_id)
    if dms_account:
        db.session.delete(dms_account)
        db.session.commit()
        return jsonify({"status": "success"})
    return jsonify({"status": "failure"})


@main_bp.route('/sync_tracking_categories_with_xero', methods=['POST'])
def sync_tracking_categories_with_xero():
    # Step 1: Fetch all tracking categories for all tenants
    xero_tracking_codes = get_tracking_categories_from_xero(current_user)

    # Step 2: Fetch the tenants from XeroTenant for the current user, excluding tenants containing the user's company name
    user_id = current_user.id
    company_name = current_user.company_name.strip().lower()  # Normalize the user's company name

    # Filter tenants for the current user, excluding those whose tenant_name contains the user's company name
    user_tenants = XeroTenant.query.filter(
        XeroTenant.user_id == user_id,
        ~XeroTenant.tenant_name.ilike(f"%{company_name}%")  # Exclude tenant names that contain the company name
    ).all()

    # Normalize tenant names (convert to lowercase and strip whitespace)
    user_tenant_names = [tenant.tenant_name.strip().lower() for tenant in user_tenants]


    # Step 3: Filter tracking categories, keeping only those that match the user's tenants
    filtered_tracking_codes = [
        code for code in xero_tracking_codes if code['tenant_name'].strip().lower() in user_tenant_names
    ]

    # Step 4: Insert filtered tracking codes into the TrackingCategory database, avoiding duplicates
    added_codes = 0
    for code in filtered_tracking_codes:
        existing_category = TrackingCategoryModel.query.filter_by(tracking_option_id=code['tracking_option_id']).first()

        if not existing_category:  # Only add if not already in the database
            new_tracking_category = TrackingCategoryModel(
                user_id=user_id,
                tenant_name=code['tenant_name'],
                tracking_category_id=code['tracking_category_id'],
                tracking_category_name=code['tracking_category_name'],
                tracking_category_option=code['tracking_category_option'],
                tracking_option_id=code['tracking_option_id'],
                store_number=None,  # Set these as None for now unless you have additional logic
                store_postcode=None,
                store_contact=None
            )
            db.session.add(new_tracking_category)
            added_codes += 1

    # Step 5: Get all tracking option IDs from the filtered Xero data
    xero_tracking_option_ids = [code['tracking_option_id'] for code in filtered_tracking_codes]

    # Step 6: Fetch all tracking categories from the database for this user
    existing_tracking_categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()

    # Step 7: Identify and delete tracking categories that are in the database but not in Xero
    deleted_codes = 0
    for category in existing_tracking_categories:
        if category.tracking_option_id not in xero_tracking_option_ids:
            db.session.delete(category)
            deleted_codes += 1

    # Commit changes to the database
    db.session.commit()

    return jsonify({
        'status': 'success',
        'message': f'{added_codes} tracking categories synced successfully, {deleted_codes} tracking categories deleted.'
    })



# Route to get tracking categories from the database
@main_bp.route('/get_tracking_categories', methods=['GET'])

def get_tracking_categories():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify([])

    tracking_categories = TrackingCategoryModel.query.filter_by(user_id=user_id).all()
    return jsonify([category.to_dict() for category in tracking_categories])



@main_bp.route('/save_tracking_categories', methods=['POST'])

def save_tracking_categories():
    if not request.is_json:
        return jsonify({'status': 'error', 'message': 'Invalid content type, expected application/json'}), 415

    try:
        categories = request.json.get('categories', [])
        user_id = session.get('user_id')

        if not user_id:
            return jsonify({'status': 'error', 'message': 'User not logged in'})

        print(f"User ID: {user_id}")  # Debugging: Check the user ID
        print(f"Categories to save: {categories}")  # Debugging: Check the incoming data

        # Loop through the categories and save them to the database
        for category in categories:
            tracking_option_id = category['tracking_option_id']
            store_number = category['store_number']
            store_postcode = category['store_postcode']
            store_contact = category['store_contact']

            # Find the existing tracking category using tracking_option_id
            existing_category = TrackingCategoryModel.query.filter_by(
                user_id=user_id,
                tracking_option_id=tracking_option_id
            ).first()

            if existing_category:
                print(f"Updating category with tracking_option_id: {tracking_option_id}")  # Debugging: Log which category is being updated
                existing_category.store_number = store_number
                existing_category.store_postcode = store_postcode
                existing_category.store_contact = store_contact
            else:
                print(f"Category with tracking_option_id {tracking_option_id} not found for user {user_id}")  # Debugging: If no category found

        db.session.commit()
        print("Changes committed to the database.")  # Debugging: Confirm database commit
        return jsonify({'status': 'success'})
    
    except Exception as e:
        print(f"Error: {str(e)}")  # Debugging: Print any errors
        return jsonify({'status': 'error', 'message': str(e)})
    


@main_bp.route('/add_tenant', methods=['POST'])
def add_tenant():
    tenant_name = request.form.get('tenant_name')
    if tenant_name:
        new_tenant = DomPurchaseInvoicesTenant(tenant_name=tenant_name, user_id=session['user_id'])
        db.session.add(new_tenant)
        db.session.commit()
        
        return jsonify({"status": "success", "tenant_id": new_tenant.id})
    
    return jsonify({"status": "error", "message": "Tenant name is required"})



@main_bp.route('/delete_tenant/<int:tenant_id>', methods=['POST'])
def delete_tenant(tenant_id):
    tenant = DomPurchaseInvoicesTenant.query.filter_by(id=tenant_id, user_id=session['user_id']).first()
    if tenant:
        db.session.delete(tenant)
        db.session.commit()
        return jsonify({"status": "success"})
    
    return jsonify({"status": "error", "message": "Tenant not found"})


#Dom Nomical Code Management

@main_bp.route('/add_nominal_code', methods=['POST'])
@user_login_required
def add_nominal_code():
    nominal_code = request.form.get('nominal_code')
    supplier_description = request.form.get('supplier_description')
    store_account_code_id = request.form.get('store_account_code_id')

    if not nominal_code or not supplier_description or not store_account_code_id:
        return jsonify({'success': False, 'message': 'Missing required fields'}), 400

    # Check if the store account code exists and belongs to the user
    store_account_code = StoreAccountCodes.query.filter_by(id=store_account_code_id, user_id=current_user.id).first()
    if not store_account_code:
        return jsonify({'success': False, 'message': 'Invalid Store Account Code'}), 400

    # Check if the nominal code already exists for the user
    existing_nominal_code = DomNominalCodes.query.filter_by(user_id=current_user.id, nominal_code=nominal_code).first()
    if existing_nominal_code:
        return jsonify({'success': False, 'message': 'Nominal code already exists for this user'}), 400

    # Create new nominal code if it doesn't exist
    new_nominal_code = DomNominalCodes(
        user_id=current_user.id,
        nominal_code=nominal_code,
        supplier_description=supplier_description,
        store_account_code_id=store_account_code_id
    )

    db.session.add(new_nominal_code)
    db.session.commit()

    # Prepare the data to return
    code_data = {
        'id': new_nominal_code.id,
        'nominal_code': new_nominal_code.nominal_code,
        'supplier_description': new_nominal_code.supplier_description,
        'store_account_code': {
            'id': store_account_code.id,
            'account_code': store_account_code.account_code,
            'account_name': store_account_code.account_name
        },
        'store_account_code_id': new_nominal_code.store_account_code_id
    }

    return jsonify({'success': True, 'nominal_code': code_data})


@main_bp.route('/get_nominal_codes', methods=['GET'])
@user_login_required
def get_nominal_codes():
    nominal_codes = DomNominalCodes.query.filter_by(user_id=current_user.id).all()
    result = []
    for code in nominal_codes:
        result.append({
            'id': code.id,
            'nominal_code': code.nominal_code,
            'supplier_description': code.supplier_description,
            'store_account_code': {
                'id': code.store_account_code.id,
                'account_code': code.store_account_code.account_code,
                'account_name': code.store_account_code.account_name
            },
            'store_account_code_id': code.store_account_code_id
        })
    return jsonify({'nominal_codes': result})


@main_bp.route('/edit_nominal_code/<int:nominal_code_id>', methods=['POST'])
@user_login_required
def edit_nominal_code(nominal_code_id):
    nominal_code_value = request.form.get('nominal_code')
    supplier_description = request.form.get('supplier_description')
    store_account_code_id = request.form.get('store_account_code_id')

    if not nominal_code_value or not supplier_description or not store_account_code_id:
        return jsonify({'success': False, 'message': 'Missing required fields'}), 400

    # Retrieve the nominal code and ensure it belongs to the current user
    code = DomNominalCodes.query.filter_by(id=nominal_code_id, user_id=current_user.id).first()
    if not code:
        return jsonify({'success': False, 'message': 'Nominal Code not found'}), 404

    # Verify the new store account code
    store_account_code = StoreAccountCodes.query.filter_by(id=store_account_code_id, user_id=current_user.id).first()
    if not store_account_code:
        return jsonify({'success': False, 'message': 'Invalid Store Account Code'}), 400

    # Update the nominal code
    code.nominal_code = nominal_code_value
    code.supplier_description = supplier_description
    code.store_account_code_id = store_account_code_id

    db.session.commit()

    # Fetch the updated code with the new store_account_code
    updated_code = DomNominalCodes.query.filter_by(id=nominal_code_id, user_id=current_user.id).first()
    updated_store_account_code = StoreAccountCodes.query.filter_by(id=updated_code.store_account_code_id, user_id=current_user.id).first()

    return jsonify({
        'success': True,
        'nominal_code': {
            'id': updated_code.id,
            'nominal_code': updated_code.nominal_code,
            'supplier_description': updated_code.supplier_description,
            'store_account_code': {
                'id': updated_store_account_code.id,
                'account_code': updated_store_account_code.account_code,
                'account_name': updated_store_account_code.account_name
            },
            'store_account_code_id': updated_code.store_account_code_id
        }
    })


@main_bp.route('/delete_nominal_code/<int:nominal_code_id>', methods=['POST'])
@user_login_required
def delete_nominal_code(nominal_code_id):
    # Retrieve the nominal code and ensure it belongs to the current user
    code = DomNominalCodes.query.filter_by(id=nominal_code_id, user_id=current_user.id).first()
    if not code:
        return jsonify({'success': False, 'message': 'Nominal Code not found'}), 404

    db.session.delete(code)
    db.session.commit()

    return jsonify({'success': True})

@main_bp.route('/add_store_account_code', methods=['POST'])
@user_login_required  # Ensure the user is logged in
def add_store_account_code():
    account_code = request.form.get('account_code')
    account_name = request.form.get('account_name')
    account_type = request.form.get('account_type')
    tax_type = request.form.get('tax_type')  # Get tax_type from form
    description = request.form.get('description')  # Get description from form

    if not account_code or not account_name or not account_type or not tax_type:
        return jsonify({'success': False, 'message': 'Missing required fields'}), 400

    if account_type not in ALLOWED_ACCOUNT_TYPES:
        return jsonify({'success': False, 'message': 'Invalid account type selected'}), 400

    if tax_type not in ALLOWED_TAX_TYPES:
        return jsonify({'success': False, 'message': 'Invalid tax type selected'}), 400

    # Check for duplicates if necessary
    existing_code = StoreAccountCodes.query.filter_by(account_code=account_code, user_id=current_user.id).first()
    if existing_code:
        return jsonify({'success': False, 'message': 'Account code already exists'}), 400

    new_store_account_code = StoreAccountCodes(
        account_code=account_code,
        account_name=account_name,
        description=description,  # Include description
        account_type=account_type,
        user_id=current_user.id,
        tax_type=tax_type  # Include tax_type
    )
    db.session.add(new_store_account_code)
    db.session.commit()

    return jsonify({
        'success': True,
        'store_account_code': {
            'id': new_store_account_code.id,
            'account_code': new_store_account_code.account_code,
            'account_name': new_store_account_code.account_name,
            'account_type': new_store_account_code.account_type,
            'tax_type': new_store_account_code.tax_type,
            'description': new_store_account_code.description
        }
    })


@main_bp.route('/edit_store_account_code', methods=['POST'])
@user_login_required
def edit_store_account_code():
    store_account_code_id = request.form.get('store_account_code_id')
    account_code = request.form.get('account_code')
    account_name = request.form.get('account_name')
    account_type = request.form.get('account_type')
    tax_type = request.form.get('tax_type')
    description = request.form.get('description')

    if not store_account_code_id or not account_code or not account_name or not account_type or not tax_type:
        return jsonify({'success': False, 'message': 'Missing required fields'}), 400

    if account_type not in ALLOWED_ACCOUNT_TYPES:
        return jsonify({'success': False, 'message': 'Invalid account type selected'}), 400

    if tax_type not in ALLOWED_TAX_TYPES:
        return jsonify({'success': False, 'message': 'Invalid tax type selected'}), 400

    # Fetch the existing record
    store_account_code = StoreAccountCodes.query.filter_by(id=store_account_code_id, user_id=current_user.id).first()
    if not store_account_code:
        return jsonify({'success': False, 'message': 'Store Account Code not found'}), 404

    # Update the record
    store_account_code.account_code = account_code
    store_account_code.account_name = account_name
    store_account_code.account_type = account_type
    store_account_code.tax_type = tax_type
    store_account_code.description = description

    db.session.commit()

    return jsonify({'success': True})


@main_bp.route('/delete_store_account_code/<int:store_account_code_id>', methods=['POST'])
@user_login_required
def delete_store_account_code(store_account_code_id):
    store_account_code = StoreAccountCodes.query.filter_by(id=store_account_code_id, user_id=current_user.id).first()
    if not store_account_code:
        return jsonify({'success': False, 'message': 'Store Account Code not found'}), 404

    db.session.delete(store_account_code)
    db.session.commit()

    return jsonify({'success': True})

@main_bp.route('/sync_xero_store_account_codes', methods=['POST'])
@user_login_required
def sync_xero_store_account_codes_route():
    result = sync_store_account_codes_with_xero(current_user)
    if result['status'] == 'success':
        return jsonify(result)
    else:
        return jsonify(result), 400
    
@main_bp.route('/get_dom_purchase_invoice_logs')
def get_dom_purchase_invoice_logs():
    if 'user_id' in session:
        user_id = session['user_id']
        # Fetch logs related to Dom Purchase Invoices for the logged-in user
        dom_purchase_logs = LogEntry.query.filter_by(user_id=user_id, log_type='dom_purchase_invoice').order_by(LogEntry.timestamp.desc()).all()
        
        # Prepare a list of log data to send to the front-end
        log_data = []
        for log in dom_purchase_logs:
            # Parse relevant data from the log message
            log_message = log.message
            timestamp = log.timestamp.strftime('%Y-%m-%d %H:%M:%S')

            # Check if the log message contains 'Processed dom purchase invoice'
            if 'Processed dom purchase invoice' in log_message:
                # Extract parts of the log using regex to handle variable spaces, single quotes, and commas
                
                match = re.search(r"Tenant Name '([^']+)' Processed dom purchase invoice '([^']+)' with attached pdf '([^']+)', Total invoice amount '([^']+)', Statement Date '([^']+)', Store code '([^']+)'", log_message)

                if match:
                    tenant_name = match.group(1)  # Extract tenant name
                    csv_filename = match.group(2)  # Extract CSV filename
                    pdf_filename = match.group(3)  # Extract PDF filename
                    total_invoice = match.group(4)  # Extract total invoice amount
                    statement_date = match.group(5)  # Extract statement date
                    store_code = match.group(6)  # Extract store code

                    log_data.append({
                        'timestamp': timestamp,
                        'tenant_name': tenant_name,
                        'csv_filename': csv_filename,
                        'pdf_filename': pdf_filename,
                        'total_invoice': total_invoice,
                        'statement_date': statement_date,
                        'store_code': store_code
                    })

        return jsonify(log_data)
    else:
        return jsonify({'error': 'User not logged in'}), 401


    

@main_bp.route('/get_dom_sales_invoice_logs')
def get_dom_sales_invoice_logs():
    if 'user_id' in session:
        user_id = session['user_id']
        # Fetch logs related to Dom Sales Invoices for the logged-in user
        dom_sales_logs = LogEntry.query.filter_by(user_id=user_id, log_type='dom_sales_invoice').order_by(LogEntry.timestamp.desc()).all()

        # Prepare a list of log data to send to the front-end
        log_data = []
        for log in dom_sales_logs:
            log_message = log.message
            timestamp = log.timestamp.strftime('%Y-%m-%d %H:%M:%S')

            # Parse the log message to extract tenant name, file name, supplier type, start date, and end date
            if 'Processed dom sales invoice' in log_message:
                parts = log_message.split("'")
                if len(parts) >= 11:  # Adjusted to accommodate start and end date
                    tenant_name = parts[1]  # Extract tenant name
                    file_name = parts[3]    # Extract file name
                    supplier_name = parts[5]  # Extract supplier name
                    start_date = parts[7]  # Extract start date
                    end_date = parts[9]  # Extract end date

                    log_data.append({
                        'timestamp': timestamp,
                        'tenant_name': tenant_name,
                        'file_name': file_name,
                        'supplier_name': supplier_name,
                        'start_date': start_date,
                        'end_date': end_date
                    })

        return jsonify(log_data)
    else:
        return jsonify({'error': 'User not logged in'}), 401


