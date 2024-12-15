import pandas as pd
from run import app  # Import your app from run.py
from app import db
from app.models import GroupTrackingCode, TrackingCode
from flask import current_app

# Function to export tracking codes and group tracking codes in vertical format
def export_tracking_codes_for_all_companies():
    # Get all distinct user_ids (representing companies) from GroupTrackingCode and TrackingCode
    user_ids = db.session.query(GroupTrackingCode.user_id).distinct().all()
    
    for user_id in user_ids:
        user_id = user_id[0]  # Extract user_id from tuple

        # Fetch all tracking codes for the company
        tracking_codes = db.session.query(TrackingCode).filter_by(user_id=user_id).all()

        # Create a list of dictionaries for tracking codes to write to Excel
        tracking_code_data = [
            {'Tracking Code': code.tracking_code, 'Upload Time': code.upload_time}
            for code in tracking_codes
        ]
        df_tracking_codes = pd.DataFrame(tracking_code_data)

        # Fetch all group tracking codes for the company
        group_tracking_codes = db.session.query(GroupTrackingCode).filter_by(user_id=user_id).all()

        # Prepare data for group tracking codes and their associated tracking codes
        group_tracking_code_data = {}
        max_len = 0
        for group in group_tracking_codes:
            group_code = f"{group.group_code} ({len(group.tracking_codes)})"  # Append count to group code
            assigned_codes = [code.tracking_code for code in group.tracking_codes]
            group_tracking_code_data[group_code] = assigned_codes
            max_len = max(max_len, len(assigned_codes))  # Find the max length of assigned codes
        
        # Adjust the group tracking code data so that all columns are of equal length
        for group_code, codes in group_tracking_code_data.items():
            # Pad the assigned codes list with empty strings if its length is less than max_len
            group_tracking_code_data[group_code] = codes + [''] * (max_len - len(codes))

        # Convert dictionary into a DataFrame, where each column is a group code
        df_group_tracking_codes = pd.DataFrame(group_tracking_code_data)

        # Create an Excel writer object to write both dataframes to separate sheets
        file_name = f"tracking_codes_company_{user_id}.xlsx"
        with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
            # Write the tracking codes to one sheet
            df_tracking_codes.to_excel(writer, sheet_name='Tracking Codes', index=False)
            
            # Write the group tracking codes and their assigned tracking codes to another sheet
            df_group_tracking_codes.to_excel(writer, sheet_name='Group Tracking Codes', index=False)

        print(f"Excel file created for company {user_id}: {file_name}")

# Run the export function within the Flask application context
if __name__ == "__main__":
    with app.app_context():  # Ensure the Flask app context is active
        export_tracking_codes_for_all_companies()
