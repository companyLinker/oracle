import streamlit as st
import pandas as pd
import io
import zipfile
import re

# ==========================================
# 0. PAGE CONFIG & KIOSK MODE (EXTREME CLEAN UI)
# ==========================================

# This must be the very first Streamlit command
st.set_page_config(page_title="DSR Post-Processing", layout="wide")

# CSS to hide the top header, hamburger menu, footer, "Manage app" button,
# and specifically the profile container and Streamlit host badge.
hide_st_style = """
            <style>
            header {visibility: hidden !important;}
            #MainMenu {visibility: hidden !important;}
            footer {visibility: hidden !important;}
            .stDeployButton {display:none !important;}
            .stAppDeployButton {display:none !important;}
            
            [class*="_profileContainer_"], [class*="_profilePreview_"] {
                display: none !important;
            }
            [class*="_viewerBadge_"], [class*="_container_gzau3_"] {
                display: none !important;
            }
            .stActionButton, .stStatusWidget, [data-testid="stStatusWidget"], [data-testid="appCreatorAvatar"] {
                display: none !important;
            }
            .block-container {
                padding-top: 1rem !important;
            }
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

# ==========================================
# 1. CORE DATA PROCESSING FUNCTION
# ==========================================

def process_dsr_file(uploaded_file):
    """Processes a single DSR Excel file and returns the output Excel file bytes."""
    
    # === Step 1: Read the stacked tables ===
    # Read directly from the uploaded file buffer
    raw_df = pd.read_excel(uploaded_file, sheet_name=0, header=None, dtype=str)

    # FIX: Reset the index after dropping empty rows so row numbers line up perfectly
    raw_df = raw_df.dropna(how='all').reset_index(drop=True)

    # Define expected columns for each section to help identify them
    headers_map = {
        "sales": ["Location Name", "Business Date", "Taxable Sales", "Tax Collected", "Tax Exempt Sales", "Tax Rate"],
        "tender": ["Location Name", "Business Date", "Tender Name", "Tender Amount"],
        "fee": ["Location Name", "Business Date", "Service Charge Name", "Service Charge Amount"],
        "dy": ["Location Name", "Business Date", "Order Type Name", "Net Sales"]
    }

    # Find where each section starts dynamically
    section_starts = {}
    for i, row in raw_df.iterrows():
        val0 = str(row[0]).strip() if pd.notna(row[0]) else ""
        val2 = str(row[2]).strip() if pd.notna(row[2]) else ""
        
        if val0 == "Location Name":
            if val2 == "Taxable Sales":
                section_starts["sales"] = i
            elif val2 == "Tender Name":
                section_starts["tender"] = i
            elif val2 == "Service Charge Name":
                section_starts["fee"] = i
            elif val2 == "Order Type Name":
                section_starts["dy"] = i

    # Sort sections by their starting index to slice them
    sorted_sections = sorted([(v, k) for k, v in section_starts.items()])

    dfs = {}
    for idx, (start_row, block_name) in enumerate(sorted_sections):
        # Determine the end of the current block
        if idx < len(sorted_sections) - 1:
            end_row = sorted_sections[idx+1][0]
        else:
            end_row = len(raw_df)

        # Slice the dataframe for the current block
        block_df = raw_df.iloc[start_row:end_row].copy()
        
        # Set the first row as the header
        block_df.columns = block_df.iloc[0].astype(str).str.strip()
        block_df = block_df[1:]  # Drop the header row from the data
        
        # Clean up empty rows, repeat headers, and grand totals
        block_df = block_df[block_df['Location Name'].notna()]
        block_df = block_df[block_df['Location Name'].astype(str).str.strip() != '']
        block_df = block_df[block_df['Location Name'] != 'Location Name']
        block_df = block_df[~block_df['Location Name'].astype(str).str.contains('Total', case=False, na=False)]
        
        # Keep only the target columns for this block
        target_cols = headers_map[block_name]
        valid_cols = [c for c in target_cols if c in block_df.columns]
        block_df = block_df[valid_cols]
        
        # Convert amount columns to numeric 
        for col in block_df.columns:
            if col not in ["Location Name", "Business Date", "Tender Name", "Service Charge Name", "Order Type Name"]:
                # Remove any commas before converting to float
                block_df[col] = block_df[col].astype(str).str.replace(',', '', regex=False)
                block_df[col] = pd.to_numeric(block_df[col], errors='coerce').fillna(0)
                
        dfs[block_name] = block_df

    # Extract the individual dataframes or create empty ones with correct columns if a block was missing
    sales_df = dfs.get("sales", pd.DataFrame(columns=headers_map["sales"]))
    fee_df = dfs.get("fee", pd.DataFrame(columns=headers_map["fee"]))
    tender_df = dfs.get("tender", pd.DataFrame(columns=headers_map["tender"]))
    dy_df = dfs.get("dy", pd.DataFrame(columns=headers_map["dy"]))

    # === Step 2: Clean column names and FORMAT DATE ===
    for df in [sales_df, fee_df, tender_df, dy_df]:
        if not df.empty:
            df.columns = df.columns.str.strip()
            if 'Business Date' in df.columns:
                # Convert to datetime, then string format 'M/D/YYYY' (removes leading zeros from month and day)
                df['Business Date'] = pd.to_datetime(df['Business Date'], errors='coerce')
                df['Business Date'] = df['Business Date'].dt.strftime('%m/%d/%Y').str.replace(r'^0', '', regex=True).str.replace(r'/0', '/', regex=True)

    # === Step 3: Pivot fee, tender, and dy ===
    fee_pivot = fee_df.pivot_table(
        index=["Location Name", "Business Date"],
        columns="Service Charge Name",
        values="Service Charge Amount",
        aggfunc="sum",
        fill_value=0
    ).reset_index() if not fee_df.empty else pd.DataFrame(columns=["Location Name", "Business Date"])

    tender_pivot = tender_df.pivot_table(
        index=["Location Name", "Business Date"],
        columns="Tender Name",
        values="Tender Amount",
        aggfunc="sum",
        fill_value=0
    ).reset_index() if not tender_df.empty else pd.DataFrame(columns=["Location Name", "Business Date"])

    dy_pivot = dy_df.pivot_table(
        index=["Location Name", "Business Date"],
        columns="Order Type Name",
        values="Net Sales",
        aggfunc="sum",
        fill_value=0
    ).reset_index() if not dy_df.empty else pd.DataFrame(columns=["Location Name", "Business Date"])

    # === Step 4: Merge Sales + Fee + Tender ===
    merged_df = sales_df.merge(fee_pivot, on=["Location Name", "Business Date"], how="left")
    merged_df = merged_df.merge(tender_pivot, on=["Location Name", "Business Date"], how="left")
    merged_df = merged_df.fillna(0)

    # === Step 4.5: Append EXACT formatting and extra columns for DY_Pivot ===
    if not dy_pivot.empty:
        temp_df = dy_pivot[['Location Name', 'Business Date']].merge(
            merged_df, on=['Location Name', 'Business Date'], how='left'
        )
        
        dd_val = temp_df['DoorDash'] if 'DoorDash' in temp_df.columns else 0
        gh_val = temp_df['GrubHub'] if 'GrubHub' in temp_df.columns else 0
        ue_val = temp_df['Uber Eats'] if 'Uber Eats' in temp_df.columns else 0
        tax_rate_val = temp_df['Tax Rate'] if 'Tax Rate' in temp_df.columns else 0

        dy_pivot['DY DoorDash_copy'] = dd_val
        dy_pivot['DY GrubHub_copy'] = gh_val
        dy_pivot['DY UberEatsP'] = ""
        dy_pivot['DY UberEats_copy'] = ue_val
        
        dy_pivot['TDY DoorDash'] = dd_val
        dy_pivot['TDY GrubHub'] = gh_val
        dy_pivot['TDY UberEatsP'] = 0
        dy_pivot['TDY UberEats'] = ue_val
        
        dy_pivot['tax'] = 0
        dy_pivot['non tab'] = ue_val
        dy_pivot['tax rate'] = tax_rate_val
        dy_pivot['Doordash taxable'] = 1
        dy_pivot['GrubHub Taxable'] = 1
        dy_pivot['UberEats Taxable'] = 0
        
        dy_pivot['    '] = dd_val + gh_val
        
        new_cols = []
        for c in dy_pivot.columns:
            if c == 'DY DoorDash_copy': new_cols.append('DY DoorDash')
            elif c == 'DY GrubHub_copy': new_cols.append('DY GrubHub')
            elif c == 'DY UberEats_copy': new_cols.append('DY UberEats')
            elif c == '    ': new_cols.append('') 
            else: new_cols.append(c)
        dy_pivot.columns = new_cols

    # === Step 5: Dynamic column ordering ===
    core_columns = ["Location Name", "Business Date", "Taxable Sales", "Tax Collected", "Tax Exempt Sales"]
    priority_columns = ["$ Tip", "Delivery Fee", "Geographical Fee", "Service Fee", "Service Charge"]
    other_columns = [c for c in merged_df.columns if c not in core_columns + priority_columns]
    
    final_columns = [c for c in core_columns if c in merged_df.columns] \
                  + [c for c in priority_columns if c in merged_df.columns] \
                  + other_columns
    merged_df = merged_df[final_columns]

    # === Step 6: Write out to BytesIO buffer ===
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
        merged_df.to_excel(writer, index=False, sheet_name="Combined")
        dy_pivot.to_excel(writer, index=False, sheet_name="DY_Pivot")
    
    return output_buffer.getvalue()

# ==========================================
# 2. STREAMLIT APP INTERFACE
# ==========================================

st.title("📊 DSR Excel Processor")
st.markdown("Upload your raw DSR Excel file(s) below to clean, pivot, and generate the formatted Combined and DY_Pivot sheets.")

st.markdown("### 📁 Upload Files")
uploaded_files = st.file_uploader("Drop your Excel Files here (.xlsx)", accept_multiple_files=True, type=['xlsx'])

if st.button("🚀 Process Files", type="primary"):
    if not uploaded_files:
        st.error("Please upload at least one Excel file.")
    else:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Dictionary to hold our processed files in memory
        processed_files = {}

        for i, uploaded_file in enumerate(uploaded_files):
            status_text.text(f"Processing {uploaded_file.name}...")
            
            try:
                # Run the exact code logic on the current file
                excel_bytes = process_dsr_file(uploaded_file)
                
                # Create a cleaned output filename
                clean_name = re.sub(r'\.xlsx$', '', uploaded_file.name, flags=re.IGNORECASE)
                processed_files[f"{clean_name}_Processed.xlsx"] = excel_bytes
                
            except Exception as e:
                st.error(f"Error processing {uploaded_file.name}: {e}")
                
            progress_bar.progress((i + 1) / len(uploaded_files))

        if processed_files:
            status_text.text("Done! Preparing download...")
            
            # If the user only uploaded 1 file, give them a direct Excel download
            if len(processed_files) == 1:
                file_name, file_bytes = list(processed_files.items())[0]
                st.success("Processing Complete!")
                st.download_button(
                    label=f"📥 Download {file_name}",
                    data=file_bytes,
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            # If they uploaded multiple files, pack them into a ZIP like the reference app
            else:
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w") as zf:
                    for file_name, file_bytes in processed_files.items():
                        zf.writestr(file_name, file_bytes)
                
                st.success("Processing Complete!")
                st.download_button(
                    label="📥 Download All Processed Files (ZIP)",
                    data=zip_buffer.getvalue(),
                    file_name="Processed_DSR_Files.zip",
                    mime="application/zip",
                    use_container_width=True
                )
