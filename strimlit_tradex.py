import streamlit as st
import pandas as pd
import re
from datetime import datetime, timedelta

st.set_page_config(page_title="Tradex Dialer Data ETL App", layout="wide")
st.title("Tradex Dialer Data ETL App")

def file_upload_with_na(label, key):
    col1, col2 = st.columns([3, 1])
    with col1:
        file = st.file_uploader(f"{label} (CSV)", type="csv", key=key)
    with col2:
        na = st.checkbox("NA", key=f"na_{key}")
    return file, na

voiso_file, voiso_na = file_upload_with_na("Voiso", "voiso")
tata_file, tata_na = file_upload_with_na("Tata", "tata")
know_file, know_na = file_upload_with_na("Knowlarity", "know")
qconn_file, qconn_na = file_upload_with_na("Qkonnect", "qconn")
stringee_file, stringee_na = file_upload_with_na("Stringee", "stringee")
team_tradex_file = st.file_uploader("Team_tradex (MANDATORY)", type="csv", key="team_tradex")

DATE_FORMATS = {
    'voiso': (r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', 'mm/dd/yyyy HH:mm:ss'),
    'tata': (r'\d{4}-\d{2}-\d{2}', 'yyyy-mm-dd'),
    'know': (r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', 'yyyy-mm-dd HH:mm:ss'),
    'qconn': (r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', 'yyyy-mm-dd HH:mm:ss'),
    'stringee': (r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AP]M', 'mm/dd/yyyy HH:mm:ss AM/PM')
}

def get_sample_and_format(df, col, regex, fmt):
    sample = df[col].dropna().astype(str).iloc[0] if not df.empty else "N/A"
    valid = bool(re.match(regex, sample))
    return sample, valid, fmt

if team_tradex_file:
    ref_d = pd.read_csv(team_tradex_file, low_memory=False)
else:
    st.warning("Please upload the Team_tradex CSV file to continue.")

run_anyway = False

if st.button("Checkpoint 1: Validate Date Formats"):
    st.markdown("### Checkpoint 1: Date Format Validation")
    validation_errors = []
    dfs = {}
    samples = []

    if voiso_file and not voiso_na:
        dfs['voiso'] = pd.read_csv(voiso_file, low_memory=False)
        sample, valid, fmt = get_sample_and_format(dfs['voiso'], 'Date and time', *DATE_FORMATS['voiso'])
        samples.append(f"voiso: {sample} (Expected: {fmt})")
        if not valid:
            validation_errors.append(f"Voiso: Expected {fmt}, found '{sample}'")
    if tata_file and not tata_na:
        dfs['tata'] = pd.read_csv(tata_file, low_memory=False)
        sample, valid, fmt = get_sample_and_format(dfs['tata'], 'Call Start Date', *DATE_FORMATS['tata'])
        samples.append(f"tata: {sample} (Expected: {fmt})")
        if not valid:
            validation_errors.append(f"Tata: Expected {fmt}, found '{sample}'")
    if know_file and not know_na:
        dfs['know'] = pd.read_csv(know_file, low_memory=False)
        sample, valid, fmt = get_sample_and_format(dfs['know'], 'Date and Time', *DATE_FORMATS['know'])
        samples.append(f"know: {sample} (Expected: {fmt})")
        if not valid:
            validation_errors.append(f"Knowlarity: Expected {fmt}, found '{sample}'")
    if qconn_file and not qconn_na:
        dfs['qconn'] = pd.read_csv(qconn_file, low_memory=False)
        sample, valid, fmt = get_sample_and_format(dfs['qconn'], 'Date time', *DATE_FORMATS['qconn'])
        samples.append(f"qconn: {sample} (Expected: {fmt})")
        if not valid:
            validation_errors.append(f"Qkonnect: Expected {fmt}, found '{sample}'")
    if stringee_file and not stringee_na:
        dfs['stringee'] = pd.read_csv(stringee_file, low_memory=False)
        sample, valid, fmt = get_sample_and_format(dfs['stringee'], 'Start time', *DATE_FORMATS['stringee'])
        samples.append(f"stringee: {sample} (Expected: {fmt})")
        if not valid:
            validation_errors.append(f"Stringee: Expected {fmt}, found '{sample}'")
    st.markdown("**Sample Dates:**")
    for s in samples:
        st.write(s)
    if validation_errors:
        st.error("Date format issues found:")
        for error in validation_errors:
            st.write(error)
        if st.button("Run Anyway (Checkpoint 1)"):
            run_anyway = True
    else:
        st.success("All date formats validated successfully!")
        run_anyway = True

if run_anyway or st.session_state.get("run_anyway_1", False):
    st.session_state["run_anyway_1"] = True

    # ========== DATA PROCESSING ==========
    # Build selected_dfs for concat
    selected_dfs = []
    # Tata
    if 'tata' in dfs:
        tata = dfs['tata']
        new_tata = tata[['Call Start Date', 'Connected to Agent','Call Status','Answer Duration (HH:MM:SS)',
                        'Hold Duration (HH:MM:SS)','Total Call Duration (HH:MM:SS)','Call Start Time','Customer Number']]
        tata_copy = new_tata.copy()
        tata_copy['Source'] = 'Tata'
        tata_copy.rename(columns={
            'Call Start Date': 'Date',
            'Connected to Agent': 'Dialer Name',
            'Customer Number' : 'Number',
            'Call Status': 'Call Status',
            'Answer Duration (HH:MM:SS)': 'Talk Time',
            'Hold Duration (HH:MM:SS)': 'Hold Time',
            'Total Call Duration (HH:MM:SS)': 'Total Call Duration',
            'Call Start Time': 'Call Start Time'
        }, inplace=True)
        tata_selected = tata_copy[['Source','Date', 'Dialer Name','Number', 'Call Status','Call Start Time','Total Call Duration', 'Talk Time', 'Hold Time']]
        selected_dfs.append(tata_selected)
    # Knowlarity
    if 'know' in dfs:
        know = dfs['know']
        new_Know = know[['Date and Time', 'Agent Name','Call Status', 'Talk Time (hh:mm:ss)', 'Hold Time (hh:mm:ss)','Total Call Duration (hh:mm:ss)','Customer']]
        new_Know['Date and Time'] = pd.to_datetime(new_Know['Date and Time'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        new_Know['Date'] = new_Know['Date and Time'].dt.date
        new_Know['Call Start Time'] = new_Know['Date and Time'].dt.strftime('%H:%M:%S')
        know_copy = new_Know.copy()
        know_copy['Source'] = 'Knowlarity'
        know_copy.rename(columns={
            'Date': 'Date',
            'Agent Name': 'Dialer Name',
            'Customer': 'Number',
            'Call Status': 'Call Status',
            'Talk Time (hh:mm:ss)': 'Talk Time',
            'Hold Time (hh:mm:ss)': 'Hold Time',
            'Total Call Duration (hh:mm:ss)': 'Total Call Duration',
            'Call Start Time': 'Call Start Time'
        }, inplace=True)
        know_selected = know_copy[['Source','Date', 'Dialer Name','Number' ,'Call Status', 'Call Start Time','Total Call Duration','Talk Time', 'Hold Time']]
        selected_dfs.append(know_selected)
    # Voiso
    if 'voiso' in dfs:
        voiso = dfs['voiso']
        new_voiso = voiso[['Date and time','Agent(s)','Disposition','Talk time','Duration','DNIS/To']]
        new_voiso['Date and time'] = pd.to_datetime(new_voiso['Date and time'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
        new_voiso['Date'] = new_voiso['Date and time'].dt.date
        new_voiso['Call Start Time'] = new_voiso['Date and time'].dt.strftime('%H:%M:%S')
        voiso_copy = new_voiso.copy()
        voiso_copy['Source'] = 'Voiso'
        voiso_copy.rename(columns={
            'Date': 'Date',
            'Agent(s)': 'Dialer Name',
            'DNIS/To': 'Number',
            'Disposition': 'Call Status',
            'Talk time': 'Talk Time',
            'Duration': 'Total Call Duration',
            'Call Start Time': 'Call Start Time'
        }, inplace=True)
        voiso_selected = voiso_copy[['Source','Date', 'Dialer Name', 'Number','Call Status','Call Start Time','Total Call Duration', 'Talk Time']]
        selected_dfs.append(voiso_selected)
    # Qkonnect
    if 'qconn' in dfs:
        Qconn = dfs['qconn']
        new_qconn = Qconn[['Date time','Agent Mobile','Call Event','Transfer Duration','Duration','User Mobile']]
        new_qconn['Date time'] = pd.to_datetime(new_qconn['Date time'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        new_qconn['Date'] = new_qconn['Date time'].dt.date
        new_qconn['Call Start Time'] = new_qconn['Date time'].dt.strftime('%H:%M:%S')
        qconn_copy = new_qconn.copy()
        qconn_copy['Source'] = 'Qkonnect'
        qconn_copy.rename(columns={
            'Date': 'Date',
            'Agent Mobile': 'Dialer Name',
            'User Mobile': 'Number',
            'Call Event': 'Call Status',
            'Transfer Duration': 'Talk Time',
            'Duration': 'Total Call Duration',
            'Call Start Time': 'Call Start Time'
        }, inplace=True)
        qconn_selected = qconn_copy[['Source','Date', 'Dialer Name','Number', 'Call Status','Call Start Time','Total Call Duration', 'Talk Time']]
        selected_dfs.append(qconn_selected)
    # Stringee
    if 'stringee' in dfs:
        df = dfs['stringee']
        def duration_to_timedelta(duration):
            try:
                hours, minutes, seconds = map(int, str(duration).split(':'))
                return timedelta(hours=hours, minutes=minutes, seconds=seconds)
            except:
                return timedelta(0)
        df['Queue Duration (timedelta)'] = df['Queue duration'].apply(duration_to_timedelta)
        df['Answer Duration (timedelta)'] = df['Answer duration'].apply(duration_to_timedelta)
        df['Total Duration (timedelta)'] = df['Queue Duration (timedelta)'] + df['Answer Duration (timedelta)']
        df['Total Duration'] = df['Total Duration (timedelta)'].apply(lambda x: str(x).split(", ")[-1])
        df = df.drop(columns=['Queue Duration (timedelta)', 'Answer Duration (timedelta)', 'Total Duration (timedelta)'])
        new_string = df[['Start time','Account','Call status','Answer duration','Hold duration','Total Duration','Customer number']]
        new_string['Start time'] = pd.to_datetime(new_string['Start time'], errors='coerce', format='%m/%d/%Y %I:%M:%S %p')
        new_string['Date'] = new_string['Start time'].dt.date
        new_string['Call Start Time'] = new_string['Start time'].dt.strftime('%H:%M:%S')
        string_copy = new_string.copy()
        string_copy['Source'] = 'Stringee'
        string_copy.rename(columns={
            'Date': 'Date',
            'Account': 'Dialer Name',
            'Customer number': 'Number',
            'Call status': 'Call Status',
            'Answer duration': 'Talk Time',
            'Hold duration': 'Hold Time',
            'Total Duration': 'Total Call Duration',
            'Call Start Time': 'Call Start Time'
        }, inplace=True)
        string_selected = string_copy[['Source','Date', 'Dialer Name','Number', 'Call Status','Call Start Time','Total Call Duration', 'Talk Time', 'Hold Time']]
        selected_dfs.append(string_selected)

    if not selected_dfs:
        st.error("No dialer files selected or uploaded. Please upload at least one dialer file or uncheck NA.")
        st.stop()
    D = pd.concat(selected_dfs, ignore_index=True)

    # Checkpoint 2: Null Date Entries
    st.markdown("### Checkpoint 2: Null Date Entries")
    null_date_entries = D[D['Date'].isnull()]
    st.write(null_date_entries)
    if st.button("Run Anyway (Checkpoint 2)"):
        st.session_state["run_anyway_2"] = True

if st.session_state.get("run_anyway_2", False):
    # Checkpoint 3: Unique Dates Per Source
    st.markdown("### Checkpoint 3: Unique Dates Per Source")
    unique_dates_per_source = D.groupby('Source')['Date'].unique()
    st.write(unique_dates_per_source)
    if st.button("Run Anyway (Checkpoint 3)"):
        st.session_state["run_anyway_3"] = True

if st.session_state.get("run_anyway_3", False):
    # Checkpoint 4: ref.nunique()
    st.markdown("### Checkpoint 4: Reference Data Uniqueness")
    st.write(ref_d.nunique())
    if st.button("Run Anyway (Checkpoint 4)"):
        st.session_state["run_anyway_4"] = True

if st.session_state.get("run_anyway_4", False):
    # Checkpoint 5: Missing CRM IDs
    st.markdown("### Checkpoint 5: Missing CRM IDs")
    # Merge with reference
    ref = ref_d.copy()
    ref = ref[~ref['Email'].str.contains('inactive', case=False, na=False)]
    ref = ref.drop_duplicates(subset=['Dialer Name','Email'])
    ref.rename(columns={'Email': 'CRM ID'}, inplace=True)
    D['Dialer Name'] = D['Dialer Name'].astype(str).str.lower().str.replace(r"\s*\([^)]*\)|@.*|;.*", "", regex=True).str.strip()
    ref['Dialer Name'] = ref['Dialer Name'].astype(str).str.lower().str.replace(r"\s*\([^)]*\)|@.*|;.*", "", regex=True).str.strip()
    merged = D.merge(ref, how='left', left_on='Dialer Name', right_on='Dialer Name')
    crm_id_null_df = merged[merged['CRM ID'].isnull()]
    source = crm_id_null_df.groupby('Source')['Dialer Name'].unique()
    st.code(source.to_string(), language='text')
    if st.button("Run Anyway (Checkpoint 5)"):
        st.session_state["run_anyway_5"] = True

if st.session_state.get("run_anyway_5", False):
    # ========== FINAL CALCULATION & DOWNLOADS ==========
    st.markdown("### Final Reports & Downloads")
    # Example: summary = merged, D = D, crm_id_null_df = crm_id_null_df
    formatted_df = merged  # Replace with your actual summary logic if needed

    today_str = datetime.now().strftime("%b_%d")
    st.download_button(
        label=f"Download Summary (summary_{today_str}.csv)",
        data=formatted_df.to_csv(index=False).encode('utf-8'),
        file_name=f"summary_{today_str}.csv"
    )
    st.download_button(
        label=f"Download Dialer Raw (dialer_raw_{today_str}.csv)",
        data=D.to_csv(index=False).encode('utf-8'),
        file_name=f"dialer_raw_{today_str}.csv"
    )
    st.download_button(
        label=f"Download Not Found Users (not_found_users_{today_str}.csv)",
        data=crm_id_null_df.to_csv(index=False).encode('utf-8'),
        file_name=f"not_found_users_{today_str}.csv"
    )
