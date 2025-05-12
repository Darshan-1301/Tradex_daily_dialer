import streamlit as st
import pandas as pd
import re
from datetime import datetime, timedelta

st.set_page_config(page_title="Tradex Dialer Data ETL App", layout="wide")
st.title("Tradex Dialer Data ETL App")

# ---- Helper for file upload with NA option ----
def file_upload_with_na(label, key):
    col1, col2 = st.columns([3, 1])
    with col1:
        file = st.file_uploader(f"{label} (CSV)", type="csv", key=key)
    with col2:
        na = st.checkbox("NA", key=f"na_{key}")
    return file, na

# ---- File uploaders for each dialer ----
voiso_file, voiso_na = file_upload_with_na("Voiso", "voiso")
tata_file, tata_na = file_upload_with_na("Tata", "tata")
know_file, know_na = file_upload_with_na("Knowlarity", "know")
qconn_file, qconn_na = file_upload_with_na("Qkonnect", "qconn")
stringee_file, stringee_na = file_upload_with_na("Stringee", "stringee")
team_tradex_file = st.file_uploader("Team_tradex (MANDATORY)", type="csv", key="team_tradex")

# ---- Initialize checkpoint states ----
for cp in ["cp1", "cp2", "cp3", "cp4"]:
    if cp not in st.session_state:
        st.session_state[cp] = False

# ---- Checkpoint 1: Show Date Samples ----
if st.button("Checkpoint 1: Show Date Samples") or st.session_state.cp1:
    st.session_state.cp1 = True
    dfs = {}
    if voiso_file and not voiso_na:
        dfs['voiso'] = pd.read_csv(voiso_file, low_memory=False)
    if tata_file and not tata_na:
        dfs['tata'] = pd.read_csv(tata_file, low_memory=False)
    if know_file and not know_na:
        dfs['know'] = pd.read_csv(know_file, low_memory=False)
    if qconn_file and not qconn_na:
        dfs['Qconn'] = pd.read_csv(qconn_file, low_memory=False)
    if stringee_file and not stringee_na:
        dfs['stringee'] = pd.read_csv(stringee_file, low_memory=False)
    st.markdown("#### Checkpoint 1: Date Columns (first 5 unique values)")
    if 'voiso' in dfs:
        st.write("voiso:", dfs['voiso']['Date and time'].unique()[:5])
    if 'tata' in dfs:
        st.write("tata:", dfs['tata']['Call Start Date'].unique()[:5])
    if 'know' in dfs:
        st.write("know:", dfs['know']['Date and Time'].unique()[:5])
    if 'Qconn' in dfs:
        st.write("Qconn:", dfs['Qconn']['Date time'].unique()[:5])
    if 'stringee' in dfs:
        st.write("stringee:", dfs['stringee']['Start time'].unique()[:5])
    st.session_state.dfs = dfs
    if st.button("Run Anyway (Checkpoint 1)"):
        st.session_state.cp2 = True

# ---- Checkpoint 2: ETL and Date Normalization ----
if st.session_state.cp2:
    st.markdown("#### Checkpoint 2: ETL and Date Normalization")
    dfs = st.session_state.dfs
    # Normalize Stringee durations
    if 'stringee' in dfs:
        def duration_to_timedelta(duration):
            try:
                hours, minutes, seconds = map(int, str(duration).split(':'))
                return timedelta(hours=hours, minutes=minutes, seconds=seconds)
            except:
                return timedelta(0)
        stringee = dfs['stringee']
        stringee['Queue Duration (timedelta)'] = stringee['Queue duration'].apply(duration_to_timedelta)
        stringee['Answer Duration (timedelta)'] = stringee['Answer duration'].apply(duration_to_timedelta)
        stringee['Total Duration (timedelta)'] = stringee['Queue Duration (timedelta)'] + stringee['Answer Duration (timedelta)']
        stringee['Total Duration'] = stringee['Total Duration (timedelta)'].apply(lambda x: str(x).split(", ")[-1])
        stringee = stringee.drop(columns=['Queue Duration (timedelta)', 'Answer Duration (timedelta)', 'Total Duration (timedelta)'])
        dfs['stringee'] = stringee
    # Voiso date fix
    if 'voiso' in dfs:
        voiso = dfs['voiso']
        voiso['Date and time'] = pd.to_datetime(voiso['Date and time'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
        voiso['Date and time'] = voiso['Date and time'].dt.strftime('%Y/%m/%d %H:%M:%S')
        dfs['voiso'] = voiso
    # Stringee date fix
    if 'stringee' in dfs:
        def fix_string_datetime(date_str):
            try:
                parsed_date = pd.to_datetime(date_str, errors='coerce', format='%m/%d/%Y %I:%M:%S %p')
                return parsed_date
            except Exception:
                return pd.NaT
        stringee = dfs['stringee']
        stringee['Start time'] = stringee['Start time'].apply(fix_string_datetime)
        stringee['Start time'] = stringee['Start time'].dt.strftime('%Y/%m/%d %H:%M:%S')
        dfs['stringee'] = stringee
    # Qconn/Know date fix
    if 'Qconn' in dfs:
        Qconn = dfs['Qconn']
        Qconn['Date time'] = pd.to_datetime(Qconn['Date time'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        dfs['Qconn'] = Qconn
    if 'know' in dfs:
        know = dfs['know']
        know['Date and Time'] = pd.to_datetime(know['Date and Time'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        dfs['know'] = know
    st.session_state.dfs = dfs
    st.write("DataFrames after ETL/Date Normalization:")
    for k, v in dfs.items():
        st.write(f"{k} sample:", v.head())
    if st.button("Run Anyway (Checkpoint 2)"):
        st.session_state.cp3 = True

# ---- Checkpoint 3: DataFrame Construction ----
if st.session_state.cp3:
    st.markdown("#### Checkpoint 3: DataFrame Construction and Merge")
    dfs = st.session_state.dfs
    # Prepare DataFrames for merging
    selected_dfs = []
    if 'tata' in dfs:
        tata = dfs['tata']
        tata_copy = tata.rename(columns={'Call Start Date': 'Date', 'Connected to Agent': 'Dialer Name',
                                         'Customer Number': 'Number', 'Answer Duration (HH:MM:SS)': 'Talk Time',
                                         'Hold Duration (HH:MM:SS)': 'Hold Time', 
                                         'Total Call Duration (HH:MM:SS)': 'Total Call Duration',
                                         'Call Start Time': 'Call Start Time'})
        tata_copy['Source'] = 'Tata'
        tata_selected = tata_copy[['Source','Date','Dialer Name','Number','Call Status','Call Start Time','Total Call Duration','Talk Time','Hold Time']]
        selected_dfs.append(tata_selected)
    if 'know' in dfs:
        know = dfs['know']
        know_copy = know.rename(columns={'Date and Time': 'Date', 'Agent Name': 'Dialer Name', 'Customer': 'Number',
                                         'Talk Time (hh:mm:ss)': 'Talk Time', 'Hold Time (hh:mm:ss)': 'Hold Time',
                                         'Total Call Duration (hh:mm:ss)': 'Total Call Duration',
                                         'Call Start Time': 'Call Start Time'})
        know_copy['Source'] = 'Knowlarity'
        know_selected = know_copy[['Source','Date','Dialer Name','Number','Call Status','Call Start Time','Total Call Duration','Talk Time','Hold Time']]
        selected_dfs.append(know_selected)
    if 'voiso' in dfs:
        voiso = dfs['voiso']
        voiso_copy = voiso.rename(columns={'Date and time': 'Date', 'Agent(s)': 'Dialer Name', 'DNIS/To': 'Number',
                                           'Disposition': 'Call Status', 'Talk time': 'Talk Time', 'Duration': 'Total Call Duration',
                                           'Call Start Time': 'Call Start Time'})
        voiso_copy['Source'] = 'Voiso'
        voiso_selected = voiso_copy[['Source','Date','Dialer Name','Number','Call Status','Call Start Time','Total Call Duration','Talk Time']]
        selected_dfs.append(voiso_selected)
    if 'Qconn' in dfs:
        Qconn = dfs['Qconn']
        qconn_copy = Qconn.rename(columns={'Date time': 'Date', 'Agent Mobile': 'Dialer Name', 'User Mobile': 'Number',
                                           'Call Event': 'Call Status', 'Transfer Duration': 'Talk Time', 'Duration': 'Total Call Duration',
                                           'Call Start Time': 'Call Start Time'})
        qconn_copy['Source'] = 'Qkonnect'
        qconn_selected = qconn_copy[['Source','Date','Dialer Name','Number','Call Status','Call Start Time','Total Call Duration','Talk Time']]
        selected_dfs.append(qconn_selected)
    if 'stringee' in dfs:
        stringee = dfs['stringee']
        stringee_copy = stringee.rename(columns={'Start time': 'Date', 'Account': 'Dialer Name', 'Customer number': 'Number',
                                                 'Call status': 'Call Status', 'Answer duration': 'Talk Time', 'Hold duration': 'Hold Time',
                                                 'Total Duration': 'Total Call Duration', 'Call Start Time': 'Call Start Time'})
        stringee_copy['Source'] = 'Stringee'
        stringee_selected = stringee_copy[['Source','Date','Dialer Name','Number','Call Status','Call Start Time','Total Call Duration','Talk Time','Hold Time']]
        selected_dfs.append(stringee_selected)
    if not selected_dfs:
        st.error("No dialer files selected or uploaded. Please upload at least one dialer file or uncheck NA.")
        st.stop()
    D = pd.concat(selected_dfs, ignore_index=True)
    st.session_state.D = D
    st.write("Combined DataFrame (D):", D.head())
    if st.button("Run Anyway (Checkpoint 3)"):
        st.session_state.cp4 = True

# ---- Checkpoint 4: Full Calculation, Gap, Attendance, Summary, Downloads ----
if st.session_state.cp4:
    st.markdown("#### Checkpoint 4: Full Calculation, Gap, Attendance, Summary, Downloads")
    D = st.session_state.D
    ref_d = pd.read_csv(team_tradex_file, low_memory=False)
    # Clean and merge with reference
    ref = ref_d.copy()
    ref = ref[~ref['Email'].str.contains('inactive', case=False, na=False)]
    ref = ref.drop_duplicates(subset=['Dialer Name','Email'])
    ref.rename(columns={'Email': 'CRM ID'}, inplace=True)
    D['Dialer Name'] = D['Dialer Name'].astype(str).str.lower().str.replace(r"\s*\([^)]*\)|@.*|;.*", "", regex=True).str.strip()
    ref['Dialer Name'] = ref['Dialer Name'].astype(str).str.lower().str.replace(r"\s*\([^)]*\)|@.*|;.*", "", regex=True).str.strip()
    merged = D.merge(ref, how='left', left_on='Dialer Name', right_on='Dialer Name')
    crm_id_null_df = merged[merged['CRM ID'].isnull()]
    # GAP and Attendance Logic
    Dialers = merged[merged['CRM ID'].notnull() & merged['Talk Time'].notnull()].copy()
    Dialers['Date'] = pd.to_datetime(Dialers['Date'], errors='coerce')
    Dialers['Call Start Time'] = pd.to_datetime(
        Dialers['Date'].dt.strftime('%Y-%m-%d').fillna('1900-01-01') + ' ' + Dialers['Call Start Time'],
        format='%Y-%m-%d %H:%M:%S', errors='coerce'
    )
    Dialers['Total Call Duration'] = Dialers['Total Call Duration'].apply(
        lambda x: pd.to_timedelta(x) if isinstance(x, str) else pd.Timedelta(0)
    )
    Dialers = Dialers.sort_values(by=['Date', 'CRM ID', 'Call Start Time']).reset_index(drop=True)
    Dialers['Call Gap'] = 'No'
    Dialers['Gap Duration'] = '00:00:00'
    start_time = pd.to_datetime('09:30:00').time()
    end_time = pd.to_datetime('18:30:00').time()
    for i in range(1, len(Dialers)):
        same_crm = Dialers.loc[i, 'CRM ID'] == Dialers.loc[i - 1, 'CRM ID']
        same_date = Dialers.loc[i, 'Date'] == Dialers.loc[i - 1, 'Date']
        if same_crm and same_date:
            current_time = Dialers.loc[i, 'Call Start Time'].time()
            previous_time = Dialers.loc[i - 1, 'Call Start Time'].time()
            previous_end = Dialers.loc[i - 1, 'Call Start Time'] + Dialers.loc[i - 1, 'Total Call Duration']
            gap_duration = Dialers.loc[i, 'Call Start Time'] - previous_end
            if gap_duration.total_seconds() < 0:
                gap_duration = timedelta(0)
            total_seconds = int(gap_duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            Dialers.loc[i, 'Gap Duration'] = f"{hours:02}:{minutes:02}:{seconds:02}"
            if start_time <= current_time <= end_time and start_time <= previous_time <= end_time:
                Dialers.loc[i, 'Call Gap'] = 'Yes' if gap_duration > timedelta(minutes=1) else 'No'
    invalid_values = []
    def to_seconds(value):
        try:
            if isinstance(value, pd.Timedelta):
                return int(value.total_seconds())
            elif re.match(r"^\d{1,2}:\d{1,2}:\d{1,2}$", str(value)):
                parts = list(map(int, value.split(':')))
                while len(parts) < 3:
                    parts.insert(0, 0)
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
            elif str(value).isdigit():
                return int(value)
            else:
                invalid_values.append(value)
                return value
        except Exception:
            invalid_values.append(value)
            return value
    for col in ['Talk Time', 'Hold Time', 'Total Call Duration', 'Gap Duration']:
        if col not in Dialers.columns:
            Dialers[col] = ''
        else:
            Dialers[col] = Dialers[col].fillna('')
    Dialers['Talk Time (seconds)'] = Dialers['Talk Time'].apply(to_seconds)
    Dialers['Hold Time (seconds)'] = Dialers['Hold Time'].apply(to_seconds)
    Dialers['Total Duration (seconds)'] = Dialers['Total Call Duration'].apply(to_seconds)
    Dialers['Gap Duration (seconds)'] = Dialers['Gap Duration'].apply(to_seconds)
    A = Dialers.groupby(['CRM ID', 'Date']).agg(
        Total_Dialed_Calls=('Call Status', 'count'),
        Unique_Dialed_Numbers=('Number', 'nunique'),
        Total_Connected_Calls=('Call Status', lambda x: (x == 'connected').sum()),
        Total_Number_of_Call_Gap=('Call Gap', lambda x: (x == 'Yes').sum()),
        Total_Call_GT_30=('Talk Time (seconds)', lambda x: ((Dialers.loc[x.index, 'Call Status'] == 'connected') & (x > 30)).sum()),
        Total_Duration=('Total Duration (seconds)', 'sum'),
        Total_Talk_Time=('Talk Time (seconds)', lambda x: x[Dialers.loc[x.index, 'Call Status'] == 'connected'].sum()),
        Total_Talk_Time_GT_30=('Talk Time (seconds)', lambda x: x[(Dialers.loc[x.index, 'Call Status'] == 'connected') & (x > 30)].sum()),
        Total_Connected_Hold_Time=('Hold Time (seconds)', lambda x: x[Dialers.loc[x.index, 'Call Status'] == 'connected'].sum()),
        Total_Gap_Duration=('Gap Duration (seconds)', 'sum')
    ).reset_index()
    A['Total_Gap_Duration'] = A['Total_Gap_Duration'].apply(lambda x: x - 3600 if x > 3600 else x)
    A['Avg_Gap_per_call'] = A['Total_Gap_Duration'] / A['Total_Dialed_Calls']
    A['Gap Duration After Leverage'] = (A['Total_Gap_Duration'] - (A['Total_Dialed_Calls'] * 45)).clip(lower=0)
    A['Login Hours'] = A['Total_Duration'] + A['Total_Gap_Duration'] + 3600
    A['Login Hours'] = pd.to_timedelta(A['Login Hours'], unit='s')
    A['Login Hours'] = A['Login Hours'].apply(lambda x: str(x).split()[-1])
    A['Login Hours (Timedelta)'] = pd.to_timedelta(A['Login Hours'])
    def mark_attendance(td):
        if td < pd.Timedelta(hours=4, minutes=30):
            return 'Absent'
        elif td < pd.Timedelta(hours=6):
            return 'Half Day'
        elif td < pd.Timedelta(hours=8, minutes=30):
            return 'Warning'
        else:
            return 'Present'
    A['Attendance'] = A['Login Hours (Timedelta)'].apply(mark_attendance)
    A.drop(columns='Login Hours (Timedelta)', inplace=True)
    unique_crm_ref = ref.drop_duplicates(subset=['CRM ID'])[['CRM ID','Employee code', 'Full Name','Pool', 'TL','Vertical']]
    all_dates = A['Date'].unique()
    date_crm_combinations = pd.MultiIndex.from_product(
        [unique_crm_ref['CRM ID'], all_dates],
        names=['CRM ID', 'Date']
    ).to_frame(index=False)
    merged = date_crm_combinations.merge(
        A,
        how='left',
        on=['CRM ID', 'Date']
    ).fillna(0)
    merged_df = unique_crm_ref.merge(
        merged,
        how='outer',
        on='CRM ID'
    )
    columns_to_format = ['Total_Dialed_Calls', 'Unique_Dialed_Numbers','Total_Connected_Calls','Total_Number_of_Call_Gap', 'Total_Call_GT_30','Total_Duration','Total_Talk_Time','Total_Talk_Time_GT_30','Total_Connected_Hold_Time','Total_Gap_Duration']
    merged_df[columns_to_format] = merged_df[columns_to_format].fillna(0)
    merged_df[columns_to_format] = merged_df[columns_to_format].astype(int)
    formatted_df = merged_df[['Date', 'Pool', 'TL', 'CRM ID','Employee code', 'Full Name', 'Vertical'] + [col for col in merged_df.columns if col not in ['Date', 'Pool', 'TL', 'CRM ID','Employee code', 'Full Name', 'Vertical']]]
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
    st.success("All calculations done! Download your results above.")

