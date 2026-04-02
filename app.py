from flask import Flask, render_template, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
import csv
import io
import os
import re
import json
import base64
from datetime import datetime

app = Flask(__name__)

FINAL_STATUSES = ['Issue/Query Resolved', 'Ticket Created / Update', 'DNP 2']
INTERMEDIATE_STATUSES = ['DNP 1', 'Ask to Call Back']
ALL_STATUSES = INTERMEDIATE_STATUSES + FINAL_STATUSES

# Google Sheets config
CREDS_FILE = os.path.join(os.path.dirname(__file__), 'credentials.json')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SHEET_ID = '13_U2_C7GQUnSNzmYuG9XIaoX48tJ4BjbszVfZ8lXoqM'

# Sheet tab names
TAB_MASTER = 'CallBack_Master'
TAB_ATTEMPTS = 'Attempt_Log'
TAB_UPLOADS = 'Upload_History'

# Column headers
MASTER_HEADERS = [
    'ID', 'Upload_Date', 'Upload_Batch', 'Missed_Call_Date', 'Missed_Call_Time',
    'Phone', 'Queue_Name', 'Time_Slot', 'Call_ID', 'Current_Status',
    'Is_Final', 'Agent_Name', 'Last_Call_Date', 'Last_Call_Time',
    'Attempt_Count', 'Notes', 'Row_Active', 'Created_At'
]
ATTEMPT_HEADERS = [
    'ID', 'Callback_ID', 'Phone', 'Agent_Name', 'Call_Date',
    'Call_Time', 'Status', 'Notes', 'Created_At'
]
UPLOAD_HEADERS = [
    'ID', 'Upload_Batch', 'Upload_Date', 'Time_Slot', 'Queue_Name',
    'Row_Count', 'Uploaded_By', 'Source_Time_Start', 'Source_Time_End', 'Created_At'
]


def get_client():
    # Try env variable first (for Railway), then local file
    creds_b64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
    if creds_b64:
        creds_json = json.loads(base64.b64decode(creds_b64))
        creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet():
    client = get_client()
    return client.open_by_key(SHEET_ID)


def get_or_create_sheet(spreadsheet, tab_name, headers):
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
        ws.append_row(headers, value_input_option='RAW')
        ws.format('1', {'textFormat': {'bold': True}})
    return ws


def init_sheets():
    """Initialize the Google Sheet with required tabs and headers."""
    ss = get_spreadsheet()
    get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    get_or_create_sheet(ss, TAB_ATTEMPTS, ATTEMPT_HEADERS)
    get_or_create_sheet(ss, TAB_UPLOADS, UPLOAD_HEADERS)
    # Remove default Sheet1 if it exists
    try:
        default = ss.worksheet('Sheet1')
        ss.del_worksheet(default)
    except (gspread.WorksheetNotFound, gspread.exceptions.APIError):
        pass
    return ss


def sheet_to_dicts(ws):
    """Convert worksheet to list of dicts."""
    records = ws.get_all_records(default_blank='')
    return records


def next_id(ws):
    """Get next auto-increment ID."""
    all_vals = ws.col_values(1)  # ID column
    if len(all_vals) <= 1:
        return 1
    ids = [int(v) for v in all_vals[1:] if v.isdigit()]
    return max(ids) + 1 if ids else 1


# --------------- Pages ---------------

@app.route('/')
def index():
    return render_template('index.html')


# --------------- Dashboard Stats ---------------

@app.route('/api/stats')
def get_stats():
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    data = sheet_to_dicts(ws)
    today = datetime.now().strftime('%Y-%m-%d')

    total = len(data)
    pending_new = sum(1 for r in data if str(r.get('Row_Active', '')) == '1' and r.get('Current_Status', '') == '')
    intermediate = sum(1 for r in data if str(r.get('Row_Active', '')) == '1' and r.get('Current_Status', '') in INTERMEDIATE_STATUSES)
    final = sum(1 for r in data if str(r.get('Is_Final', '')) == '1')
    uploaded_today = sum(1 for r in data if r.get('Upload_Date', '') == today)

    status_counts = {}
    for s in ALL_STATUSES:
        status_counts[s] = sum(1 for r in data if r.get('Current_Status', '') == s)

    # Queue breakdown
    queue_map = {}
    for r in data:
        q = r.get('Queue_Name', '')
        if not q:
            continue
        if q not in queue_map:
            queue_map[q] = {'queue_name': q, 'cnt': 0, 'active_cnt': 0}
        queue_map[q]['cnt'] += 1
        if str(r.get('Row_Active', '')) == '1':
            queue_map[q]['active_cnt'] += 1
    queues = sorted(queue_map.values(), key=lambda x: -x['cnt'])

    # Agent activity today from attempts
    ws_att = get_or_create_sheet(ss, TAB_ATTEMPTS, ATTEMPT_HEADERS)
    att_data = sheet_to_dicts(ws_att)
    attempts_today = sum(1 for a in att_data if a.get('Call_Date', '') == today)
    agent_map = {}
    for a in att_data:
        if a.get('Call_Date', '') == today and a.get('Agent_Name', ''):
            agent_map[a['Agent_Name']] = agent_map.get(a['Agent_Name'], 0) + 1
    agents_today = [{'agent_name': k, 'total_attempts': v} for k, v in sorted(agent_map.items(), key=lambda x: -x[1])]

    return jsonify({
        'total': total,
        'active': sum(1 for r in data if str(r.get('Row_Active', '')) == '1'),
        'pending_new': pending_new,
        'intermediate': intermediate,
        'final': final,
        'uploaded_today': uploaded_today,
        'attempts_today': attempts_today,
        'status_counts': status_counts,
        'queues': queues,
        'agents_today': agents_today
    })


# --------------- Callback Master ---------------

@app.route('/api/callbacks')
def get_callbacks():
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    data = sheet_to_dicts(ws)

    status = request.args.get('status', '')
    queue = request.args.get('queue', '')
    search = request.args.get('search', '')
    active_only = request.args.get('active_only', '')

    filtered = []
    for r in data:
        r_status = r.get('Current_Status', '')
        r_active = str(r.get('Row_Active', ''))
        r_final = str(r.get('Is_Final', ''))

        if status:
            if status == 'pending' and r_status != '':
                continue
            elif status == 'intermediate' and r_status not in INTERMEDIATE_STATUSES:
                continue
            elif status == 'final' and r_final != '1':
                continue
            elif status not in ('pending', 'intermediate', 'final') and r_status != status:
                continue
        if queue and r.get('Queue_Name', '') != queue:
            continue
        if active_only == '1' and r_active != '1':
            continue
        if search:
            s = search.lower()
            searchable = (str(r.get('Phone', '')) + str(r.get('Queue_Name', '')) + str(r.get('Notes', '')) + str(r.get('Agent_Name', ''))).lower()
            if s not in searchable:
                continue

        # Normalize fields for frontend
        filtered.append({
            'id': int(r.get('ID', 0)),
            'upload_date': r.get('Upload_Date', ''),
            'upload_batch': r.get('Upload_Batch', ''),
            'missed_call_date': r.get('Missed_Call_Date', ''),
            'missed_call_time': r.get('Missed_Call_Time', ''),
            'phone': str(r.get('Phone', '')),
            'queue_name': r.get('Queue_Name', ''),
            'time_slot': r.get('Time_Slot', ''),
            'call_id': str(r.get('Call_ID', '')),
            'current_status': r_status,
            'is_final': int(r_final) if r_final.isdigit() else 0,
            'agent_name': r.get('Agent_Name', ''),
            'last_call_date': r.get('Last_Call_Date', ''),
            'last_call_time': r.get('Last_Call_Time', ''),
            'attempt_count': int(r.get('Attempt_Count', 0) or 0),
            'notes': r.get('Notes', ''),
            'row_active': int(r_active) if r_active.isdigit() else 0,
        })

    # Sort: active first, then by ID desc
    filtered.sort(key=lambda x: (-x['row_active'], -x['id']))
    return jsonify(filtered)


@app.route('/api/callbacks/<int:callback_id>')
def get_callback(callback_id):
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    data = sheet_to_dicts(ws)
    for r in data:
        if int(r.get('ID', 0)) == callback_id:
            return jsonify({
                'id': int(r['ID']),
                'phone': str(r.get('Phone', '')),
                'queue_name': r.get('Queue_Name', ''),
                'current_status': r.get('Current_Status', ''),
                'is_final': int(r.get('Is_Final', 0) or 0),
                'attempt_count': int(r.get('Attempt_Count', 0) or 0),
                'agent_name': r.get('Agent_Name', ''),
                'notes': r.get('Notes', ''),
                'row_active': int(r.get('Row_Active', 0) or 0),
            })
    return jsonify({'error': 'Not found'}), 404


# --------------- Agent: Update Status ---------------

@app.route('/api/callbacks/<int:callback_id>/update-status', methods=['POST'])
def update_status(callback_id):
    req = request.json
    new_status = req.get('status', '').strip()
    agent_name = req.get('agent_name', '').strip()
    notes = req.get('notes', '').strip()

    if not new_status or new_status not in ALL_STATUSES:
        return jsonify({'error': f'Invalid status. Must be one of: {ALL_STATUSES}'}), 400
    if not agent_name:
        return jsonify({'error': 'Agent name is required'}), 400

    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    data = ws.get_all_values()
    headers = data[0]

    # Find row by ID
    target_row = None
    for i, row in enumerate(data[1:], start=2):
        if row[0] == str(callback_id):
            target_row = i
            break

    if not target_row:
        return jsonify({'error': 'Callback not found'}), 404

    row_data = data[target_row - 1]
    is_final_col = headers.index('Is_Final')
    if row_data[is_final_col] == '1':
        return jsonify({'error': 'This callback already has a final status and cannot be updated'}), 400

    now = datetime.now()
    is_final = 1 if new_status in FINAL_STATUSES else 0
    row_active = 0 if is_final else 1
    attempt_col = headers.index('Attempt_Count')
    current_attempts = int(row_data[attempt_col] or 0)

    # Update cells in the master sheet
    updates = {
        'Current_Status': new_status,
        'Is_Final': str(is_final),
        'Row_Active': str(row_active),
        'Agent_Name': agent_name,
        'Last_Call_Date': now.strftime('%Y-%m-%d'),
        'Last_Call_Time': now.strftime('%H:%M:%S'),
        'Attempt_Count': str(current_attempts + 1),
        'Notes': notes,
    }
    for col_name, value in updates.items():
        col_idx = headers.index(col_name) + 1
        ws.update_cell(target_row, col_idx, value)

    # Log attempt
    ws_att = get_or_create_sheet(ss, TAB_ATTEMPTS, ATTEMPT_HEADERS)
    att_id = next_id(ws_att)
    ws_att.append_row([
        att_id, callback_id, str(row_data[headers.index('Phone')]),
        agent_name, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'),
        new_status, notes, now.strftime('%Y-%m-%d %H:%M:%S')
    ], value_input_option='RAW')

    return jsonify({'message': 'Status updated', 'is_final': bool(is_final)})


# --------------- Attempt Log ---------------

@app.route('/api/callbacks/<int:callback_id>/attempts')
def get_attempts(callback_id):
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_ATTEMPTS, ATTEMPT_HEADERS)
    data = sheet_to_dicts(ws)
    results = []
    for r in data:
        if str(r.get('Callback_ID', '')) == str(callback_id):
            results.append({
                'id': int(r.get('ID', 0)),
                'callback_id': int(r.get('Callback_ID', 0)),
                'phone': str(r.get('Phone', '')),
                'agent_name': r.get('Agent_Name', ''),
                'call_date': r.get('Call_Date', ''),
                'call_time': r.get('Call_Time', ''),
                'status': r.get('Status', ''),
                'notes': r.get('Notes', ''),
            })
    results.sort(key=lambda x: -x['id'])
    return jsonify(results)


@app.route('/api/attempts')
def get_all_attempts():
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_ATTEMPTS, ATTEMPT_HEADERS)
    data = sheet_to_dicts(ws)

    phone = request.args.get('phone', '')
    agent = request.args.get('agent', '')
    date = request.args.get('date', '')

    results = []
    for r in data:
        if phone and str(r.get('Phone', '')) != phone:
            continue
        if agent and r.get('Agent_Name', '') != agent:
            continue
        if date and r.get('Call_Date', '') != date:
            continue
        results.append({
            'id': int(r.get('ID', 0)),
            'phone': str(r.get('Phone', '')),
            'agent_name': r.get('Agent_Name', ''),
            'call_date': r.get('Call_Date', ''),
            'call_time': r.get('Call_Time', ''),
            'status': r.get('Status', ''),
            'notes': r.get('Notes', ''),
        })
    results.sort(key=lambda x: -x['id'])
    return jsonify(results[:500])


# --------------- TL: Upload CSV ---------------

def parse_time_slot(slot):
    slot = slot.strip().upper()
    m = re.match(r'(\d{1,2})\s*(?:AM|PM)?\s*[-\u2013]\s*(\d{1,2})\s*(AM|PM)', slot)
    if m:
        start_h = int(m.group(1))
        end_h = int(m.group(2))
        period = m.group(3)
        if period == 'PM' and end_h != 12:
            if end_h < 12:
                end_h += 12
            if start_h < 12 and start_h < end_h - 12:
                start_h += 12
        if period == 'AM' and start_h == 12:
            start_h = 0
        return start_h, end_h
    return None, None


@app.route('/api/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'error': 'Only CSV files allowed'}), 400

    time_slot = request.form.get('time_slot', '').strip()
    uploaded_by = request.form.get('uploaded_by', '').strip()
    queue_filter = request.form.get('queue_name', '').strip()
    if not time_slot:
        return jsonify({'error': 'Time slot is required'}), 400

    today = datetime.now().strftime('%Y-%m-%d')
    batch_id = f"{today}_{time_slot.replace(' ', '_')}"
    start_h, end_h = parse_time_slot(time_slot)

    stream = io.StringIO(file.stream.read().decode('utf-8-sig'))
    reader = csv.DictReader(stream)

    ss = get_spreadsheet()
    ws_master = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    ws_uploads = get_or_create_sheet(ss, TAB_UPLOADS, UPLOAD_HEADERS)

    master_data = sheet_to_dicts(ws_master)
    upload_data = sheet_to_dicts(ws_uploads)

    # Existing time ranges for today
    existing_ranges = []
    for u in upload_data:
        if u.get('Upload_Date', '') == today:
            try:
                existing_ranges.append((int(u['Source_Time_Start']), int(u['Source_Time_End'])))
            except (ValueError, TypeError, KeyError):
                pass

    # Final and active phones
    final_phones = set()
    active_phones = set()
    for r in master_data:
        phone = str(r.get('Phone', ''))
        if str(r.get('Is_Final', '')) == '1':
            final_phones.add(phone)
        if str(r.get('Row_Active', '')) == '1':
            active_phones.add(phone)

    # Read CSV
    all_rows = []
    answered_phones = set()
    for row in reader:
        phone = row.get('Phone', '').strip()
        answered = row.get('Answered/Hungup', '').strip().lower()
        if phone and answered in ('answered', 'yes', 'true', '1'):
            answered_phones.add(phone)
        all_rows.append(row)

    imported = 0
    skipped_final = 0
    skipped_duplicate = 0
    skipped_active = 0
    skipped_answered = 0
    skipped_other = 0
    current_id = next_id(ws_master)
    new_rows = []

    for row in all_rows:
        phone = row.get('Phone', '').strip()
        if not phone:
            skipped_other += 1
            continue
        answered = row.get('Answered/Hungup', '').strip().lower()
        if answered in ('answered', 'yes', 'true', '1'):
            skipped_answered += 1
            continue
        if phone in answered_phones:
            skipped_answered += 1
            continue
        row_queue = row.get('Queue Name', '').strip()
        if queue_filter and row_queue and row_queue != queue_filter:
            skipped_other += 1
            continue
        if phone in final_phones:
            skipped_final += 1
            continue
        if phone in active_phones:
            skipped_active += 1
            continue

        call_time_str = row.get('Call Time', '').strip()
        call_hour = None
        missed_date = ''
        missed_time = ''
        if call_time_str:
            for fmt in ('%d/%m/%Y %I:%M:%S %p', '%Y-%m-%d %H:%M:%S'):
                try:
                    dt = datetime.strptime(call_time_str, fmt)
                    call_hour = dt.hour
                    missed_date = dt.strftime('%Y-%m-%d')
                    missed_time = dt.strftime('%H:%M:%S')
                    break
                except ValueError:
                    pass

        if call_hour is not None and existing_ranges:
            in_existing = any(s <= call_hour < e for s, e in existing_ranges)
            if in_existing:
                skipped_duplicate += 1
                continue

        call_id = row.get('Call ID', row.get('CallID', '')).strip()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        new_rows.append([
            current_id, today, batch_id, missed_date, missed_time,
            phone, row_queue, time_slot, call_id, '', '0', '', '', '',
            '0', '', '1', now_str
        ])
        current_id += 1
        imported += 1
        active_phones.add(phone)

    # Batch append to sheet
    if new_rows:
        ws_master.append_rows(new_rows, value_input_option='RAW')

    # Log upload
    upload_id = next_id(ws_uploads)
    ws_uploads.append_row([
        upload_id, batch_id, today, time_slot, queue_filter, imported,
        uploaded_by, str(start_h) if start_h is not None else '',
        str(end_h) if end_h is not None else '',
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ], value_input_option='RAW')

    return jsonify({
        'imported': imported,
        'skipped_final': skipped_final,
        'skipped_duplicate': skipped_duplicate,
        'skipped_active': skipped_active,
        'skipped_answered': skipped_answered,
        'skipped_other': skipped_other,
        'batch_id': batch_id
    })


# --------------- Upload History ---------------

@app.route('/api/upload-history')
def get_upload_history():
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_UPLOADS, UPLOAD_HEADERS)
    data = sheet_to_dicts(ws)
    results = []
    for r in data:
        results.append({
            'id': int(r.get('ID', 0)),
            'upload_batch': r.get('Upload_Batch', ''),
            'upload_date': r.get('Upload_Date', ''),
            'time_slot': r.get('Time_Slot', ''),
            'queue_name': r.get('Queue_Name', ''),
            'row_count': int(r.get('Row_Count', 0) or 0),
            'uploaded_by': r.get('Uploaded_By', ''),
            'source_time_start': r.get('Source_Time_Start', ''),
            'source_time_end': r.get('Source_Time_End', ''),
        })
    results.sort(key=lambda x: -x['id'])
    return jsonify(results)


# --------------- Queues ---------------

@app.route('/api/queues')
def get_queues():
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    data = sheet_to_dicts(ws)
    queues = sorted(set(r.get('Queue_Name', '') for r in data if r.get('Queue_Name', '')))
    return jsonify(queues)


@app.route('/api/agents')
def get_agents():
    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    data = sheet_to_dicts(ws)
    agents = sorted(set(r.get('Agent_Name', '') for r in data if r.get('Agent_Name', '')))
    return jsonify(agents)


# --------------- TL: Manual Add Row ---------------

@app.route('/api/manual-add', methods=['POST'])
def manual_add():
    data = request.json
    phone = data.get('phone', '').strip()
    queue_name = data.get('queue_name', '').strip()
    missed_call_date = data.get('missed_call_date', '').strip()
    missed_call_time = data.get('missed_call_time', '').strip()
    time_slot = data.get('time_slot', '').strip()

    if not phone:
        return jsonify({'error': 'Phone number is required'}), 400
    if not queue_name:
        return jsonify({'error': 'Queue name is required'}), 400

    ss = get_spreadsheet()
    ws = get_or_create_sheet(ss, TAB_MASTER, MASTER_HEADERS)
    master_data = sheet_to_dicts(ws)

    for r in master_data:
        if str(r.get('Phone', '')) == phone:
            if str(r.get('Is_Final', '')) == '1':
                return jsonify({'error': 'This number already has a final status (closed). Cannot add again.'}), 400
            if str(r.get('Row_Active', '')) == '1':
                return jsonify({'error': 'This number is already active in the queue.'}), 400

    today = datetime.now().strftime('%Y-%m-%d')
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_id = next_id(ws)

    ws.append_row([
        new_id, today, f"{today}_MANUAL", missed_call_date, missed_call_time,
        phone, queue_name, time_slot, '', '', '0', '', '', '',
        '0', '', '1', now_str
    ], value_input_option='RAW')

    return jsonify({'message': 'Row added successfully'}), 201


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
