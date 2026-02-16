import logging
from flask import Flask, request, jsonify, session, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
from oauthlib.oauth2 import WebApplicationClient
import google.auth.transport.requests
import google.oauth2.credentials
import googleapiclient.discovery
import requests
import os
import json
import logging
import pandas as pd
from urllib.parse import unquote
from qa_module import handle_question, generate_unique_id
import base64
import urllib.parse
from db_manager import DBManager
from file_engine import file_engine
from db_manager import DBManager
DBManager.init_db()
import math
from semantic_matcher import SemanticMatcher
from ga4_metadata import GA4_METRICS, GA4_DIMENSIONS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
semantic = SemanticMatcher(os.path.join(BASE_DIR, "vectorizer.pkl"))
semantic.build_metric_index(GA4_METRICS)
semantic.build_dimension_index(GA4_DIMENSIONS)

def sanitize(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj

app = Flask(__name__)
app.secret_key = os.urandom(24)
UPLOAD_FOLDER = 'uploaded_files'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# 통합 데이터셋 저장용 변수
integrated_datasets = {}

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    session['uploaded_file_path'] = filepath  # 세션에 저장

    return jsonify({"message": "File uploaded successfully", "file_path": filepath})



os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

with open("client_secret.json", "r") as f:
    google_creds = json.loads(f.read())

GOOGLE_CLIENT_ID = google_creds["web"]["client_id"]
GOOGLE_CLIENT_SECRET = google_creds["web"]["client_secret"]
GOOGLE_DISCOVERY_URL = "https://accounts.google.com/.well-known/openid-configuration"

client = WebApplicationClient(GOOGLE_CLIENT_ID)

def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()

def fetch_accounts(credentials):
    analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
    accounts = analytics.accounts().list().execute()
    return accounts

def fetch_properties(analytics, account_id):
    properties = analytics.properties().list(filter=f'parent:accounts/{account_id}').execute()
    return properties

@app.route("/")
def index():
    if 'credentials' not in session:
        app.logger.info("No credentials in session. Redirecting to login.")
        return redirect(url_for('login'))
    app.logger.info("Credentials found in session. Serving index page.")
    return send_from_directory('static', 'index.html')

@app.route("/login")
def login():
    google_provider_cfg = get_google_provider_cfg()
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]

    request_uri = client.prepare_request_uri(
        authorization_endpoint,
        redirect_uri=url_for('callback', _external=True),
        scope=["openid", "email", "profile", "https://www.googleapis.com/auth/analytics.readonly"],
    )
    app.logger.info(f"Redirecting to: {request_uri}")
    return redirect(request_uri)

@app.route("/oauth2callback")
def callback():
    code = request.args.get("code")
    google_provider_cfg = get_google_provider_cfg()
    token_endpoint = google_provider_cfg["token_endpoint"]

    token_url, headers, body = client.prepare_token_request(
        token_endpoint,
        authorization_response=request.url,
        redirect_url=url_for('callback', _external=True),
        code=code
    )
    app.logger.info(f"Token URL: {token_url}")
    app.logger.info(f"Headers: {headers}")
    app.logger.info(f"Body: {body}")

    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET),
    )
    token_response_data = token_response.json()
    
    if 'error' in token_response_data:
        app.logger.error(f"Error in token response: {token_response_data['error']}")
        return jsonify({"error": "Authentication failed"}), 400

    # 토큰 정보를 클라이언트에 파싱 (add_token 호출 전 필수)
    client.parse_request_body_response(json.dumps(token_response_data))

    # 유저 정보 가져오기 (Email을 user_id로 사용)
    userinfo_endpoint = google_provider_cfg["userinfo_endpoint"]
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo_response = requests.get(uri, headers=headers, data=body)
    user_data = userinfo_response.json()
    
    session['user_id'] = user_data["email"]
    session['user_name'] = user_data.get("name", "User")

    session['credentials'] = {
        'token': token_response_data['access_token'],
        'refresh_token': token_response_data.get('refresh_token'),
        'token_uri': token_endpoint,
        'client_id': GOOGLE_CLIENT_ID,
        'client_secret': GOOGLE_CLIENT_SECRET,
        'scopes': ['openid', 'email', 'profile', 'https://www.googleapis.com/auth/analytics.readonly']
    }
    
    # 첫 접속 시 conversation_id 생성
    if 'conversation_id' not in session:
        import uuid
        session['conversation_id'] = str(uuid.uuid4())
        # DB에 초기 레코드 생성
        from db_manager import DBManager
        DBManager.save_conversation_record(
            session['conversation_id'], 
            session['user_id'], 
            session.get("property_id"), 
            session.get("preprocessed_data_path") or session.get("uploaded_file_path")
        )

    app.logger.info(f"User {session['user_id']} logged in. Session: {session['conversation_id']}")
    return redirect(url_for("index"))

def ensure_new_conversation():
    """데이터 구성이 바뀌면 새 대화 세션 발급"""
    import uuid
    from db_manager import DBManager
    old_id = session.get('conversation_id')
    new_id = str(uuid.uuid4())
    session['conversation_id'] = new_id
    
    property_id = session.get("property_id")
    file_path = session.get("preprocessed_data_path") or session.get("uploaded_file_path")
    
    DBManager.save_conversation_record(new_id, session.get('user_id'), property_id, file_path)
    app.logger.info(f"Conversation rotated: {old_id} -> {new_id}")

def refresh_credentials(credentials):
    request_ = google.auth.transport.requests.Request()
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(request_)
    return credentials

@app.route("/list_all")
def list_all():
    try:
        if 'credentials' not in session:
            app.logger.error("No credentials in session. Redirecting to login.")
            return redirect(url_for('login'))

        credentials = google.oauth2.credentials.Credentials(**session['credentials'])
        credentials = refresh_credentials(credentials)

        analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
        accounts = fetch_accounts(credentials)
        account_data = [
            {'id': account['name'].split('/')[1], 'name': account['displayName']}
            for account in accounts.get('accounts', [])
        ]

        return jsonify(account_data)
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return jsonify({"error": f"Failed to fetch data: {e}"}), 500

@app.route("/list_properties")
def list_properties():
    try:
        if 'credentials' not in session:
            app.logger.error("No credentials in session. Redirecting to login.")
            return redirect(url_for('login'))

        account_id = request.args.get('accountId')
        if not account_id:
            return jsonify({"error": "Account ID is required"}), 400

        credentials = google.oauth2.credentials.Credentials(**session['credentials'])
        credentials = refresh_credentials(credentials)

        analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
        properties = fetch_properties(analytics, account_id)
        property_data = [
            {'id': prop['name'].split('/')[1], 'name': prop['displayName']}
            for prop in properties.get('properties', [])
        ]

        return jsonify(property_data)
    except Exception as e:
        logging.error(f"Error fetching properties: {e}")
        return jsonify({"error": f"Failed to fetch properties: {e}"}), 500

@app.route("/set_property", methods=["POST"])
def set_property():
    data = request.get_json()

    property_id = data.get("property_id")
    property_name = data.get("property_name")

    if not property_id or not property_name:
        return jsonify({"error": "Property ID and name are required"}), 400

    old_prop = session.get('property_id')
    session['property_id'] = property_id
    session['property_name'] = property_name
    session["active_source"] = "ga4"
    
    if old_prop != property_id:
        ensure_new_conversation()

    # ✅ DB에 컨텍스트 저장
    DBManager.save_conversation_context(session.get('conversation_id'), {
        "active_source": "ga4",
        "property_id": property_id,
        "file_path": session.get("preprocessed_data_path") or session.get("uploaded_file_path")
    })

    return jsonify({
        "success": True, 
        "property_id": property_id, 
        "conversation_id": session.get('conversation_id')
    })


@app.route("/logout")
def logout():
    session.clear()
    app.logger.info("Session cleared. Redirecting to index.")
    return redirect(url_for("index"))
@app.route('/autocomplete')
def autocomplete():
    query = request.args.get('query')
    suggestions = get_suggestions(query)
    return jsonify(suggestions)

def get_suggestions(query):
    all_questions = [
        "총 사용자 수가 얼마나 되나요?",
        "활성 사용자는 얼마인가요?",
        "사용자는 얼마나 들어오나요?",
        "페이지뷰가 얼마나 되나요?",
        "조회수는 얼마나 되나요?",
        "평균 세션시간은 어떻게 되나요?",
        "이탈률이 어떻게 되나요?",
        "가장 인기 있는 페이지는 무엇인가요?",
        "신규 사용자가 몇 명인가요?",
        "가장 많은 트래픽을 보내는 소스는 무엇인가요?",
        "디바이스별 사용자 수는 얼마나 되나요?"
    ]
    return [q for q in all_questions if query.lower() in q.lower()]

#GET요청용 API엔드포인트이다. 브라우저나 프론트에서 GET요청을 보내면 이 함수가 실행된다.
@app.route("/traffic")
def traffic():
    question = request.args.get("question")
    property_id = request.args.get("propertyId")
    
    if not question:
        return jsonify({"error": "Question is required"}), 400
    if not property_id:
        return jsonify({"error": "Property ID is required"}), 400

    try:
        logging.info(f"[Traffic] Question: {question}, Prop: {property_id}")
        response = handle_question(
            question,
            property_id=property_id,
            conversation_id=session.get("conversation_id"),
            user_id=session.get("user_id", "anonymous"),
            semantic=semantic
        )


        # [P0] Sanitize
        return jsonify(sanitize(response))
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return jsonify({"error": f"Failed to process traffic request: {e}"}), 500

@app.route("/visualize")
def visualize():
    try:
        graph_type = request.args.get("type")
        logging.info(f"[Visualize] Request Type: {graph_type}")

        if not graph_type:
            return jsonify({"error": "Graph type is required"}), 400

        last_response = session.get('last_response')
        if not last_response:
            logging.warning("[Visualize] No last_response in session")
            return jsonify({"error": "No data available for visualization"}), 400

        # [Fix] Handle nested 'response' key from handle_question structure
        if isinstance(last_response, dict) and 'response' in last_response:
            data = last_response['response'].get('plot_data')
        else:
            data = last_response.get('plot_data')

        # [P0] Allow dict or list & Sanitize
        if not isinstance(data, (list, dict)):
            logging.error(f"[Visualize] Invalid plot_data type: {type(data)}")
            return jsonify({"error": "Plot data is not a list or dict"}), 400
        if 'response' in last_response:
            if last_response['response'].get("status") == "clarify":
                return jsonify({"error": "Clarify 상태에서는 시각화할 수 없습니다."}), 400

        # [Fix] Sanitize before sanitize
        data = sanitize(data)
        
        # Ensure list wrapping if dict (for client compatibility if needed, safely)
        # If client expects list, wrap it. ApexCharts often handles both but list is safer for series.
        # But 'data' here might be the full config object or just series?
        # Usually it's the full config {type:..., labels:..., series:...}
        # If it's a dict, sanitize is done.
        
        logging.debug(f"[Visualize] Data: {str(data)[:200]}...") # Log summary
        plot_data = base64.b64encode(json.dumps(data).encode()).decode()
        return jsonify({"plot_data": plot_data})
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return jsonify({"error": f"Failed to visualize: {e}"}), 500



@app.route("/upload_data", methods=["POST"])
def upload_data():
    file = request.files['file']
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    df = pd.read_csv(file)
    session['uploaded_data'] = df.to_dict(orient='records')
    return jsonify({"success": True, "data": df.head().to_dict(orient='records')})
@app.route('/preprocess_data_preview', methods=['POST'])
def preprocess_data_preview():
    actions = request.json.get('actions', [])
    file_path = session.get('uploaded_file_path')

    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "No file uploaded or file not found"}), 400

    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return jsonify({"error": "Unsupported file type"}), 400

        for action in actions:
            if action['type'] == 'drop_column':
                if 'column' in action and action['column']:
                    df.drop(columns=[action['column']], inplace=True)
                else:
                    return jsonify({"error": "Column name for drop_column cannot be empty"}), 400
            elif action['type'] == 'rename_column':
                if 'old_name' in action and 'new_name' in action:
                    df.rename(columns={action['old_name']: action['new_name']}, inplace=True)
                else:
                    return jsonify({"error": "'old_name' and 'new_name' are required for renaming columns"}), 400
            elif action['type'] == 'filter_rows':
                df = df[df[action['column']].astype(str).str.contains(action['value'], na=False)]

        df.fillna('', inplace=True)
        columns = df.columns.tolist()
        data = df.to_dict(orient='records')

        return jsonify({'columns': columns, 'data': data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/preprocess_data', methods=['POST'])
def preprocess_data():
    actions = request.json.get('actions', [])
    file_path = session.get('uploaded_file_path')

    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "No file uploaded or file not found"}), 400

    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return jsonify({"error": "Unsupported file type"}), 400

        for action in actions:
            if action['type'] == 'drop_column':
                df.drop(columns=[action['column']], inplace=True)
            elif action['type'] == 'rename_column':
                if 'old_name' in action and 'new_name' in action:
                    df.rename(columns={action['old_name']: action['new_name']}, inplace=True)
                else:
                    return jsonify({"error": "'old_name' and 'new_name' are required for renaming columns"}), 400
            elif action['type'] == 'filter_rows':
                df = df[df[action['column']].astype(str).str.contains(action['value'], na=False)]

        df.fillna('', inplace=True)
        columns = df.columns.tolist()
        data = df.to_dict(orient='records')

        # 전처리된 데이터를 파일에 저장하고 파일 경로를 세션에 저장
        preprocessed_file_path = os.path.join(UPLOAD_FOLDER, f'preprocessed_{generate_unique_id()}.csv')
        df.to_csv(preprocessed_file_path, index=False)
        # [Fix] Key Consistency: Use 'preprocessed_data_path'
        session['preprocessed_data_path'] = preprocessed_file_path
        logging.info(f"[Preprocess] Saved to {preprocessed_file_path}, Session Key: preprocessed_data_path")

        return jsonify({'columns': columns, 'data': data, 'file_path': preprocessed_file_path})
    except Exception as e:
        logging.error(f"Error during preprocessing: {e}")
        return jsonify({"error": str(e)}), 500



@app.route('/save_preprocessed_data', methods=['POST'])
def save_preprocessed_data():
    dataset_name = request.json.get('dataset_name')
    data = request.json.get('data')

    if not dataset_name:
        return jsonify({"error": "Dataset name is required"}), 400

    if not data:
        return jsonify({"error": "No data to save"}), 400

    preprocessed_file_path = os.path.join(UPLOAD_FOLDER, f'{dataset_name}.csv')

    try:
        df = pd.DataFrame(data)
        df.to_csv(preprocessed_file_path, index=False)
        return jsonify({"message": "Dataset saved successfully", "dataset_name": dataset_name})
    except Exception as e:
        logging.error(f"Error saving preprocessed data: {e}")
        return jsonify({"error": str(e)}), 500



@app.route('/list_datasets', methods=['GET'])
def list_datasets():
    try:
        datasets = []

        # GA4 계정 목록 추가
        if 'credentials' in session:
            credentials = google.oauth2.credentials.Credentials(**session['credentials'])
            credentials = refresh_credentials(credentials)

            analytics = googleapiclient.discovery.build('analyticsadmin', 'v1beta', credentials=credentials)
            accounts = fetch_accounts(credentials)
            for account in accounts.get('accounts', []):
                account_id = account['name'].split('/')[1]
                properties = fetch_properties(analytics, account_id)
                for prop in properties.get('properties', []):
                    datasets.append({'type': 'GA4', 'name': f"{account['displayName']} - {prop['displayName']}", 'id': prop['name'].split('/')[1]})

        # 업로드한 파일 목록 추가
        uploaded_files = [f for f in os.listdir(UPLOAD_FOLDER) if os.path.isfile(os.path.join(UPLOAD_FOLDER, f))]
        for file in uploaded_files:
            datasets.append({'type': 'File', 'name': file, 'id': file})

        return jsonify(datasets)
    except Exception as e:
        logging.error(f"Error listing datasets: {e}")
        return jsonify({"error": f"Failed to list datasets: {e}"}), 500

@app.route('/select_dataset', methods=['POST'])
def select_dataset():
    dataset_names = request.json.get('dataset_names')
    if not dataset_names:
        return jsonify({"error": "Dataset names are required"}), 400

    session['selected_datasets'] = dataset_names
    return jsonify({"success": True, "dataset_names": dataset_names})

@app.route('/fetch_ga4_data', methods=['POST'])
def fetch_ga4_data():
    # GA4 데이터 가져오는 로직 추가
    property_id = session.get('property_id')
    if not property_id:
        return jsonify({"error": "GA4 property ID is not set"}), 400

    try:
        # GA4 데이터 가져오기 로직 구현
        data = get_traffic_data(
            dimensions=[{"name": "date"}], 
            metrics=[{"name": "activeUsers"}], 
            start_date='7daysAgo', 
            end_date='today', 
            property_id=property_id
        )
        dataset_name = f"GA4_{property_id}"
        integrated_datasets[dataset_name] = data
        return jsonify({"message": "GA4 data fetched successfully", "dataset_name": dataset_name})
    except Exception as e:
        logging.error(f"Error fetching GA4 data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/fetch_other_api_data', methods=['POST'])
def fetch_other_api_data():
    api_endpoint = request.json.get('api_endpoint')
    if not api_endpoint:
        return jsonify({"error": "API endpoint is required"}), 400

    try:
        response = requests.get(api_endpoint)
        response.raise_for_status()
        data = response.json()
        dataset_name = f"API_{generate_unique_id()}"
        integrated_datasets[dataset_name] = pd.DataFrame(data)
        return jsonify({"message": "API data fetched successfully", "dataset_name": dataset_name})
    except Exception as e:
        logging.error(f"Error fetching API data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/get_uploaded_data', methods=['GET'])
def get_uploaded_data():
    file_path = session.get('uploaded_file_path')
    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "No file uploaded or file not found"}), 400

    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return jsonify({"error": "Unsupported file type"}), 400

        df.fillna('', inplace=True)  # NaN 값을 빈 문자열로 대체
        columns = df.columns.tolist()
        data = df.to_dict(orient='records')
        return jsonify({'columns': columns, 'data': data})
    except Exception as e:
        logging.error(f"Error reading uploaded data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/get_preprocessed_data', methods=['GET'])
def get_preprocessed_data():
    dataset_name = request.args.get('dataset_name')

    if not dataset_name:
        return jsonify({'error': 'Dataset name is required'}), 400

    try:
        dataset_name = unquote(dataset_name)
        dataset_path = os.path.join(UPLOAD_FOLDER, dataset_name)

        if not os.path.isfile(dataset_path):
            return jsonify({'error': 'Dataset not found'}), 404

        df = pd.read_csv(dataset_path)
        df = df.where(pd.notnull(df), None)  # NaN -> None
        data = df.to_dict(orient="records")

        # ✅ 세션에 파일 경로 저장
        old_path = session.get('preprocessed_data_path')
        session['preprocessed_data_path'] = dataset_path
        session["active_source"] = "file"
        
        if old_path != dataset_path:
            ensure_new_conversation()

        # ✅ DB에 컨텍스트 저장
        DBManager.save_conversation_context(session.get('conversation_id'), {
            "active_source": "file",
            "property_id": session.get("property_id"),
            "file_path": dataset_path
        })

        return jsonify({'data': data})

    except Exception as e:
        app.logger.error(f'Unexpected error: {e}')
        return jsonify({'error': 'An unexpected error occurred', 'details': str(e)}), 500


@app.route('/ask_question', methods=['POST'])
def ask_question():
    try:
        data = request.get_json()
        question = data.get('question')

        if not question:
            return jsonify({"error": "Question is required"}), 400

        property_id = session.get("property_id")
        # 🔥 파일 경로 세션 연동 (전처리 우선)
        file_path = session.get("preprocessed_data_path") or session.get("uploaded_file_path")
        
        user_id = session.get("user_id", "anonymous")
        conversation_id = session.get("conversation_id")

        if not conversation_id:
            import uuid
            session['conversation_id'] = str(uuid.uuid4())
            conversation_id = session['conversation_id']

        # 🔥 handle_question에 user_id와 conversation_id 추가 전달
        logging.info(f"[Ask] Question: {question}, Prop: {property_id}, File: {file_path}")
        
        response = handle_question(
            question,
            property_id=property_id,
            file_path=file_path,
            user_id=user_id,
            conversation_id=conversation_id,
            semantic=semantic
        )

        
        logging.info(f"[Ask] Response Keys: {response.keys() if isinstance(response, dict) else 'Not Dict'}")

        # [P0] Session Save
        session['last_response'] = response

        # [P0] Sanitize Response (NaN -> None)
        sanitized_response = sanitize(response)
        return jsonify(sanitized_response)

    except Exception as e:
        logging.error(f"Error processing question: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/save_report', methods=['POST'])
def save_report():
    try:
        data = request.get_json()
        title = data.get('title', 'Untitled Report')
        content = data.get('content') # JSON structure of the report
        
        user_id = session.get('user_id', 'anonymous')
        conversation_id = session.get('conversation_id')
        
        if not content:
            return jsonify({"error": "Report content is required"}), 400
            
        success = DBManager.save_report(user_id, conversation_id, title, content)
        
        if success:
            return jsonify({"success": True, "message": "Report saved to database"})
        else:
            return jsonify({"error": "Failed to save report to database"}), 500
            
    except Exception as e:
        logging.error(f"Error saving report: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/list_reports', methods=['GET'])
def list_reports():
    try:
        user_id = session.get('user_id', 'anonymous')
        reports = DBManager.get_reports(user_id)
        return jsonify({"success": True, "reports": reports})
    except Exception as e:
        logging.error(f"Error listing reports: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/get_report/<int:report_id>', methods=['GET'])
def get_report(report_id):
    try:
        report = DBManager.get_report_by_id(report_id)
        if report:
            return jsonify({"success": True, "report": report})
        else:
            return jsonify({"error": "Report not found"}), 404
    except Exception as e:
        logging.error(f"Error getting report: {e}")
        return jsonify({"error": str(e)}), 500

# [PHASE 2] Block Editing API
@app.route('/edit_block', methods=['POST'])
def edit_block():
    """Edit a report block using AI with different modes"""
    try:
        data = request.get_json()
        text = data.get('text', '')
        mode = data.get('mode', 'concise')
        
        if not text:
            return jsonify({"error": "Text is required"}), 400
        
        # Define mode-specific prompts
        mode_prompts = {
            "concise": "다음 텍스트를 간결하게 재작성하세요. 핵심만 남기고 불필요한 문장은 제거하세요. 2-3줄 이내로 작성하세요.",
            "executive": "다음 텍스트를 임원 보고용으로 재작성하세요. 비즈니스 임팩트와 핵심 수치를 강조하세요. 전문적이고 간결하게 작성하세요.",
            "marketing": "다음 텍스트를 마케팅 자료용으로 재작성하세요. 긍정적이고 설득력 있게 작성하세요. 성과를 강조하세요.",
            "data-focused": "다음 텍스트를 데이터 중심으로 재작성하세요. 구체적인 수치와 통계를 강조하세요. 객관적으로 작성하세요."
        }
        
        prompt = mode_prompts.get(mode, mode_prompts["concise"])
        
        # Call LLM
        import openai
        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a professional content editor. Follow the instructions precisely."},
                {"role": "user", "content": f"{prompt}\n\n원본 텍스트:\n{text}"}
            ],
            temperature=0.3
        )
        
        edited_text = res['choices'][0]['message']['content'].strip()
        
        return jsonify({
            "success": True,
            "original": text,
            "edited": edited_text,
            "mode": mode
        })
        
    except Exception as e:
        logging.error(f"Error editing block: {e}")
        return jsonify({"error": str(e)}), 500



# [PHASE 3] Report-level Editing API
@app.route('/edit_report', methods=['POST'])
def edit_report():
    """Edit entire report using AI with structured block format (SAFE JSON MERGE VERSION)"""
    try:
        import json
        import re
        import uuid
        import openai

        data = request.get_json()
        blocks = data.get("blocks", [])
        instruction = data.get("instruction", "")

        if not blocks or not instruction:
            return jsonify({"error": "Blocks and instruction are required"}), 400

        # ------------------------------------------------------------------
        # 1) Ensure every block has a stable id (critical for safe merging)
        # ------------------------------------------------------------------
        normalized_blocks = []
        for b in blocks:
            if not isinstance(b, dict):
                continue

            block_id = b.get("id")
            if not block_id:
                block_id = str(uuid.uuid4())

            normalized_blocks.append({
                "id": block_id,
                "html": b.get("html", ""),
                "plotData": b.get("plotData"),
                "chartId": b.get("chartId"),
                "source": b.get("source"),
                "created_at": b.get("created_at")
            })

        if not normalized_blocks:
            return jsonify({"error": "No valid blocks found"}), 400

        # ------------------------------------------------------------------
        # 2) LLM Input should NOT include plotData (token waste + corruption risk)
        #    Only pass id + html to rewrite safely
        # ------------------------------------------------------------------
        llm_context = [
            {
                "id": b["id"],
                "html": b["html"]
            }
            for b in normalized_blocks
        ]

        context_json = json.dumps(llm_context, ensure_ascii=False, indent=2)

        prompt = f"""
다음은 데이터 분석 리포트 블록들입니다. (JSON 배열)

{context_json}

사용자 요청:
{instruction}

요청에 따라 각 블록의 "html"만 수정하세요.

반드시 아래 JSON 형식 그대로 반환하세요:

[
  {{
    "id": "...원본 id 그대로...",
    "html": "...수정된 html..."
  }}
]

주의:
- id는 절대 변경하지 마세요.
- 블록을 삭제하거나 새로 추가하지 마세요.
- JSON 배열만 반환하세요.
- 설명 문장 없이 JSON만 반환하세요.
"""

        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a professional report editor. Return ONLY valid JSON array. Do not add explanations."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )

        response_text = res["choices"][0]["message"]["content"].strip()

        # ------------------------------------------------------------------
        # 3) Remove markdown fences if exists
        # ------------------------------------------------------------------
        if "```" in response_text:
            response_text = re.sub(r"```json|```", "", response_text).strip()

        edited_minimal = json.loads(response_text)

        if not isinstance(edited_minimal, list):
            return jsonify({"error": "LLM output is not a JSON array"}), 500

        # ------------------------------------------------------------------
        # 4) Build edited html map (id -> html)
        # ------------------------------------------------------------------
        edited_html_map = {}
        for item in edited_minimal:
            if not isinstance(item, dict):
                continue
            if "id" not in item or "html" not in item:
                continue
            edited_html_map[item["id"]] = item["html"]

        # ------------------------------------------------------------------
        # 5) Merge: preserve plotData/chartId/source/created_at
        # ------------------------------------------------------------------
        merged_blocks = []
        for b in normalized_blocks:
            block_id = b["id"]

            merged_blocks.append({
                "id": block_id,
                "html": edited_html_map.get(block_id, b.get("html", "")),
                "plotData": b.get("plotData"),
                "chartId": b.get("chartId"),
                "source": b.get("source"),
                "created_at": b.get("created_at")
            })

        return jsonify({
            "success": True,
            "blocks": merged_blocks
        })

    except Exception as e:
        logging.error(f"Error editing report: {e}")
        return jsonify({"error": str(e)}), 500


            
@app.route('/ask_csv', methods=['POST'])
def ask_csv():
    data = request.get_json()
    question = data.get("question")
    dataset_name = data.get("dataset_name")

    if not question or not dataset_name:
        return jsonify({"error": "question and dataset_name required"}), 400

    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, UPLOAD_FOLDER, dataset_name)
    response = file_engine.process(question, file_path)

    return jsonify({"response": response, "route": "file"})


@app.route('/list_preprocessed_data', methods=['GET'])
def list_preprocessed_data():
    files = os.listdir('uploaded_files')
    return jsonify(files)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5001)

