import os
import pandas as pd
from flask import Flask, jsonify, request, render_template # <-- Make sure render_template is imported
from sqlalchemy import create_engine, text
from flask_cors import CORS

# --- Flask App Initialization (Moved Up) ---
app = Flask(__name__)
CORS(app) # Enable Cross-Origin Resource Sharing

# --- Database Connection ---
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
engine = create_engine(DATABASE_URL)

# --- Helper Function ---
def run_query(query_string):
    # ... (rest of the function is the same)
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query_string), connection)
        results = df.to_dict(orient='records')
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# --- Route to Serve the HTML Page (Now correctly placed after app is created) ---
@app.route('/')
def index():
    """Serves the main index.html file."""
    return render_template('index.html')

# --- All other API Endpoints ---
# ... (all your other @app.route functions like /api/raw-data-preview, etc.) ...
@app.route('/api/raw-data-preview')
def get_raw_data_preview():
    # ...
    try:
        df_raw = pd.read_csv('RAW DATA DBMS.csv')
        df_preview = df_raw.head(100)
        results = df_preview.to_dict(orient='records')
        return jsonify(results)
    except FileNotFoundError:
        return jsonify({"error": "RAW DATA DBMS.csv not found on the server."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
# (Include all 9 of your other common query routes here)

@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    # ... (function content is the same)
    data = request.get_json()
    query = data.get('query')
    if not query:
        return jsonify({"error": "No query provided."}), 400
    return run_query(query)

# --- Main block for local development ---
if __name__ == '__main__':
    app.run(debug=True)
