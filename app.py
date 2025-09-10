# app.py

from flask import Flask, jsonify, request
from sqlalchemy import create_engine
import pandas as pd
from flask_cors import CORS

# --- Database Connection (No changes) ---
db_user = 'root'
db_password = 'Arrow#1234' # Make sure your password is correct
db_host = 'localhost'
db_name = 'dbms_project'
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

app = Flask(__name__)
CORS(app)

# --- Helper function to run queries ---
def run_query(query):
    try:
        df = pd.read_sql(query, engine)
        results = df.to_dict(orient='records')
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# --- API Endpoints ---

@app.route('/api/raw-data-preview')
def get_raw_data_preview():
    return run_query("SELECT * FROM wholesale_prices LIMIT 100;") # Simplified to use our helper

# --- Pre-defined Query Endpoints ---

@app.route('/api/top-commodities')
def get_top_commodities():
    query = "SELECT COMM_NAME, AVG(Index_Value) AS AverageIndex FROM wholesale_prices GROUP BY COMM_NAME ORDER BY AverageIndex DESC LIMIT 10;"
    return run_query(query)

@app.route('/api/recent-entries')
def get_recent_entries():
    query = "SELECT COMM_NAME, Date, Index_Value FROM wholesale_prices ORDER BY Date DESC LIMIT 10;"
    return run_query(query)

@app.route('/api/most-volatile')
def get_most_volatile():
    query = "SELECT COMM_NAME, STDDEV(Index_Value) AS Volatility FROM wholesale_prices GROUP BY COMM_NAME ORDER BY Volatility DESC LIMIT 10;"
    return run_query(query)

@app.route('/api/most-stable')
def get_most_stable():
    query = "SELECT COMM_NAME, STDDEV(Index_Value) AS Volatility FROM wholesale_prices GROUP BY COMM_NAME HAVING Volatility > 0 ORDER BY Volatility ASC LIMIT 10;"
    return run_query(query)

@app.route('/api/highest-peak')
def get_highest_peak():
    query = "SELECT COMM_NAME, MAX(Index_Value) AS HighestIndex FROM wholesale_prices GROUP BY COMM_NAME ORDER BY HighestIndex DESC LIMIT 10;"
    return run_query(query)

@app.route('/api/lowest-trough')
def get_lowest_trough():
    query = "SELECT COMM_NAME, MIN(Index_Value) AS LowestIndex FROM wholesale_prices GROUP BY COMM_NAME ORDER BY LowestIndex ASC LIMIT 10;"
    return run_query(query)

@app.route('/api/onion-prices')
def get_onion_prices():
    query = "SELECT Date, Index_Value FROM wholesale_prices WHERE COMM_NAME = 'Onion' ORDER BY Date ASC;"
    return run_query(query)

@app.route('/api/yearly-average')
def get_yearly_average():
    query = "SELECT YEAR(Date) as Year, AVG(Index_Value) as AverageIndex FROM wholesale_prices GROUP BY YEAR(Date) ORDER BY Year ASC;"
    return run_query(query)

@app.route('/api/unique-commodities')
def get_unique_commodities():
    query = "SELECT COUNT(DISTINCT COMM_NAME) as NumberOfUniqueCommodities FROM wholesale_prices;"
    return run_query(query)

# --- Custom Query Endpoint ---

@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    data = request.get_json()
    query = data.get('query')
    if not query:
        return jsonify({"error": "No query provided."}), 400
    return run_query(query) # Use our helper here too

if __name__ == '__main__':
    app.run(debug=True)