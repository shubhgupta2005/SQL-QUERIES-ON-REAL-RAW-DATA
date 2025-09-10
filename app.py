import os
import pandas as pd
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
from flask_cors import CORS
@app.route('/')
def index():
    """Serves the main index.html file."""
    return render_template('index.html')

# --- Database Connection ---
# This code is designed for deployment. It reads the database URL 
# from an environment variable set by the hosting service (like Render).
DATABASE_URL = os.environ.get('DATABASE_URL')

# Render's PostgreSQL URLs start with 'postgres://', but SQLAlchemy needs 'postgresql://'
# This line ensures the URL is in the correct format.
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Create the connection to the database
engine = create_engine(DATABASE_URL)

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app) # Enable Cross-Origin Resource Sharing

# --- Helper Function ---
# A reusable function to execute queries and handle errors
def run_query(query_string):
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query_string), connection)
        results = df.to_dict(orient='records')
        return jsonify(results)
    except Exception as e:
        # Return any database errors to the frontend
        return jsonify({"error": str(e)}), 400

# --- API Endpoints ---

# 1. Endpoint for the raw CSV data preview
@app.route('/api/raw-data-preview')
def get_raw_data_preview():
    try:
        df_raw = pd.read_csv('RAW DATA DBMS.csv')
        df_preview = df_raw.head(100)
        results = df_preview.to_dict(orient='records')
        return jsonify(results)
    except FileNotFoundError:
        return jsonify({"error": "RAW DATA DBMS.csv not found on the server."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 2. Endpoint for Top 10 Commodities by Average Index
@app.route('/api/top-commodities')
def get_top_commodities():
    query = "SELECT COMM_NAME, AVG(Index_Value) AS AverageIndex FROM wholesale_prices GROUP BY COMM_NAME ORDER BY AverageIndex DESC LIMIT 10;"
    return run_query(query)

# 3. Endpoint for 10 Most Recent Entries
@app.route('/api/recent-entries')
def get_recent_entries():
    query = "SELECT COMM_NAME, Date, Index_Value FROM wholesale_prices ORDER BY Date DESC LIMIT 10;"
    return run_query(query)

# 4. Endpoint for Top 10 Most Volatile Commodities
@app.route('/api/most-volatile')
def get_most_volatile():
    query = "SELECT COMM_NAME, STDDEV(Index_Value) AS Volatility FROM wholesale_prices GROUP BY COMM_NAME ORDER BY Volatility DESC LIMIT 10;"
    return run_query(query)

# 5. Endpoint for Top 10 Most Stable Commodities
@app.route('/api/most-stable')
def get_most_stable():
    query = "SELECT COMM_NAME, STDDEV(Index_Value) AS Volatility FROM wholesale_prices GROUP BY COMM_NAME HAVING Volatility > 0 ORDER BY Volatility ASC LIMIT 10;"
    return run_query(query)

# 6. Endpoint for Top 10 Commodities with the Highest Peak Price
@app.route('/api/highest-peak')
def get_highest_peak():
    query = "SELECT COMM_NAME, MAX(Index_Value) AS HighestIndex FROM wholesale_prices GROUP BY COMM_NAME ORDER BY HighestIndex DESC LIMIT 10;"
    return run_query(query)

# 7. Endpoint for Top 10 Commodities with the Lowest Price Trough
@app.route('/api/lowest-trough')
def get_lowest_trough():
    query = "SELECT COMM_NAME, MIN(Index_Value) AS LowestIndex FROM wholesale_prices GROUP BY COMM_NAME ORDER BY LowestIndex ASC LIMIT 10;"
    return run_query(query)

# 8. Endpoint for "Onion" Price Trend
@app.route('/api/onion-prices')
def get_onion_prices():
    query = "SELECT Date, Index_Value FROM wholesale_prices WHERE COMM_NAME = 'Onion' ORDER BY Date ASC;"
    return run_query(query)

# 9. Endpoint for Overall Average Index Per Year
@app.route('/api/yearly-average')
def get_yearly_average():
    query = "SELECT YEAR(Date) as Year, AVG(Index_Value) as AverageIndex FROM wholesale_prices GROUP BY YEAR(Date) ORDER BY Year ASC;"
    return run_query(query)

# 10. Endpoint to Count Unique Commodities
@app.route('/api/unique-commodities')
def get_unique_commodities():
    query = "SELECT COUNT(DISTINCT COMM_NAME) as NumberOfUniqueCommodities FROM wholesale_prices;"
    return run_query(query)

# 11. Endpoint for the Custom SQL Query Runner
@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    data = request.get_json()
    query = data.get('query')
    if not query:
        return jsonify({"error": "No query provided."}), 400
    return run_query(query)

# This block allows you to run the app locally for testing
if __name__ == '__main__':
    # For local development, you might need to set a default DATABASE_URL
    # if it's not in your environment.
    # For example:
    # DATABASE_URL = "mysql+mysqlconnector://root:your_password@localhost/dbms_project"
    # engine = create_engine(DATABASE_URL)
    app.run(debug=True)

