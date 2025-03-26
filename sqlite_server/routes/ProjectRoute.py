from flask import Flask, request, jsonify
import sqlite3
import os
import uuid
import pandas as pd
from werkzeug.utils import secure_filename
import time
import threading

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"sqlite", "csv"}
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def delete_old_files():
    while True:
        current_time = time.time()
        for file in os.listdir(UPLOAD_FOLDER):
            if file == "": ####### UPDATE THE DEFAULT FILE HERE
                continue  # Skip deleting this specific file
            file_path = os.path.join(UPLOAD_FOLDER, file)
            if os.path.isfile(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > 14400:  # 4 hours in seconds
                    try:
                        os.remove(file_path)
                        print(f"Deleted old file: {file}")
                    except Exception as e:
                        print(f"Error deleting file {file}: {e}")
        time.sleep(3600)

def convert_csv_to_sqlite(csv_path, sqlite_path):
    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(sqlite_path)
    df.to_sql("csv_data", conn, index=False, if_exists="replace")
    conn.close()

@app.route("/upload-file", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    
    if file and allowed_file(file.filename):
        ext = file.filename.rsplit(".", 1)[1].lower()
        file_uuid = str(uuid.uuid4())
        filename = f"{file_uuid}.{ext}"
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(file_path)
        
        if ext == "csv":
            sqlite_path = os.path.join(app.config["UPLOAD_FOLDER"], f"{file_uuid}.sqlite")
            try:
                convert_csv_to_sqlite(file_path, sqlite_path)
                os.remove(file_path)  # Delete CSV after conversion
                return jsonify({"uuid": file_uuid}), 200
            except Exception as e:
                return jsonify({"error": f"CSV conversion failed: {str(e)}"}), 500
        
        return jsonify({"uuid": file_uuid}), 200
    
    return jsonify({"error": "Invalid file type"}), 400

@app.route("/execute-query", methods=["POST"])
def execute_query():
    data = request.json
    file_uuid = data.get("uuid")
    query = data.get("query")
    
    if not file_uuid or not query:
        return jsonify({"error": "Missing uuid or query"}), 400
    
    db_path = os.path.join(app.config["UPLOAD_FOLDER"], f"{file_uuid}.sqlite")
    
    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found"}), 404
    
    if not query.strip().lower().startswith("select"):
        return jsonify({"error": "Only SELECT queries are allowed"}), 400
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        return jsonify({"results": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@app.route("/get-schema/<uuid>", methods=["GET"])
def get_schema(uuid):
    db_path = os.path.join(app.config["UPLOAD_FOLDER"], f"{uuid}.sqlite")
    
    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found"}), 404
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        schema = []
        
        for table_name, create_statement in tables:
            schema.append({"table": table_name, "create_statement": create_statement})
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3;")
            schema[-1]["example_rows"] = cursor.fetchall()
        
        conn.close()
        return jsonify({"schema": schema}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@app.route("/")
def home():
    return "Server is up and running"

if __name__ == "__main__":
    threading.Thread(target=delete_old_files, daemon=True).start()
    app.run(debug=True, port=3001)