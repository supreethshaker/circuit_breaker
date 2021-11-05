from flask import Flask, escape, request, jsonify
import time

app = Flask(__name__)

"""
Flask application that simply serves GET endpoints
This web server is to demonstrate the implementation for a
circuit breaker on a proxy server but can be extended
into a full-fledged web application easily without requiring any
change on the proxy OR the circuitbreaker
"""

@app.route("/", methods=["GET"])
def get_json_response():
    return {"message": "success"}

@app.route("/test-path", methods=["GET"])
def get_test():
    #ime.sleep(35)
    return {"message": "test path is successful"}

if __name__ == "__main__":
    app.run(debug=False, use_debugger=False, use_reloader=True)