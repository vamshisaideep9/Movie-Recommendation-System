from flask import Flask
from src.logger import logging

app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def index():
    logging.info("We are testing logging")
    return "Welcome to the page"
if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True)