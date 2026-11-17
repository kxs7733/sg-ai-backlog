from flask import Flask, send_file
import os

app = Flask(__name__)

@app.route('/')
def home():
    return send_file('index.html')

@app.route('/math')
def math_game():
    return send_file('math.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
