import pandas as pd
from flask import Flask, render_template, request

app = Flask(__name__)
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/', methods=['GET', 'POST'])
def home():
    return '<h1>Hello World!</h1><a href="http://127.0.0.1:5000/signin">登录</a>'

@app.route('/signin', methods=['GET'])
def signin_form():
    return '''<form action="/signin" method="post">
    <p><input name="username"></p>
    <p><input name="password" type="password"></p>
    <p><button type="submit">Sign In</button></p>
    </form>'''

@app.route('/signin', methods=['POST'])
def signin():
    if request.form['username']=='admin' and request.form['password']=='password':
        return '<h2>hello,admin</h2>'
    return '<h2>bad username or password</h2>'

if __name__ == '__main__':
    app.run(debug=True)
