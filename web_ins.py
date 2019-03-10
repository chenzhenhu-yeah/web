import pandas as pd
from flask import Flask, render_template, request, redirect

def read_log():
    today = '2019-01-21'
    logfile= 'autotrade.log'
    #df = pd.read_csv(logfile,sep=' ',header=None,encoding='ansi')
    df = pd.read_csv(logfile,sep=' ',header=None,encoding='gbk')
    df = df[df[0]==today]
    # print(df)

    r = []
    for i, row in df.iterrows():
        r.append(str(list(row)))

    return r

def read_ins():
    pass

app = Flask(__name__)
# app = Flask(__name__,template_folder='tpl') # 指定一个参数使用自己的模板目录

@app.route('/')
def index():
 return 'log file ins'

@app.route('/log')
def show_log():
    items = read_log()
    return render_template("show_log.html",title="Show Log",items=items)

@app.route('/file')
def upload_file():
    return render_template("upload_file.html",title="Show Log",items=items)

@app.route('/ins')
def ins():
    items = read_ins()
    return render_template("ins.html",title="ins",items=items)

@app.route('/confirm_ins', methods=['post'])
def confirm_ins():
    ins_type = request.form.get('ins_type')
    portfolio = request.form.get('portfolio')
    agent = request.form.get('agent')
    return ins_type+portfolio+agent

@app.route('/submit_ins', methods=['post'])
def submit_ins():
    pass

if __name__ == '__main__':
    app.run(debug=True)
