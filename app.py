import os
import requests 
import json
import string
import random
import pymysql
import datetime
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from flask import Flask
from flask import jsonify
from flask import request

app = Flask(__name__)

def logger(file_name):
    """

    :param file_name:
    :return:
    """

    log_filename = os.path.join(file_name)
    logger = logging.getLogger('getdata')
    logger.setLevel(logging.DEBUG)
    handler = TimedRotatingFileHandler(log_filename, when="midnight")
    formatter = logging.Formatter('%(levelname)-6s %(asctime)s : %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

N = 7

DATABASE_HOST = "mt2.com"
DATABASE_USR = "root"
DATABASE_PWD = "adm_user@123"
DATABASE_NAME = "oat"

def db_conn():
    try:
        db = pymysql.connect(host=DATABASE_HOST,user=DATABASE_USR,password=DATABASE_PWD,database=DATABASE_NAME)
        cursor = db.cursor()
    except Exception as e:
        logger.error(e)
        print (e)
    return db, cursor


@app.route('/')
def index():
    return "Welcome, Please use Grafana dashboard http://mt2:3000/d/n2FYA0-Vk/cronmon?orgId=1 to view your cron details or use endpoint /create/new_cron to create new cron monitor" 

@app.route("/create/new_cron",methods = ['POST'])
def get_details():
    try:
        data = request.json
        result = {}
        output = ""
        cron_name = data.get('cron_name')
        cron_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))
        owner = data.get('owner_email')
        dt_added = datetime.datetime.now()
        interval_min = data.get('interval_min')
        language = data.get('language')
        
        logger.info("cron: {}".format(data))
        
        db, cursor = db_conn()
        
        sql = ("""INSERT INTO cronmon (cron_id, cron_name, owner, dt_added, interval_min, language) VALUES ('%s','%s','%s','%s','%s','%s')""" )%(cron_id, cron_name, owner, dt_added, interval_min,language)
        try:
            cursor.execute(sql)
            db.commit()
            output= "successfully added cron for monitoring"  
            result["msg"] = output
            result["cron_id"] = cron_id
            sample_code = """response = requests.request("POST", "http://127.0.0.1:4455/update", json={"cron_id":<CRON_ID_HERE>, "run_id":run_id, "stage":"start"})"""
            result["usage"] = "Use the following snippet in your code: {} ".format(sample_code)     
            
        except Exception as e:
            logger.exception(e)
            db.rollback()
            result["msg"] = "Failed to add cron to monitoring, please contact admin"
            
        output = jsonify(result)
        
    except Exception as err_msg:
        logger.exception(err_msg)
        db.rollback()
        output = "Error occurred, {}".format(err_msg)
    
    db.close()
    return output
        
@app.route("/update",methods = ['POST'])
def get_updates():
    try:
        result = {}
        output = ""
        
        content = request.json
        logger.info("details: {}".format(content))
        
        db, cursor = db_conn()
        
        cron_id = content["cron_id"]
        run_id = content["run_id"]
        stage = content["stage"]
        sql = ("""select cron_name from cronmon where cron_id='%s'""")%(cron_id)
        cursor.execute(sql)
        data = cursor.fetchall()
        cron_name = data[0][0]
        
        
        
        if stage == 'start':
            start_time = datetime.datetime.now()
            sql1 = ("""INSERT INTO cronmon_details (cron_id, cron_name, run_id, start_time) VALUES ('%s','%s','%s','%s')""" )%(cron_id, cron_name, run_id, start_time)
        else:
            end_time = datetime.datetime.now()
            sql1 = ("""Update cronmon_details set end_time='%s' where cron_id='%s' and run_id='%s'  """ )%(end_time, cron_id, run_id)
        try:
            cursor.execute(sql1)
            db.commit()
            output= "successfully updated cron run"  
               
            
        except Exception as e:
            logger.exception(e)
            db.rollback()
        
    except Exception as err_msg:
        logger.exception(err_msg)
        db.rollback()
        output = "Error occurred, {}".format(err_msg)

    db.close()
    return output

@app.route('/ping')
def ping():
    msg = {'msg':'Pong!!!'
    }
    return msg


if __name__ == '__main__':
    logger = logger('cronmon.log')
    app.run(host='0.0.0.0', port=5000, debug=True)