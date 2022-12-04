from flask import Flask ,jsonify,request,render_template,redirect,url_for
from mysql.connector import pooling
import mysql.connector
import os
from datetime import date

"""
Abdulrahman Amer Finalysis, RISK API 
"""

app=Flask(__name__)


@app.route("/v1/risk/<string:symbol1>/<string:apiKEY>", methods=['GET','POST'])
def correlation(symbol1: str, apiKEY:str):

    today = date.today()
    db = CONN_POOL.get_connection()
    curr = db.cursor(buffered=True)
    api_key = apiKEY
    try:
        sql = "SELECT correlation,openValue,closeValue FROM hiddenRisk WHERE LOWER(stock) = (%s)"
        val = (symbol1.lower(),)
        curr.execute(sql,val)
        db.commit()
        a = curr.fetchall()
        list1, list2, list3 = zip(*a)

        list = []

        for i in range(0,len(list1)):
            dict_ = {}
            name = list1[i]
            open_ = list2[i]
            close_= list3[i]
            dict_['ticker']=name
            dict_['open']=open_
            dict_['close']=close_
            list.append(dict_)
        
        if a:
            return jsonify({"status":200, "msg":"ok",'ticker':f'{symbol1}', "result":[{'corr':list},{"date":f'{today}'}]})
        else:
            return jsonify({"status":200, "msg":'ok', 'result':['NONE']})
    except mysql.connector.Error as e:
        msg = f'{e}'
        return jsonify({'error':500,'msg':msg})
    finally:
        db.close()
        curr.close()

if __name__ == "__main__":
    try:
       
        URL_ENV_VAR = os.environ.get('URL_ENV_VAR')      
        host = os.environ.get('Risk_HOST')
        port = os.environ.get('Risk_PORT')
        db_name = os.environ.get('Risk_DATABASE_NAME')
        db_host = os.environ.get('Risk_DATABASE_HOST')
        db_user = os.environ.get('Risk_DATABASE_USER')
        db_pass = os.environ.get('Risk_DATABASE_PASS')
        db_pool_size = os.environ.get('Risk_DATABASE_POOL_SIZE')

        if URL_ENV_VAR is None:
            URL_ENV_VAR = 'http://127.0.0.1:5000/'

        if host is None:
            host = '127.0.0.1'

        if port is None:
            port = 5000
        else:
            port = int(port)

        if db_name is None:
            db_name = 'Risk'

        if db_host is None:
            db_host = 'localhost'

        if db_user is None:
            db_user = 'root'

        if db_pass is None:
            db_pass = '13L0ck22!Mjordan23!'

        if db_pool_size is None:
            db_pool_size = 5
        else:
            db_pool_size = int(db_pool_size)

        CONN_POOL = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="finalysis_pool",
            pool_size=db_pool_size,
            pool_reset_session=True,
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_pass)

    except mysql.connector.Error as e:
        print(f'MySQL Connection Error: {e}')

    finally:    
        app.run(host=host, port=port, threaded=True)