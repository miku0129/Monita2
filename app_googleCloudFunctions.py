# -*- coding: utf-8 -*-
import fitbit
from ast import literal_eval
from datetime import datetime, timedelta, timezone
import pandas 
import json

#Fitbit ID等設定
def build_fitbit_authed_client(CLIENT_ID,CLIENT_SECRET,ACCESS_TOKEN,REFRESH_TOKEN):
    # ID等の設定
    authed_client = fitbit.Fitbit(CLIENT_ID, CLIENT_SECRET
                             ,access_token=ACCESS_TOKEN, refresh_token=REFRESH_TOKEN)
    return authed_client

# 直近7日間の日付リストを作成する
def build_date_list():
    date_array = []    
    # タイムゾーンの生成
    JST = timezone(timedelta(hours=+9),'JST')
    for i in range(0,7):
        date = datetime.now(JST).date() - timedelta(days = i)
        date_array.append(str(date))
    return date_array

# 実行日の分リストを作成する
def build_minutes_list():
    minutes_array = []
    # タイムゾーンの生成
    JST = timezone(timedelta(hours=+9),'JST')    
    seed_timestamp = datetime.now(JST).replace(hour=0,minute=0,second=0,microsecond=0)         
    for minute in range(1,1440):
        minutes_array.append( (seed_timestamp + timedelta(minutes = minute)).strftime("%H:%M:%S"))
    return minutes_array

# 直近7日間のデータを日別で取得し、辞書を返却する(Activity系)
def build_days_metrics_dict(authed_client,dates_list):
    days_result_dict = {}

    for date in dates_list:
        singleday_activity_metrics = []        

        # 該当日のActivity系の指標を取得
        activity_metrics = ['caloriesOut','steps','lightlyActiveMinutes','veryActiveMinutes']
        activity_response = authed_client.activities(date=date)
        for metrics_name in activity_metrics:
            try:
                singleday_activity_metrics.append(activity_response['summary'][metrics_name])
            except:
                singleday_activity_metrics.append(0) 

        # 該当日の指標を辞書に格納
        days_result_dict[date] = singleday_activity_metrics
    return days_result_dict

# 実行日のデータを分単位で取得し、辞書を返却する
def build_intraday_metrics_dict(authed_client,minutes_list):
    intraday_minutes_result_dict = {}

    per_minutes_steps = authed_client.intraday_time_series('activities/steps', base_date=str((datetime.now()).date()), detail_level='1min', start_time="00:00", end_time="23:59")

    per_minutes_calories = authed_client.intraday_time_series('activities/calories', base_date=str((datetime.now()).date()), detail_level='1min', start_time="00:00", end_time="23:59")    

    # リクエストから、分単位のデータを取得し、辞書に整形する
    for minute in minutes_list:
        per_minute_result = []  

        #分刻みのStepsを配列に追加
        steps_values = [x['value'] for x in per_minutes_steps['activities-steps-intraday']['dataset'] if x['time'] == minute]
        step_value = steps_values[0] if len(steps_values) else ''
        per_minute_result.append(step_value)

        #分刻みのCaloriesを配列に追加
        calories_values = [x['value'] for x in per_minutes_calories['activities-calories-intraday']['dataset'] if x['time'] == minute]
        calories_value = calories_values[0] if len(calories_values) else ''
        per_minute_result.append(calories_value)        

        intraday_minutes_result_dict[minute] = per_minute_result

    return intraday_minutes_result_dict


# DataFrameをBigQueryの任意のプロジェクト / 保存先にエクスポートする
def export_df_to_bq(df,project_id,dataset_name,table_name):
    response = df.to_gbq(dataset_name + '.' + table_name, project_id, if_exists = 'replace')
    return response

# DictをDataFrameに変換する
def convert_dict_to_dataframe(dic,column_names,index_name):
    converted_df = pandas.DataFrame.from_dict(dic,orient = 'index',columns = column_names).reset_index()
    return converted_df   

def fitbit_data_byDayAndMinutes():
    #Fitbit ID等設定
    CLIENT_ID     = "22C2HT"
    CLIENT_SECRET = "cd36c066c7dd5191eadf89ff466c5ea5" 
    ACCESS_TOKEN =  "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyMkMySFQiLCJzdWIiOiI4WDRQUlMiLCJpc3MiOiJGaXRiaXQiLCJ0eXAiOiJhY2Nlc3NfdG9rZW4iLCJzY29wZXMiOiJyc29jIHJhY3QgcnNldCBybG9jIHJ3ZWkgcmhyIHJwcm8gcm51dCByc2xlIiwiZXhwIjoxNjA0OTEyODEyLCJpYXQiOjE2MDQ4ODQwMTJ9.SHqn4mzgvHKdWDiRXpkyReDkrKBx8c5aPG2YpADOndg"
    REFRESH_TOKEN =  "fda7902458179d9fc776592aaaff73caffc43ab1bd156b34ae3924c0c1f30928"
    
    ## BigQuery関連(★★自身の情報に変更します★★)
    project_id = "bigminiconf-nov2020"
    dataset_name = "1234_5"

    # 認証済みクライアントの作成
    authed_client = build_fitbit_authed_client(CLIENT_ID,CLIENT_SECRET,ACCESS_TOKEN,REFRESH_TOKEN)

     # 時系列リストの生成
    dates_list = build_date_list()
    minutes_list = build_minutes_list()  

    # 日別データの取得 -> DataFrameに変換
    ## データの取得
    days_result_dict = build_days_metrics_dict(authed_client,dates_list)
    # print('days_result_dict',days_result_dict)

     ## DataFrameに変換/day
    days_clumns_name = ['caloriesOut','steps','lightlyActiveMinutes','veryActiveMinutes']    
    days_result_df = convert_dict_to_dataframe(days_result_dict,days_clumns_name,'date')
    print('--------日別の数値--------')
    print(days_result_df)
    print('------------------------')
    # BigQueryに書き込み
    days_table_name = 'days_metrics'
    export_df_to_bq(days_result_df,project_id,dataset_name,days_table_name)    

    #DataFrameに変換/minutes 
    minute_result_dict = build_intraday_metrics_dict(authed_client,minutes_list)
    minute_clumns_name = ['steps','calories']   
    minute_result_df = convert_dict_to_dataframe(minute_result_dict,minute_clumns_name,'minute')    
    print('--------時間別の数値--------')
    print(minute_result_df)
    print('------------------------')
    # BigQueryに書き込み    
    minute_table_name = 'minutes_metrics'
    export_df_to_bq(minute_result_df,project_id,dataset_name,minute_table_name) 

# This code is necessary to invoke function in cloud function 
def get_fitbit_data_byDayAndMinutes(event,context):
    fitbit_data_byDayAndMinutes()