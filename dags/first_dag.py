from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

output_meet = "/home/tnanun/airflow-meet/data/data_meet.csv"
output_ans =  "/home/tnanun/airflow-meet/data/data_ans.csv"
output_folders = "/home/tnanun/airflow-meet/data/data_folder.csv"
output_com = "/home/tnanun/airflow-meet/data/data_com.csv"
output_obj = "/home/tnanun/airflow-meet/data/data_obj.csv"
output_final = "/home/tnanun/airflow-meet/data/final_dataset.csv"

def fetch_data(table, output):
  # con = create_engine("postgresql://postgres:12#3$45)6G88@db.oymrhotncbnylwsroxra.supabase.co:5432/postgres")
  con = create_engine("postgres://postgres.oymrhotncbnylwsroxra:12#3$45)6G88@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres")
  ## postgres://postgres.oymrhotncbnylwsroxra:12#3$45)6G88@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres
  df = pd.read_sql(f'select * from "{table}";', con)
  if table == "meeting":
      df = df[ df.meetStatus == True ]
      df.to_csv(output, index = False)
  elif table == "answer":
      df.to_csv(output, index = False)
  elif table == "folders":
      df.to_csv(output, index = False)
  elif table == "meetObj":
      df = df[ df.objStatus == False ]
      df.to_csv(output, index = False)
  else:
      df.to_csv(output, index = False)

  print(f"Output to {output} yoyoyoyoyoyo")

def clean_meet(output_meet):
    meet_type = {
    1: 'Decision-Making',
    2: 'Problem-Solving',
    3: 'Info/Opinion-Sharing'
    }

    df = pd.read_csv(output_meet)
    ## แปลงเลขเป็นคำ
    df['meetTagId'] = df['meetTagId'].replace(meet_type)
    ## แปลงให้เป็น Datetime
    df[['meetStartDate', 'meetEndDate', 'meetStartTime', 'meetEndTime']] = df[['meetStartDate', 'meetEndDate', 'meetStartTime', 'meetEndTime']].apply(pd.to_datetime)
    ## เอาวัน 
    df['meetStartDate'] = df['meetStartDate'].dt.date
    df = df.dropna(subset=['meetStartDate'])
    # ## หาเวลาใช้ในการประชุม
    df['minusTime'] = df['meetEndTime'] - df['meetStartTime']
    df['meetStartTime'] = df['meetStartTime'].dt.time
    df['meetEndTime'] = df['meetEndTime'].dt.time
    df["minusTime"] = df["minusTime"].dt.total_seconds() / 60

    ## หา day of week
    df["dow"] = df[['meetStartDate']].apply(lambda x: dt.datetime.strftime(x['meetStartDate'], '%A'), axis=1)
    df_meet = df[["folderId", "meetId", "meetName", "minusTime", "meetStartDate","dow", "meetTagId"]].rename( columns = {"minusTime":"duration_minutes", "meetTagId": "meetTag"})
    # df_meet['folderId'] = df_meet['folderId'].astype(int)
    df_meet.to_csv(output_meet, index = False)
    print(f"Output to {output_meet} yoyoyoyoyoyo")

def clean_answer(output_ans):
    df = pd.read_csv(output_ans)
    ## นับจำนวนคนที่เข้าร่วมแต่ละ meeting
    df_count_per_meet = pd.DataFrame(df.groupby('meetId')['userId'].nunique()).rename(columns={"userId": "no_ans"})
    df_count_per_meet['no_ans'] = df_count_per_meet['no_ans'].astype(int)
    ## คำนวน meeting score
    df_sum = pd.DataFrame(df.groupby('meetId')['answer'].agg(['sum', 'mean', 'std'])).reset_index()

    df_sum['meeting_success'] = pd.cut(df_sum['mean'], bins=[0, 1.9, 3.9, 6, 8, 10], labels=['Very bad meeting', 'Rather bad meeting', 'Satisfactory meeting', 'Good meeting', 'Very good meeting'])
    # merge เพื่อหารจำนวนคน
    df_sum_cpm = df_sum.merge(df_count_per_meet, on = 'meetId')
    df_sum_cpm.to_csv(output_ans, index = False)
    print(f"Output to {output_ans} yoyoyoyoyoyo")

def clean_com(output_com):
    df = pd.read_csv(output_com)
    df = df[['meetId', 'comId', 'senId']].rename(columns={'senId': 'sentiment'})
    df.to_csv(output_com, index = False)
    print(f"Output to {output_com} yoyoyoyoyoyo")

def clean_obj(output_obj):
    df = pd.read_csv(output_obj)
    df = df[["folderId", "meetId", "objDes"]]
    df.to_csv(output_obj, index = False)
    print(f"Output to {output_com} yoyoyoyoyoyo")

def merge_data(output_meet, output_ans, output_folders, output_com, output_obj, output_final):
    df_meet = pd.read_csv(output_meet)
    df_sum_cpm = pd.read_csv(output_ans)
    ##
    df_folder = pd.read_csv(output_folders)
    df_com = pd.read_csv(output_com)
    df_obj = pd.read_csv(output_obj)
    
    ## merge meeting - folder
    df_meet_fol = df_meet.merge(df_folder, on = 'folderId', how = 'left')
    ## merge meeting, folder - answer
    df_meet_fol_ans = df_meet_fol.merge(df_sum_cpm, on = 'meetId', how = 'left')
    ## merge meeting, folder, answer - comment
    df_meet_fol_ans_com = df_meet_fol_ans.merge(df_com, on = 'meetId', how = 'left')
    ## merge meeting, folder, answer, comment - objective
    df_meet_fol_ans_com_obj = df_meet_fol_ans_com.merge(df_obj, on = ["folderId", "meetId"], how = 'left')
    df_meet_fol_ans_com_obj = df_meet_fol_ans_com_obj[["folderId", "folderName", "meetId", "meetName", "duration_minutes", "meetStartDate", "dow", "meetTag", "sum", "mean", 'meeting_success', "std", "no_ans", "comId", "sentiment", "objDes"]]
    
    # df_meet_fol_ans_com_obj['no_ans'] = df_meet_fol_ans_com_obj['no_ans'].astype('int32', errors = 'ignore')
    # df_meet_fol_ans_com_obj.loc["no_ans"] = int(0)
    # df_meet_fol_ans_com_obj["no_ans"] = df_meet_fol_ans_com_obj["no_ans"].fillna(0).astype(int)
    df_meet_fol_ans_com_obj = df_meet_fol_ans_com_obj.set_index("folderId").sort_index().sort_values(by = 'meetId')
        
    df_meet_fol_ans_com_obj.to_csv(output_final, index = True)

with DAG(
    "meeting_project492",
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup = False
) as dag:

  t1 = PythonOperator(
    task_id = 'fetch_meeting',
    python_callable = fetch_data,
    op_kwargs = {
       "table": "meeting",
       "output": output_meet
       }
  )

  t2 = PythonOperator(
    task_id = 'fetch_answers',
    python_callable = fetch_data,
    op_kwargs = {
       "table": "answer",
       "output": output_ans
       }
  )
    
  t3 = PythonOperator(
    task_id = 'fetch_folders',
    python_callable = fetch_data,
    op_kwargs = {
        "table": "folders",
        "output": output_folders
    }
  )
    
  t4 = PythonOperator(
    task_id = 'fetch_comment',
    python_callable = fetch_data,
    op_kwargs = {
        "table": "comment",
        "output": output_com
    }
  )

  t5 = PythonOperator(
      task_id = 'fetch_meet_object',
      python_callable = fetch_data,
      op_kwargs = {
          "table": "meetObj",
          "output": output_obj
      }
  )

  t6 = PythonOperator(
    task_id = 'clean_meeting',
    python_callable = clean_meet,
    op_kwargs = {"output_meet": output_meet}
  )

  t7 = PythonOperator(
    task_id = 'clean_answers',
    python_callable = clean_answer,
    op_kwargs = {"output_ans": output_ans}
  )

  t8 = PythonOperator(
    task_id = 'clean_comment',
    python_callable = clean_com,
    op_kwargs = {"output_com": output_com}
  )

  t9 = PythonOperator(
      task_id = 'clean_meet_obj',
      python_callable = clean_obj,
      op_kwargs = {"output_obj": output_obj}
  )

  t10 = PythonOperator(
    task_id = 'merge_data',
    python_callable = merge_data,
    op_kwargs = {
       "output_meet": output_meet,
       "output_ans": output_ans,
       "output_folders": output_folders,
       "output_com": output_com,
       "output_obj": output_obj,
       "output_final" : output_final
       }
  )

  t1 >> t6
  t2 >> t7
  t3
  t4 >> t8
  t5 >> t9
  [ t3, t6, t7, t8, t9 ] >> t10