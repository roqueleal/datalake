import time
import os
import pandas as pd

print('running the ETL job first time')
job_number=1
os.system("python etl.py")
job_number = job_number +1

while True:
    #ETL 2min
    time.sleep(120)
    print(f'running the ETL job numero {job_number}')
    save_index = pd.read_csv('save_index.csv')
    index_job = job_number -1
    index_number = save_index.loc[save_index['job_nb'] == index_job, 'index_saved'].values[0]
    os.system(f"python etl.py --index_mongo={index_number} --job_number={job_number}")
    job_number = job_number + 1
