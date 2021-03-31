from datetime import datetime
import  pandas as pd
from minio import Minio
from sqlalchemy import create_engine
from DataQualityCheck import  *
import sys
def from_pandas_to_postgrest(pandas_df, table_name):
    db_connection_url = "postgresql://postgres:supersecretpassword@host.docker.internal:5432/postgres"
    engine = create_engine(db_connection_url)
    pandas_df.to_sql(table_name,engine, if_exists= 'append',method='multi')
def make_pandas(good_lines):
    drivers = []
    legs = []
    for line in good_lines:
        job_id = line['job_id']
        driver_id = line['driver_id']
        start_time=line['start_time']
        start_state= line['start_state']
        lat_lon = line['start_coordinate'].strip('][').split(",")
        start_coordinate_lat = int(lat_lon[0])
        start_coordinate_lon= int(lat_lon[1])
        drivers.append([job_id,driver_id,start_time,start_state,start_coordinate_lon,start_coordinate_lat,str(datetime.now())])
        for i in range(0,20):
            if "leg_" + str(i) in line:
                temp = []
                temp.append(job_id)
                temp.append(driver_id)
                temp.append("leg_" + str(i))
                temp.append(line["leg_" + str(i)]['arrive_time'])
                temp.append(line["leg_" + str(i)]['type'])
                temp.append(line["leg_" + str(i)]['leave_time'])
                # print(line["leg_" + str(i)]['coordinate'])
                lat_lon_leg = line["leg_" + str(i)]['coordinate'].strip('][').split(",")
                temp.append(int(lat_lon_leg[0]))
                temp.append(int(lat_lon_leg[1]))
                temp.append(str(datetime.now()))
                # print(temp)
                legs.append(temp)
    drivers = pd.DataFrame(drivers)
    drivers.columns = ['job_id', 'driver_id' , 'start_time','start_state','start_coordinate_lat','start_coordinate_lon', 'timestamp']
    legs = pd.DataFrame(legs)
    legs.columns = ['job_id','driver_id', 'leg_num','arrive_time','type','leave_time','leg_lat' , 'leg_lon','timestamp']
    from_pandas_to_postgrest(drivers, "driver")
    from_pandas_to_postgrest(legs, "leg")
def proces_data(access_key='minio_access_key',secret_key='minio_secret_key'):
    client = Minio(
        'host.docker.internal:9000',
        access_key=access_key,
        secret_key=secret_key,secure=False)
    required_buckets = ["my-bucket", "good" ,"bad"]
    for bucket in required_buckets:
        if client.bucket_exists(bucket):
            print(bucket + " exists")
        else:
            client.make_bucket(bucket)

    objects = client.list_objects("my-bucket", recursive=True)
    data_check = DataQualityCheck()

    for obj in objects:
        # print(obj.__dict__)
        obj_name = obj.__dict__['_object_name']
        response = client.get_object("my-bucket", obj.__dict__['_object_name'])
        client.fget_object("my-bucket", obj_name, obj_name)

        good_lines = []
        bad_lines = []
        with open(obj_name) as f:
            for line in f:
                line = str(line)
                if data_check.is_valid_json(line):
                    line = data_check.quality_check(line)
                else:
                    bad_lines.append(line)
                    continue
                if len(line['error']) ==0:
                    good_lines.append(line)
                else:
                    bad_lines.append(line)
        badline_file = 'bad_line' + str(datetime.now()) + '.txt'
        with open(badline_file, 'w') as filehandle:
            for bad in bad_lines:
                filehandle.write('%s\n' % bad)
        goodline_file = 'good_line' + str(datetime.now()) + '.txt'
        with open(goodline_file, 'w') as filehandle:
            for good in goodline_file:
                filehandle.write('%s\n' % good)
        print(good_lines)
        if len(good_lines) >0:
            make_pandas(good_lines)
        client.fput_object("bad", badline_file, badline_file,)
        client.fput_object("good", goodline_file, goodline_file, )
        client.remove_object("my-bucket", obj_name)

if __name__ == "__main__" :
    while True:
        proces_data()
        time.sleep(10)



