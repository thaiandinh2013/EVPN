import time
from DataQualityCheck import *

from minio import Minio
import os

print("Path at terminal when executing this file")
print(os.getcwd() + "\n")
import os

print("This file path, relative to os.getcwd()")
print(__file__ + "\n")

print("This file full path (following symlinks)")
full_path = os.path.realpath(__file__)
print(full_path + "\n")

print("This file directory and name")
path, filename = os.path.split(full_path)
print(path + ' --> ' + filename + "\n")

print("This file directory only")
print(os.path.dirname(full_path))
a =0
data_check = DataQualityCheck()
while a<1:
    print("testkkk-=== ")
    print("fsdffsd")

    client = Minio(
        'host.docker.internal:9000',
        access_key='minio_access_key',
        secret_key='minio_secret_key', secure=False)
    print(client)
    if client.bucket_exists("my-bucket"):
        print("my-bucket exists")
    else:
        print("my-bucket does not exist")
    a+=1
