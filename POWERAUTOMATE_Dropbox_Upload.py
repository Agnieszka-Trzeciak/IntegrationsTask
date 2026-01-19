import dropbox
import json
import random

with open('iris.json','r') as json_file:
    with open('Token.txt','r') as token_file:
        dbx = dropbox.Dropbox(token_file.readline())
        file_name=f"/iris{round(1000*random.random())}.json"
        dbx.files_upload(json.dumps(json.load(json_file)).encode("ascii"),file_name)
        print(f'File {file_name} added to Dropbox.')
