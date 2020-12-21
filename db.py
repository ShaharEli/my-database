import json
import os
import datetime


class Database():

    def __init__(self,schema):
        self.location = os.path.expanduser("/pyDb"+"/"+schema)
        if not os.path.exists(self.location):
              os.makedirs(self.location)

    def load(self,table):
        try:
            with open(self.location+"/"+table+".json") as f:
                return json.load(f)["data"]
        except:
            return False

    def write(self,schema,data):
        try:
            with open(self.location+"/"+schema+".json", 'a') as json_file:
                json.dump(data, json_file.data)
            return True
        except:
            return False

    def create_table(self, table_name, table):
        try:
            file_name =self.location+"/"+table_name+".json"
            if  os.path.exists(os.path.expanduser(file_name)):
                raise Exception("Table already exists")
            with open(file_name, 'w') as json_file:
                new_table={
                    "data":[],
                    "metadata" :{"items":{}}
                }
                valid_types= ["string","number","boolean","date"]
                new_table["metadata"]["createdAt"]=str(datetime.datetime.now())
                if not isinstance(table,dict):
                    return False
                for k,v in table.items():
                    if not isinstance(v,dict):
                        return False
                    if "type" in v:
                        if v["type"] not in valid_types:
                            return False
                    else:
                        return False
                    new_table["metadata"]["items"][k]={"type":v["type"],"required":False}
                    print(0)    
                    if "required" in v:
                        if v["required"]==True:
                            new_table["metadata"]["items"][k]["required"]=True
                json.dump(new_table, json_file)
            return True
        except Exception as e:
            print(e)
            return False
    
hi=Database("lala")
print(hi.load("my_man"))