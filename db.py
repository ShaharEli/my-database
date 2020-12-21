import json
import os
import datetime


class Database():

    def __init__(self,schema):
        self.location = os.path.expanduser("/pyDb"+"/"+schema)
        self.valid_types= {"string": str ,"number": int,"boolean": bool, "date": "date"}
        if not os.path.exists(self.location):
              os.makedirs(self.location)

    def load(self,table_name):
        try:
            with open(self.location+"/"+table_name+".json") as json_file:
                return json.load(json_file)
        except Exception as e:
            print(e)
            return False
    
    
    def loadData(self,table_name):
        try:
            with open(self.location+"/"+table_name+".json") as json_file:
                return json.load(json_file)["data"]
        except:
            return False

    def __loadByParam(self, table_name, params):
        data = self.loadData(table_name)
        result = []
    
        for entry in data:
            match = True
            for k,v in params.items():
                if not entry[k] == v:
                    match = False
                    break
            if match is True:
                result.append(entry)
                
        return result           

    def loadById(self, table_name, id):
        data = self.__loadByParam(table_name, {"id": id})
        if len(data) > 0: 
            return data[0]
        print("Id not found")
        return False

    def checkDataIsValid(self, table,data):
        if not isinstance(data,dict):
            return "Data must be of type dict"
        metadata = table["metadata"]["items"]
        for key in list(data.keys()):
            if key not in list(metadata.keys()):
                print(f'Collumn {key} is not valid, Ignoring...')
                del data[key]
        if len(list(data.keys()))==0:
            return 'No data...'
        for k,v in metadata.items():
            if v["required"] == True:
                if k not in data:
                    return f'Collumn {k} must be supplied'
            if k in data:
                typeToCheck = self.valid_types[v["type"]]
                if typeToCheck == "date":
                    try:
                        datetime.datetime(data[k])
                        data[k] = str(datetime.datetime(data[k]))
                    except:
                        return f'Collumn {k} is not of type date'
                else:
                    if isinstance(data[k],typeToCheck)==False:
                        return f'Collumn {k} is not of type {typeToCheck}'
        table["currentId"]+=1
        data["id"]= table["currentId"]
        data["createdAt"] = str(datetime.datetime.now())
        table["data"].append(data)
        return True
    

    def __write_to_db(self,table_name,data):
        try:
            table = self.load(table_name)
            for entry in data:
                isValid = self.checkDataIsValid(table,entry)
                if  isValid!=True:
                    raise Exception(isValid)
            with open(self.location+"/"+table_name+".json", 'w') as json_file:
                json.dump(table, json_file)
            return True
        except Exception as e:
            print(e)
            return False

    def bulkAdd(self,table_name,data):
        if isinstance(data, list)==False:
            print("data needs to be a list")
            return False
        self.__write_to_db(table_name,data) 

    def add(self,table_name,data):
        self.__write_to_db(table_name,[data])


    def create_table(self, table_name, table):
        try:
            file_name =self.location+"/"+table_name+".json"
            if  os.path.exists(os.path.expanduser(file_name)):
                raise Exception("Table already exists")
            new_table={
                "data":[],
                "currentId":0,
                "metadata" :{"items":{}}
            }
            new_table["metadata"]["createdAt"]=str(datetime.datetime.now())
            if not isinstance(table,dict):
                raise Exception("Table must be of type dict")
            for k,v in table.items():
                if not isinstance(v,dict):
                    raise Exception("Entity must be of type dict")
                if "type" in v:
                    if v["type"] not in list(self.valid_types.keys()):
                        raise Exception("{} is not valid type choose one of the following:\n {}".format(v["type"],"\n".join(self.valid_types.keys())))
                else:
                    raise Exception("property 'type' was not supplied in collumn {}".format(k))
                new_table["metadata"]["items"][k]={"type":v["type"],"required":False}
                if "required" in v:
                    if v["required"]==True:
                        new_table["metadata"]["items"][k]["required"]=True
            with open(file_name, 'w') as json_file:
                json.dump(new_table, json_file)
            print('Table {} Created Succesfully'.format(table_name))
            return True
        except Exception as e:
            print(e)
            return False

mydate = datetime.datetime.now()
hi=Database("lala")
# hi.create_table("zach",{"lovePizza":{"type":"string"}})
print(hi.add("zach",{"flafel":"ya"}))

print(hi.load("zach"))


