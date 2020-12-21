import json
import os
import datetime


class Database():

    def __init__(self,schema):
        self.location = os.path.expanduser("/pyDb"+"/"+schema)
        self.valid_types= {"string": str ,"number": int,"boolean": bool, "date": "date"}
        if not os.path.exists(self.location):
              os.makedirs(self.location)

    # Getting all the table data
    def __load(self,table_name):
        try:
            with open(self.location+"/"+table_name+".json") as json_file:
                return json.load(json_file)
        except Exception as e:
            print(e)
            return False

    # Remove table
    def drop_table(self, table_name):
        try:
            os.remove(self.location+"/"+table_name+".json")
            print(f"{table_name} deleted succesfully")
        except Exception as e:
            print(e)
    
    # Getting all the table data except the metadata
    def load_data(self,table_name):
        try:
            with open(self.location+"/"+table_name+".json") as json_file:
                return json.load(json_file)["data"]
        except:
            return False
            
    # Validates all supplied params match supplied data 
    def __check_by_all_params(self,entry, params):
        valid_ops = {"gt":">","lt":"<","eq":"==", "gte":">=","lte":"<="} # later add gte & lte
        match = True
        for key,value in params.items():
            if key not in entry:
                match = False
                break
            if isinstance(value,dict):
                
                for op,op_value in value.items():
                    if op in valid_ops:
                        match = eval(f"{entry[key]} {valid_ops[op]} {op_value}")
                        if match==False:
                            break
                    else:
                        match = False
                        break
                    
            elif not entry[key] == value:
                match = False
                break
        return match

    # 
            
    # Get by attribuets
    def find_by_params(self, table_name, params, attrs = "all"):
        data = self.load_data(table_name)
        results = []
        for entry in data:
            match = self.__check_by_all_params(entry, params)
            if match == True:
                if isinstance(attrs,list)==False:
                    results.append(entry)
                else: 
                    single_result = {}
                    for attr in attrs:
                        single_result[attr] = entry[attr]
                    results.append(single_result) 
        return results           

    # Get by id
    def find_by_id(self, table_name, id, attrs = "all"):
        data = self.find_by_params(table_name, {"id": id}, attrs)
        if len(data) > 0: 
            return data[0]
        print("Id not found")
        return False

    # Validate user input before sending to db
    def __check_data_is_valid(self, table,data,update=False):
        if not isinstance(data,dict):
            return "Data must be of type dict"
        metadata = table["metadata"]["items"]
        strictVals = ["id", "createdAt", "updatedAt"]
        for key in list(data.keys()):
            if key not in list(metadata.keys()):
                print(f'Column {key} is not valid, Ignoring...')
                del data[key] 
        for k,v in metadata.items():
            if update==False:
                if v["required"] == True:
                    if k not in data:
                        return f'Column {k} must be supplied'
            if len(list(data.keys()))==0:
                return 'No data...'
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
        data["updatedAt"] = str(datetime.datetime.now())
        if  update == False:
            table["currentId"]+=1
            data["id"]= table["currentId"]
            data["createdAt"] = str(datetime.datetime.now())
            table["data"].append(data)
        return True
    
    # Add list data in form of a list
    def __write_to_db(self,table_name,data):
        try:
            table = self.__load(table_name)
            for entry in data:
                isValid = self.__check_data_is_valid(table,entry)
                if  isValid!=True:
                    raise Exception(isValid)
            with open(self.location+"/"+table_name+".json", 'w') as json_file:
                json.dump(table, json_file)
            return True
        except Exception as e:
            print(e)
            return False

    # Add list data in form of a list
    def bulk_add(self,table_name,data):
        if isinstance(data, list)==False:
            print("data needs to be a list")
            return False
        self.__write_to_db(table_name,data) 
        
    def update(self, table_name, params, data):
        table = self.__load(table_name)
        valid = self.__check_data_is_valid(table, data, True)
        if valid != True:
            print(f'{valid} not valid')
            return valid
        tableHasChanged = False
        index=0
        count=0
        for entry in table["data"]:
            match = self.__check_by_all_params(entry, params)
            if match == True:
                count+=1
                tableHasChanged = True
                for k,v in data.items():
                    table["data"][index][k] = v
                
                
            index+=1
        if tableHasChanged == True:
            with open(self.location+"/"+table_name+".json", 'w') as json_file:
                json.dump(table, json_file)
            print(f"{count} updated")
                    

    def add(self,table_name,data):
        if isinstance(data, dict):
            self.__write_to_db(table_name,[data])
        else:
            print("format of data is not supported")
            return False

    def delete_by_params(self, table_name,params):
        try:
            table = self.__load(table_name)
            count=0
            index=0
            for entry in table["data"]:
                match = self.__check_by_all_params(entry,params)
                if match== True:
                    count+=1
                    del table["data"][index]
                    index-=1
                index+=1
            with open(self.location+"/"+table_name+".json", 'w') as json_file:
                json.dump(table, json_file)
            print(f"{count} deleted")
        except Exception as e:
            print(e)


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
# # hi.create_table("zach",{"lovePizza":{"type":"string"}})++++
# # print(hi.bulkAdd("zach",[{"lovePizza":"ya"},{"lovePizza":"5"}]))

# print(hi.loadById("zach",5))
print(
    # hi.delete_by_params("zach",{"lovePizza":"ya"})

    hi.find_by_params("zach", {"id":{"eqx":1}})
)

# hi.drop_table("zach")