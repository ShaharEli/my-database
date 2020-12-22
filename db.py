import json
import os
import datetime
import math
from HashMap import HashMap

class Database():

    def __init__(self,schema):
        self.location = os.path.expanduser("/pyDb"+"/"+schema)
        self.valid_types= {"string": str ,"number": int,"boolean": bool, "date": "date"}
        self.block_size=10
        if not os.path.exists(self.location):
              os.makedirs(self.location)

    # Getting all the table data
    def __load(self,table_name):
        try:
            with open(self.location+"/"+table_name+".json") as json_file:
                table= json.load(json_file)
                try:
                    nextFile = table["nextFile"]
                    while nextFile!=False:
                        with open(self.location+nextFile) as next_file:
                            json_file=json.load(next_file)
                            table["data"]+=json_file["data"]
                            nextFile=json_file["nextFile"]
                except Exception as e:
                    print(e)
                finally:
                    return table
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
    def __load_data(self,table_name):
        try:
            with open(self.location+"/"+table_name+".json") as json_file:
                table =json.load(json_file)
                try:
                    nextFile = table["nextFile"]
                    while nextFile!=False:
                       with open(self.location+nextFile) as next_file:
                            json_file=json.load(next_file)
                            table["data"]+=json_file["data"]
                            nextFile=json_file["nextFile"]
                except Exception as e:
                    print(e)
                finally:
                    return table["data"]
        except:
            return False
            
    # Validates all supplied params match supplied data 
    def __check_by_all_params(self,entry, params):
        valid_ops = {"gt":">","lt":"<","eq":"==", "gte":">=","lte":"<="}
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

            
    # Get by attribuets
    def find_all(self, table_name, params, attrs = "all"):
        data = self.__load_data(table_name)
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
        data = self.find_all(table_name, {"id": id}, attrs)
        if len(data) > 0: 
            return data[0]
        print("Id not found")
        return False

    # Validate user input before sending to db
    def __check_data_is_valid(self, table,data,table_name="",update=False):
        if not isinstance(data,dict):
            return "Data must be of type dict"
        metadata = table["metadata"]["items"]
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
            tableIndex= table["index"]
            if tableIndex!=False:
                tableMap=HashMap(self.location,table_name)
                tableMap.add(data[tableIndex],len(table["data"])-1)
        return True
    
    # Add list data in form of a list
    def __write_to_db(self,table_name,data):
        try:
            table = self.__load(table_name)
            for entry in data:
                isValid = self.__check_data_is_valid(table,entry,table_name)
                if  isValid!=True:
                    raise Exception(isValid)
            currentId = table["currentId"]
            rangeOfFiles=int(math.ceil(currentId/self.block_size))
            if currentId>self.block_size:
                for file_to_add in range(rangeOfFiles):
                    table_to_add=table.copy()
                    size = file_to_add*self.block_size
                    if file_to_add==0:
                        table_to_add["data"]=table_to_add["data"][:self.block_size-1]
                        table_to_add["nextFile"]="/{}ExtraData/{}{}.json".format(table_name,table_name,file_to_add+1)
                        with open(self.location+"/"+table_name+".json", 'w') as json_file:
                            json.dump(table_to_add, json_file)
                    else:
                        del table_to_add["metadata"]
                        del table_to_add["currentId"]
                        del table_to_add["index"]
                        table_to_add["data"]=table_to_add["data"][size-1:self.block_size+size-1]
                        if rangeOfFiles-1>file_to_add:
                            table_to_add["nextFile"]="/{}{}.json".format(table_name,file_to_add+1)
                        else:
                            table_to_add["nextFile"]=False

                        extraDataPath = self.location+"/"+table_name+"ExtraData/"
                        if not os.path.exists(extraDataPath):
                            os.makedirs(extraDataPath)
                        with open(extraDataPath+table_name+str(file_to_add)+".json", 'w') as json_file:
                            json.dump(table_to_add, json_file)
            else:
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
        valid = self.__check_data_is_valid(table, data,"",True)
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
                    

    def add_index(self,table_name,key):
        table=self.__load(table_name)
        tableMap=HashMap(self.location,table_name)
        added= tableMap.bulk_add(table["data"],key)
        if added==True:
            table["index"]=key
            with open(self.location+"/"+table_name+".json", 'w') as json_file:
                json.dump(table, json_file)
            print("index added")
        else:
            print("error adding index")

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


    def join(self,table1_name,table2_name,field1=False, field2=False):
        try:
            table1 = self.__load(table1_name)
            table2 = self.__load(table2_name)
            joinedData=[]
            if table2_name in table1["metadata"]["fks"]:
                field2="id"
                field1= table1["metadata"]["fks"][table2_name]
            if not field1 and not field2:
                for entity1 in table1["data"]:
                    for entity2 in table2["data"]:
                        entity1[table2_name.title()]=entity2
                        joinedData.append(entity1)
                return joinedData
            if field1 not in table1["metadata"]["items"] and field1 != "id":
                return f" can't find field {field1} in table {table1_name}"
            if field2 not in table2["metadata"]["items"] and field2 != "id": 
                return f" can't find field {field2} in table {table2_name}"
            for entity1 in table1["data"]:
                added = False
                for entity2 in table2["data"]:
                    if not added:
                        try:
                            if entity1[field1]==entity2[field2]:
                                entity1[table2_name.title()]=entity2
                                joinedData.append(entity1)
                                added=True
                        except:
                            pass
                if not added:
                    joinedData.append(entity1)
            return joinedData
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
                "nextFile":False,
                "index":False,
                "metadata" :{"items":{},"fks":{}}
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
                if "fk" in v:
                    new_table["metadata"]["items"][k]["type"]="number"
                    print("foreign key must be of type number (supplied {}) - changed accordingly".format(v['type']))
                    new_table["metadata"]["fks"][v["fk"]]=k

            with open(self.location+"/"+ table_name +".json", 'w') as json_file:
                json.dump(new_table, json_file)
            print('Table {} Created Succesfully'.format(table_name))
            return True
        except Exception as e:
            print(e)
            return False

mydate = datetime.datetime.now()
hi=Database("lala")
# hi.create_table("users",{"name":{"type":"string"},"relatedId":{"type":"string","fk": "zach"}})
print(hi.bulk_add("nitzan",[{"lovePizza":"878787efefefefefkjvld;akdjfcpdajcp;"}]))

# print(hi.loadById("zach",5))

# print(
#     # hi.delete_by_params("zach",{"lovePizza":"ya"})
#    hi.join("users","zach")
# )
# print(hi.find_all("nitzan",{}))

# hi.add_index("nitzan","lovePizza")