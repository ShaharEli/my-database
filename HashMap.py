import json
import os

class HashMap:
        def __init__(self,location,table_name):
            self.location=location+"/"+table_name+"ExtraData"
            if not os.path.exists(self.location):
                os.makedirs(self.location)
            self.file_path = os.path.expanduser(self.location+"/"+table_name+"Index"+".json")
            self.block_size=10
            self.salt = 7
            self.map = {}
            try:
                with open(self.file_path) as json_file:
                    self.map= json.load(json_file)
            except:
                pass

        def _get_hash(self, key):
            hash = 0
            for char in str(key):
                    hash += ord(char)
            return hash % self.salt
        def bulk_add(self,data,key):
            try:
                index=0
                for item in data:
                    if key in item:
                        self.add(item[key],index)
                    index+=1
                return True
            except Exception as e:
                print(e)
                return False
		
        def add(self, key, value):
            key_hash = self._get_hash(key)
            if key_hash not in self.map:
                self.map[key_hash]=value
                with open(self.file_path, 'w') as json_file:
                    json.dump(self.map, json_file)
			
        def get(self, key):
            key_hash = self._get_hash(key)
            return self.map[key_hash]
			
        def delete(self, key):
            key_hash = self._get_hash(key)
            index=self.map[key_hash]
            del self.map[key_hash]
            for k,v in self.map.items():
                if v>index:
                    self.map[k]=v-1
            with open(self.file_path, 'w') as json_file:
                json.dump(self.map, json_file)
                
