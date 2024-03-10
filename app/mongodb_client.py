from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        self.database = None
        self.collection = None

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
        
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)

    #Mètode per escriure les dades a la DB.
    def set_data(self, data):
        if self.collection:
            result = self.collection.insert_one(data)
            return result.inserted_id
        else:
            raise Exception("No collection selected. Please select a collection using getCollection method.")
    
    #Mètode per obtenir les dades de la DB.
    def get_data(self, query=None):
        if self.collection:
            if query:
                result = self.collection.find(query)
            else:
                result = self.collection.find()
            return [data for data in result]
        else:
            raise Exception("No collection selected. Please select a collection using getCollection method.")
    


