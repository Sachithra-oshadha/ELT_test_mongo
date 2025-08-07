import pymongo
from pymongo.errors import ConfigurationError, OperationFailure

class Database:
    def __init__(self, db_config, logger):
        self.db_config = db_config
        self.logger = logger
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = pymongo.MongoClient(self.db_config['host'], self.db_config['port'])
            self.db = self.client[self.db_config['database']]
            # Test connection
            self.client.admin.command('ping')
            self.logger.info("Successfully connected to MongoDB database")
        except ConfigurationError as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")

    def insert_one(self, collection, document):
        try:
            result = self.db[collection].insert_one(document)
            return result.inserted_id
        except OperationFailure as e:
            self.logger.error(f"Insert one failed: {e}")
            raise

    def insert_many(self, collection, documents):
        try:
            result = self.db[collection].insert_many(documents, ordered=False)
            self.logger.info(f"Inserted {len(result.inserted_ids)} documents into {collection}")
            return len(result.inserted_ids)
        except OperationFailure as e:
            self.logger.error(f"Batch insert failed: {e}")
            raise

    def find_one(self, collection, query):
        try:
            return self.db[collection].find_one(query)
        except OperationFailure as e:
            self.logger.error(f"Find failed: {e}")
            raise