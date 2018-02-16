from classes import *

mongo = MongoHandler(ConfigHandler.get("mongo_host"),ConfigHandler.get("mongo_db_name"))
