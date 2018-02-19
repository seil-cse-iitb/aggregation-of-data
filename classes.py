import datetime
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import paho.mqtt.client as mqtt_driver
import pm
import time


class UtilsHandler:
    @staticmethod
    def current_timestamp():
        # datetime.datetime.
        return str(datetime.datetime.now())
    @staticmethod
    def str_from_timestamp(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M:%S")

    @staticmethod
    def timestamp_from_str(str):
        return time.mktime(datetime.datetime.strptime(str,
                                                      "%d/%m/%Y %H:%M:%S").timetuple())




class ConfigHandler:
    config_file_path = "./config.json"
    config = None

    @staticmethod
    def init():
        config_fp = open(ConfigHandler.config_file_path, 'r')
        ConfigHandler.config = json.load(config_fp)
        config_fp.close()

    @staticmethod
    def get(key):
        if ConfigHandler.config is None:
            ConfigHandler.init()
        if key not in ConfigHandler.config.keys():
            LogHandler.log_error("[Config]Config not found for key: " + str(key))
        return ConfigHandler.config[key]

    @staticmethod
    def set(key, value):
        if ConfigHandler.config is None:
            ConfigHandler.init()
        ConfigHandler.config[key] = value

    @staticmethod
    def save():
        config_fp = open(ConfigHandler.config_file_path, 'w')
        json.dump(ConfigHandler.config, config_fp)
        config_fp.close()

    @staticmethod
    def set_and_save(key, value):
        ConfigHandler.set(key, value)
        ConfigHandler.save()


class ReportHandler:
    @staticmethod
    def report_error(report_reciever_email, text):
        script_identity_text = ConfigHandler.get("script_identity_text")
        if script_identity_text is None:
            script_identity_text = ""
        ReportHandler.report(report_reciever_email, script_identity_text, "[Error]" + text)

    @staticmethod
    def report_info(report_reciever_email, text):
        script_identity_text = ConfigHandler.get("script_identity_text")
        if script_identity_text is None:
            script_identity_text = ""
        ReportHandler.report(report_reciever_email, script_identity_text, "[Info]" + text)

    @staticmethod
    def report(report_reciever_email, subject, text):
        enable_report = ConfigHandler.get("enable_report")
        if not enable_report:
            return
        report_sender_email = ConfigHandler.get("report_sender_email")
        report_sender_password = ConfigHandler.get("report_sender_password")
        msg = MIMEMultipart()
        msg['From'] = report_sender_email
        msg['To'] = report_reciever_email
        msg['Subject'] = str(subject)
        body = "[" + UtilsHandler.current_timestamp() + "]" + str(text)
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP('imap.cse.iitb.ac.in', 25)
        server.starttls()
        server.login(report_sender_email, report_sender_password)
        msg_text = msg.as_string()
        server.sendmail(report_sender_email, report_reciever_email, msg_text)
        server.quit()


class LogHandler:
    log_file_path = ConfigHandler.get("log_file_path")

    @staticmethod
    def log_info(info):
        LogHandler.log("[Info]" + info)

    @staticmethod
    def log_error(error):
        LogHandler.log("[Error]" + error)
        ReportHandler.report_error(ConfigHandler.get("report_reciever_email"), error)

    @staticmethod
    def log(text):
        print("[" + UtilsHandler.current_timestamp() + "]" + str(text) + "\n")
        log = open(LogHandler.log_file_path, 'a')
        log.write("[" + UtilsHandler.current_timestamp() + "]" + str(text) + "\n")
        log.close()


class MongoHandler:
    db_host = db_name = db_client = db = None

    def __init__(self, db_host, db_name):
        self.db_host = db_host
        self.db_name = db_name
        self.connect()

    def connect(self):
        try:
            self.db_client = pm.MongoClient(self.db_host, 27017)
            self.db = self.db_client[self.db_name]
        except Exception as e:
            LogHandler.log_error("[Mongo](Could not connect to database) " + str(e))
            exit()

    def select(self, collection_name, query, columns):
        result = None
        if columns is not None:
            result = self.db[collection_name].find(query, columns)
        else:
            result = self.db[collection_name].find(query)
        return result


class SQLiteHandler:
    pass


class MQTTHandler:
    mqtt_host = mqtt_client = None
    publish_info = None

    def __init__(self):
        self.mqtt_client = mqtt_driver.Client()
        self.mqtt_host = ConfigHandler.get("mqtt_host")
        self.connect()

    def connect(self):
        self.mqtt_client.connect(self.mqtt_host)
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_connect = self.on_connect

    def on_disconnect(self):
        LogHandler.log_info("[MQTT]Client disconnected.")

    def on_connect(self):
        LogHandler.log_info("[MQTT]Client connected.")

    def publish(self, topic, payload):
        self.publish_info = self.mqtt_client.publish(topic, payload, qos=0, retain=True)

    def wait_for_publish(self):
        if self.publish_info is not None:
            self.publish_info.wait_for_publish()


class Sensor:
    sensor_id = channel = data = schema = None
    ts_fetch_from = ts_fetch_till= None
    mongo_query = None

    def __init__(self, id,channel):
        self.sensor_id = id
        self.channel = channel
        self.schema = ConfigHandler.get("channelwise_schema")[channel]
        self.ts_fetch_from, self.ts_fetch_till  = self.get_fetch_timestamps()
        self.mongo_query ={"$query": {"TS": {"$gt":self.ts_fetch_till, "$lte": self.ts_fetch_from}}, "$orderby": {"TS": -1}}

    def send_data(self, mongo_db, mqtt_handler):
        mongo_db.select(self.sensor_id,self.mongo_query,self.schema)
        #update time till data is sent in sqlite
        pass

    def get_fetch_timestamps(self):
        ts_fetch_from = ""                      #TODO
        records_batch_size_in_seconds = int(ConfigHandler.get("records_batch_size_in_seconds"))
        ts_fetch_till = ts_fetch_from - records_batch_size_in_seconds
        return ts_fetch_from, ts_fetch_till

class Reader:
    mongo_db = None
    mqtt_handler = None
    def __init__(self):
        self.mongo_db = MongoHandler(ConfigHandler.get('mongo_host'), ConfigHandler.get('mongo_db_name'))
        self.mqtt_handler = MQTTHandler()

    def start_reading(self):
        channels = ConfigHandler.get("channels")
        channelwise_collections = ConfigHandler.get("channelwise_collections")
        for channel in channels:
            for collections in channelwise_collections[channel]:
                for sensor_id in collections:
                    sensor = Sensor(sensor_id,channel)
                    sensor.send_data(self.mongo_db,self.mqtt_handler)