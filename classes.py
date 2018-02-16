import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pm
from django.db.models.functions import datetime


class UtilsHandler:
    @staticmethod
    def current_timestamp():
        # datetime.datetime.
        return str(datetime.datetime.now())


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


class Row:
    data = None

    def __init__(self):
        self.data = {}
        pass

    def __init__(self, data):
        self.data = data
