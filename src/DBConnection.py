import configparser
import psycopg2


class DBConnection:
    def __init__(self):
        self._config = self._get_config()
        self._HOST = self._config.get("POSTGRES", "HOST")
        self._DBNAME = self._config.get("POSTGRES", "DBNAME")
        self._USER = self._config.get("POSTGRES", "USER")
        self._PASSWORD = self._config.get("POSTGRES", "PASSWORD")

    def _get_config(self):
        config = configparser.ConfigParser()
        config.read_file(open(r'C:\Users\gokay\Documents\GitHub\Spotify-Recommender\recommender\src\access.cfg'))
        return config

    def get_connector(self):
        try:
            conn = psycopg2.connect(
                f"dbname={self._DBNAME} user={self._USER} host={self._HOST} password={self._PASSWORD}")
            return conn
        except Exception as e:
            print("Connection Error: " + str(e))
