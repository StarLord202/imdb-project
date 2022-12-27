from pyspark.sql import SparkSession

class Extractor:
    def __init__(self, location:str, datasets_names:list, spark_session:SparkSession, type="tsv"):
        self.__location = location
        self.__datasets_names = datasets_names
        self.__type = type
        self.__datasets = {}
        self.__session = spark_session
        self.__schemas = {}

    def __extract_file(self, path:str, schema):
        file = self.__session.read.csv(path, sep=r'\t', header=True, nullValue='\\N', schema=schema)
        return file

    @property
    def schemas(self):
        return self.__schemas

    @property
    def session(self):
        return self.__session

    @schemas.setter
    def schemas(self, schems):
        self.__schemas = schems

    def extract_all(self):
        for name in self.__datasets_names:
            path = self.__location + "/" + name + "." + self.__type
            schema = self.__schemas[name]
            df = self.__extract_file(path, schema)
            self.__datasets[name] = df

    def extract(self, name):
        path = self.__location + "/" + name + "." + self.__type
        schema = self.__schemas[name]
        df = self.__extract_file(path, schema)
        self.__datasets[name] = df

    def access_all(self):
        return self.__datasets


    def access(self, dataset_name):
        return self.__datasets[dataset_name]


