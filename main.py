from Extraction import Extractor
from pyspark.sql import SparkSession, DataFrame
from Schemas import *
from Transformation import *


def setup(session:SparkSession)->Extractor:
    ext = Extractor("D:\imdb-data", ["title.akas", "name.basics", "title.basics", "title.principals", "title.ratings", "title.episode", "title.crew"], session)
    schemas = {"title.akas":TITLE_AKAS_SCHEMA,
               "name.basics":NAME_BASICS_SCHEMA,
               "title.basics":TITLE_BASICS_SCHEMA,
               "title.principals":TITLE_PRINCIPALS_SCHEMA,
               "title.ratings":TITLE_RATINGS_SCHEMA,
               "title.episode":TITLE_EPISODE_SCHEMA,
               "title.crew":TITLE_CREW_SCHEMA}
    ext.schemas = schemas
    return ext

def save(*tasks):
    for i in range(len(tasks)):
        task: DataFrame = tasks[i]
        path = "task" + str(i) + ".csv"
        task.write.csv(path, header=True)

def main():
    session = SparkSession.builder.appName("PySpark for imdb-project").getOrCreate()
    ext = setup(session)
    ext.extract_all()
    task1: DataFrame = get_all_ukranian(ext)
    task2: DataFrame = get_people_born_in_19th(ext)
    task3: DataFrame = get_all_movies_2h_titles(ext)
    task4: DataFrame = get_people_movies_characters(ext)
    task5: DataFrame = get_adult_movies_top_100(ext)
    task6: DataFrame = get_episodes(ext)
    task7: DataFrame = get_movies_by_each_decade(ext)
    task8: DataFrame = get_movies_by_genre(ext)
    save(task1, task2, task3, task4, task5, task6, task7, task8)
    session.stop()

if __name__ == '__main__':
    main()
