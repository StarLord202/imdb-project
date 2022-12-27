from Extraction import Extractor
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame, Row
from functools import reduce
from Schemas import *
from pandas import date_range
import findspark
findspark.init()

def get_all_ukranian(ext:Extractor) -> DataFrame:
    spark:SparkSession = ext.session
    df:DataFrame = ext.access("title.akas")
    df.createOrReplaceTempView("titleakas")
    query = """SELECT DISTINCT title  
               FROM titleakas 
               WHERE region='UA'
               ;"""
    res = spark.sql(query)
    print("completed get all ukr")
    return res

def get_people_born_in_19th(ext:Extractor) -> DataFrame:
    spark: SparkSession = ext.session
    df:DataFrame = ext.access("name.basics")
    df.createOrReplaceTempView("namebasics")
    query = """SELECT  primaryName
               FROM namebasics
               WHERE birthYear >= '1800-01-01' AND birthYear <= '1899-12-31'
               ;"""
    res = spark.sql(query)
    print("completed get people born")
    return res

def get_all_movies_2h_titles(ext:Extractor) -> DataFrame:
    spark: SparkSession = ext.session
    df:DataFrame = ext.access("title.basics")
    df.createOrReplaceTempView("titlebasics")
    query = """SELECT  originalTitle
               FROM titlebasics
               WHERE titleType = 'movie' AND runtimeMinutes > 120
               ;"""
    res = spark.sql(query)
    print("completed get movies 2h")
    return res

# def get_adult_movies_per_region(ext:Extractor) -> DataFrame:
#     spark: SparkSession = ext.session
#     df1:DataFrame = ext.access("title.basics")
#     df1.createOrReplaceTempView("titlebasics")
#     df2:DataFrame = ext.access("title.akas")
#     df2.createOrReplaceTempView("titleakas")
#     query1 = """SELECT COUNT(a.titleId) AS ct, a.region
#                FROM titleakas AS a
#                RIGHT JOIN titlebasics AS b ON a.titleId = b.tconst
#                WHERE region IS NOT null AND isAdult = 1
#                GROUP BY a.region
#                ORDER BY ct desc
#                ;"""
#     res1 = spark.sql(query1)
#     return res1

def get_adult_movies_top_100(ext:Extractor) -> DataFrame:
    spark: SparkSession = ext.session
    df1:DataFrame = ext.access("title.basics")
    df1.createOrReplaceTempView("titlebasics")
    df2:DataFrame = ext.access("title.akas")
    df2.createOrReplaceTempView("titleakas")
    df3:DataFrame = ext.access("title.ratings")
    df3.createOrReplaceTempView("titleratings")
    query2 = """SELECT r.region, primaryTitle, averageRating FROM titleakas AS r LEFT JOIN  (
                       SELECT a.primaryTitle, b.averageRating 
                       FROM titlebasics AS a
                       INNER JOIN titleratings as b ON a.tconst = b.tconst
                       ORDER BY b.averageRating DESC
                       LIMIT 100
                       );
             """
    res2 = spark.sql(query2)
    print("completed get 100 adult")
    return res2

def get_episodes(ext:Extractor) -> DataFrame:
    query1 = """SELECT COUNT(a.tconst) AS ct, b.primaryTitle AS seriesTitle
                FROM titleepisode AS a
                LEFT JOIN titlebasics AS b ON a.parentTconst = b.tconst
                GROUP BY b.primaryTitle
                ORDER BY ct DESC
                LIMIT 50
             ;"""
    spark: SparkSession = ext.session
    df1: DataFrame = ext.access("title.episode")
    df2: DataFrame = ext.access("title.basics")
    df1.createOrReplaceTempView("titleepisode")
    df2.createOrReplaceTempView("titlebasics")
    res1 = spark.sql(query1)
    print("completed get episodes")
    return res1

def get_movies_by_genre(ext:Extractor) -> DataFrame:
    spark: SparkSession = ext.session
    df1: DataFrame = ext.access("title.basics")
    df1.createOrReplaceTempView("titlebasics")
    df2: DataFrame = ext.access("title.ratings")
    df2.createOrReplaceTempView("titleratings")
    query1 = """SELECT DISTINCT genres 
               FROM titlebasics
               WHERE NOT CONTAINS(genres, ',')
               ;"""
    query2 = lambda x: f"""SELECT a.primaryTitle AS title
                 FROM titlebasics AS a
                 INNER JOIN titleratings AS b ON a.tconst = b.tconst
                 WHERE CONTAINS(a.genres, '{x}')
                 ORDER BY b.numVotes DESC
                 LIMIT 10
                 ;"""
    res1 = spark.sql(query1).collect()
    genres = [el[0] for el in res1]
    data = []
    for genre in genres:
        res = spark.sql(query2(genre)).collect()
        for el in res:
            title = el[0]
            data.append(Row(title = title, genre = genre))
    res2 = spark.createDataFrame(data)
    print("completed get movies by genre")
    return res2

def get_movies_by_each_decade(ext:Extractor) -> DataFrame:
    spark: SparkSession = ext.session
    df1: DataFrame = ext.access("title.basics")
    df1.createOrReplaceTempView("titlebasics")
    df2: DataFrame = ext.access("title.ratings")
    df2.createOrReplaceTempView("titleratings")
    query1 = """SELECT startYear FROM titlebasics WHERE startYear IS NOT null ORDER BY startYear DESC LIMIT 1;"""
    query2 = """SELECT startYear FROM titlebasics WHERE startYear IS NOT null ORDER BY startYear LIMIT 1;"""
    min_date = spark.sql(query2).collect()[0][0]
    max_date = spark.sql(query1).collect()[0][0]
    min_decade = min_date - min_date % 10
    max_decade = max_date - max_date % 10 + 10
    date_range_ = date_range(start=str(min_decade), end=str(max_decade+10), freq='10y')
    decades = [str(d)[:4] for d in date_range_]
    query3 = lambda x, y: f"""SELECT a.primaryTitle, a.startYear 
                              FROM titlebasics AS a 
                              INNER JOIN titleratings AS b ON a.tconst = b.tconst 
                              WHERE a.startYear IS NOT null  AND a.startYear >= {x} AND a.startYear < {y}
                              ORDER BY b.numVotes DESC
                              LIMIT 10
                              ;"""
    dfs = []
    for i in range(len(decades)-1):
        start = decades[i]
        end = decades[i+1]
        res = spark.sql(query3(start, end))
        dfs.append(res)
    res = reduce(DataFrame.unionAll, dfs).orderBy("startYear")
    print("completed get movies by decade")
    return res













def get_people_movies_characters(ext:Extractor) -> DataFrame:
    query = """ SELECT a.primaryName, c.primaryTitle, b.characters
                FROM namebasics AS a
                RIGHT JOIN titleprincipals as b ON a.nconst = b.nconst
                LEFT JOIN titlebasics as c ON b.tconst = c.tconst
                WHERE b.characters IS NOT null
                ;
                """
    spark: SparkSession = ext.session
    df1: DataFrame = ext.access("title.basics")
    df2: DataFrame = ext.access("title.principals")
    df3: DataFrame = ext.access("name.basics")
    df1.createOrReplaceTempView("titlebasics")
    df2.createOrReplaceTempView("titleprincipals")
    df3.createOrReplaceTempView("namebasics")
    res = spark.sql(query)
    return res

def main():
    session = SparkSession.builder.appName("PySpark for imdb-project").getOrCreate()
    ext = Extractor("D:\imdb-data", ["title.akas", "name.basics", "title.basics", "title.principals", "title.ratings", "title.episode"], session)
    schemas = {"title.akas":TITLE_AKAS_SCHEMA,
               "name.basics":NAME_BASICS_SCHEMA,
               "title.basics":TITLE_BASICS_SCHEMA,
               "title.principals":TITLE_PRINCIPALS_SCHEMA,
               "title.ratings":TITLE_RATINGS_SCHEMA,
               "title.episode":TITLE_EPISODE_SCHEMA}
    ext.schemas = schemas
    ext.extract_all()
    session.stop()


if __name__ == '__main__':
    main()