
from pyspark.sql.types import *



TITLE_PRINCIPALS_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), False),
        StructField("ordering", IntegerType(), False),
        StructField("nconst", StringType(), False),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ]
)

TITLE_AKAS_SCHEMA = StructType(
    [
        StructField("titleId", StringType(), False),
        StructField("ordering", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), False),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), False),
        StructField("isOriginalTitle", IntegerType(), False)

    ]
)

TITLE_BASICS_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), False),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), False),
        StructField("startYear", IntegerType(), False),
        StructField("endYear", IntegerType(), False),
        StructField("runtimeMinutes", IntegerType(), True),
        StructField("genres", StringType(), True)
    ]
)

TITLE_CREW_SCHEMA = StructType(
    [
        StructField("tconst", IntegerType(), False),
        StructField("directors", StringType(), False),
        StructField("writers", StringType(), False)
    ]
)

TITLE_EPISODE_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), False),
        StructField("parentTconst", StringType(), False),
        StructField("seasonNumber", IntegerType(), False),
        StructField("episodeNumber", IntegerType(), False)
    ]
)

TITLE_RATINGS_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), False),
        StructField("averageRating", FloatType(), False),
        StructField("numVotes", IntegerType(), False)
    ]
)

NAME_BASICS_SCHEMA = StructType(
    [
        StructField("nconst", StringType(), False),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", DateType(), False),
        StructField("deathYear", DateType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True)
    ]
)