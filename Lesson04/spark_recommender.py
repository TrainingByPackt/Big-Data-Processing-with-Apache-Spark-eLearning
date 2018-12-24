from pyspark import SparkContext
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql.types import (
    StructType, StructField, FloatType, StringType, IntegerType
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

import os


class MovieRecommender(object):
    def __init__(self, sc, sqlctx, data_dir=None):
        # spark context
        self.sc = sc

        # sql context
        self.sqlctx = sqlctx

        if data_dir is not None:
            self.data_dir = data_dir
        elif 'SPARK_DATA' in os.environ:
            self.data_dir = os.environ['SPARK_DATA']
        else:
            print('SPARK_DATA directory or variable not set')
            exit(1)
        self.initialize()

    def initialize(self):
        self.load_datasets()
        self.build_model()

    def load_datasets(self):
        """
        Loads movielens dataset from a given location
        """
        reader = DataFrameReader(self.sqlctx)

        fields = [
            StructField('userId', IntegerType(), True),
            StructField('movieId', IntegerType(), True),
            StructField('rating', FloatType(), True),
            StructField('timestamp', StringType(), True),
        ]
        schema = StructType(fields)
        self.ratings = reader.csv(
           os.path.join(self.data_dir, 'ratings.csv'),
           schema=schema,
           header=True, mode="DROPMALFORMED"
        )

    def build_model(self):
        """
        Builds a Collaborative Filtering model for the movielens dataset
        Using alternating least squares
        """
        (self.training, self.test) = self.ratings.randomSplit([0.8, 0.2])

        self.als = ALS(
            maxIter=5, regParam=0.01,
            userCol="userId", itemCol="movieId", ratingCol="rating",
            coldStartStrategy="drop")
        self.model = self.als.fit(self.training)

    def update_model(self, data):
        """
        Recomputes model by appending additional data to the original dataset
        """
        fields = [
            StructField('userId', IntegerType(), True),
            StructField('movieId', IntegerType(), True),
            StructField('rating', FloatType(), True),
            StructField('timestamp', StringType(), True),
        ]
        schema = StructType(fields)

        df = self.sqlctx.createDataFrame(data, schema=schema)
        self.ratings = self.ratings.union(df)
        self.build_model()

    def test_model(self):
        # Evaluate the model by computing the RMSE on the test data
        predictions = self.model.transform(self.test)
        evaluator = RegressionEvaluator(
            metricName="rmse", labelCol="rating", predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        print("Root-mean-square error = " + str(rmse))

    def get_recommendations(self, users, n_items=10):
        return self.model.recommendForUserSubset(users, n_items)


def initialize():
    sc = SparkContext(appName='MovieLens Recommender')
    sc.setLogLevel("DEBUG")
    sql_context = SQLContext(sc)
    recommender = MovieRecommender(sc, sql_context)
    recommender.test_model()
    # get a list of top 3 users
    user_list = [
       (r['userId'],) for r in
       recommender.ratings.select('userId').distinct().limit(3).collect()
    ]

    fields = [
        StructField('userId', IntegerType(), True),
    ]
    schema = StructType(fields)

    users = sql_context.createDataFrame(user_list, schema)
    print(users)

    recommendations = recommender.get_recommendations(users)
    print(recommendations)

if __name__ == '__main__':
    initialize()
