import urllib.request
import os


URLS = (
    'https://raw.githubusercontent.com/maigfrga/nt-recommend/master/data/db/ml-latest-small/tags.csv',  # noqa
    'https://raw.githubusercontent.com/maigfrga/nt-recommend/master/data/db/ml-latest-small/ratings.csv',  # noqa
    'https://raw.githubusercontent.com/maigfrga/nt-recommend/master/data/db/ml-latest-small/movies.csv'  # noqa,

)


def download_dataset():
    """
    Download the reduced version of movieles dataset
    """

    def download(url):
        response = urllib.request.urlopen(url)
        data = response.read()
        data = data.decode('utf-8')
        fname = url.split('/')[-1]
        with open(
            os.path.join(os.environ['SPARK_DATA'], fname), 'w') as f:
            f.write(data)


    for url in URLS:
        download(url)


fname = os.path.join(
    os.environ['SPARK_DATA'], 'movies.csv'
)
movies = spark.read.csv(
    fname, header=True, mode="DROPMALFORMED"
)

movies
movies.count()
movies.head()
movies.head(n=5)
