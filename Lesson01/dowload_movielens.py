import urllib.request
import os

URLS = (
    'https://raw.githubusercontent.com/maigfrga/spark-streaming-book/master/data/movielens/tags.csv',  # noqa
    'https://raw.githubusercontent.com/maigfrga/spark-streaming-book/master/data/movielens/ratings.csv',  # noqa
    'https://raw.githubusercontent.com/maigfrga/spark-streaming-book/master/data/movielens/movies.csv'  # noqa,
)


def main():
    """
    Download the reduced version of movieles dataset
    """

    def download(url):
        response = urllib.request.urlopen(url)
        data = response.read()
        data = data.decode('utf-8')
        fname = url.split('/')[-1]
        with open(os.path.join(
                os.environ['SPARK_DATA'], fname), 'w') as f:
                f.write(data)

    for url in URLS:
        download(url)

if __name__ == '__main__':
    if 'SPARK_DATA' not in os.environ:
        print('Error. Please define SPARK_DATA variable')
        exit(1)

    main()
