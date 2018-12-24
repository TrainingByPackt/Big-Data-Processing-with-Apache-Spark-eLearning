from pyspark import SparkContext

import urllib.request


def create_text_rdd_from_url(url):
    """
    Downloads content from a url and
    creates a text based RDD
    """
    response = urllib.request.urlopen(url)
    data = response.read().decode('utf-8')
    lines = data.split('\n')
    # creates a RDD for the book
    return sc.parallelize(lines)


import re

def clean_book(book):
    def clean_line(line):
        """
        Remove \ufeff\r characters
        Remove \t \n \r
        Remove additional characters
        """
        return line.replace('\ufeff\r', '').\
            replace('\t', ' ').replace('\n', '').replace('\r', '').\
            replace('(', '').replace(')', '').replace("'", '').\
            replace('"', '').replace(',', ''). replace('.', '').\
            replace('*', '')

    def normalize_tokenize(line):
        """
        Normalize: lowercase
        tokenize: split in tokens
        """
        return re.sub('\s+', ' ', line).strip().lower().split(' ')

    return book.map(lambda x: clean_line(x)).\
        filter(lambda x: x != '').flatMap(normalize_tokenize)


def remove_stop_words(book):
    """
    Simple removal of words length less or equal to 3
    """
    return book.filter(lambda s: len(s) > 3)


def exclude_popular_words(book, n):
    """
    Exclude top n most popular words
    """
    book = book.map(lambda x: (x, 1)).\
        reduceByKey(lambda accumulator, value: accumulator + value)
    cut_frequency = book.takeOrdered(8, key=lambda x: -x[1])[-1][1]
    return book.filter(lambda x: x[1] < cut_frequency)

def report(book1, book2):
    unique_book1 = book1.\
        leftOuterJoin(book2).filter(lambda x: x[1][1] is None).\
        map(lambda x: x[0])

    print(
        "\n{} Words only exist in book {} ".format(
            unique_book1.count(), book1.name()))
    print( "Sample: {}".format(', '.join(unique_book1.take(5))) )

    unique_book2 = book2.\
        leftOuterJoin(book1).filter(lambda x: x[1][1] is None).\
        map(lambda x: x[0])

    print(
        "\n{} Words only exist in book {}".format(
            unique_book2.count(), book2.name()))
    print( "Sample: {}".format(', '.join(unique_book2.take(5))) )

    common_words = book1.join(book2)
    print(
        "\nCommon words in both books: {}".format(common_words.count()))
    print("Sample: {}".format(common_words.take(5)))


def statistics(book):
    import operator
    import statistics
    # average
    print(book.take(2))
    avg =  book.map(lambda x: len(x[0]) ).reduce(operator.add) / book.count()
    print("Stats for book {}".format(book.name()))
    print('Average word length: {}'.format(avg))
    dev = statistics.stdev(
        book.map(
            lambda x: len(x[0])).collect()
    )
    print('Standard Deviation: {}'.format(dev))
    print('Top 5 most frequent words:')
    print(book.takeOrdered(5, key=lambda k: -k[1]))

def process_books():
    tale2cities_url = 'https://raw.githubusercontent.com/maigfrga/spark-streaming-book/'\
    'master/data/books/tale2cities.txt'

    hamlet_url = 'https://raw.githubusercontent.com/maigfrga/spark-streaming-book/'\
        'master/data/books/hamlet.txt'

    tale2cities = clean_book(create_text_rdd_from_url(tale2cities_url))
    tale2cities = remove_stop_words(tale2cities)
    tale2cities = exclude_popular_words(tale2cities, 10)
    tale2cities.setName('A tale of two cities')

    hamlet = clean_book(create_text_rdd_from_url(hamlet_url))
    hamlet = remove_stop_words(hamlet)
    hamlet = exclude_popular_words(hamlet, 10)
    hamlet.setName('hamlet')

    #report(tale2cities, hamlet)
    statistics(tale2cities)
    statistics(hamlet)

def main():
    process_books()


if __name__ == "__main__":
    sc = SparkContext(appName="bookAnalysis")
    sc.setLogLevel("ERROR")
    main()


sc.stop()

