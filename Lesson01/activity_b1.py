sc.setLogLevel("ERROR")
# downloads a plain text version of the book
# "A tale of two cities"
import urllib.request
url = 'https://raw.githubusercontent.com/maigfrga/spark-streaming-book/'\
    'master/data/books/tale2cities.txt'
response = urllib.request.urlopen(url)
data = response.read()
data = data.decode('utf-8')
# data is downloaded as very long string
len(data)

# split book in lines
lines = data.split('\n')
len(lines)

# creates a RDD for the book

book = sc.parallelize(lines)
book.count()
book.first()


def clean_line(line):
    """
    Remove \ufeff\r characters
    Remove \t \n \r
    Remove additional characters
    """
    return line.replace('\ufeff\r', '').replace('\t', ' ').replace('\n', '').\
        replace('\r', '').replace('(', '').replace(')', '').replace("'", '').\
        replace('"', '').replace(',', '').replace('.', '').replace('*', '')

# Remove characters and empty lines
cleaned_book = book.map(
    lambda x: clean_line(x)).filter(lambda x: x != '')
cleaned_book.count()
cleaned_book.first()

# 3. normalize and tokenize

import re

def normalize_tokenize(line):
    """
    Normalize: lowercase
    tokenize: split in tokens
    """
    return re.sub('\s+', ' ', line).strip().lower().split(' ')


tokens = cleaned_book.flatMap(normalize_tokenize)
tokens.count()
tokens.first()

# 4. remove stop words
reduced_tokens = tokens.filter(lambda s: len(s) > 3)
reduced_tokens.count()
reduced_tokens.first()

# 5. Count words frequency
counts = reduced_tokens.map(lambda x: (x, 1))
counts.count()
counts.first()
reduced_counts = counts.reduceByKey(
    lambda accumulator, value: accumulator + value)
reduced_counts.take(4)

# ordered by natural key (word)
reduced_counts.takeOrdered(4)

# ordered by frequency
reduced_counts.takeOrdered(4, key=lambda x: x[1])

# reverse order by frequency
reduced_counts.takeOrdered(8, key=lambda x: -x[1])

# 6 exclude top n words with top high frequency but meaningless
twocities_book = reduced_counts.filter(lambda x: x[1] < 500)
twocities_book.takeOrdered(8, key=lambda x: -x[1])



# 7 download Hamlet from Shakespeare

import urllib.request
hamlet_url = 'https://raw.githubusercontent.com/maigfrga/spark-streaming-book/'\
    'master/data/books/hamlet.txt'
response = urllib.request.urlopen(hamlet_url)
data = response.read().decode('utf-8').split('\n')

# Creates a RDD for the book
# Removes characters, empty lines
# Tokenize
# Removes stop words
# Counts frequency

shakespeare_book = sc.parallelize(data).\
    map(lambda x: clean_line(x)).\
    filter(lambda x: x != '').\
    flatMap(normalize_tokenize).\
    filter(lambda s: len(s) > 3).\
    map(lambda x: (x, 1)).\
    reduceByKey(
        lambda accumulator, value: accumulator + value)

shakespeare_book.count()
shakespeare_book.first()

# ordered by frequency
shakespeare_book.takeOrdered(4, key=lambda x: x[1])

# reverse order by frequency
shakespeare_book.takeOrdered(8, key=lambda x: -x[1])

hamlet_book = shakespeare_book.filter(lambda x: x[1] < 140)
hamlet_book.takeOrdered(8, key=lambda x: -x[1])



# 8 Perform join operation to find out what words are used
# in both books

common_words = twocities_book.join(hamlet_book)
common_words.count()

# ordering by word
common_words.takeOrdered(8)
common_words.takeOrdered(8, key=lambda x: -1 * x[0])

# ordering by the sum of the frequencies in both books
common_words.takeOrdered(8, key=lambda x: x[1][0] + x[1][1])
common_words.takeOrdered(8, key=lambda x: -1 * (x[1][0] + x[1][1]))


# words that are unique to twocities_book 
unique_twocities_book = twocities_book.\
    leftOuterJoin(hamlet_book).filter(lambda x: x[1][1] is None).\
    map(lambda x: x[0])

unique_twocities_book.count()
unique_twocities_book.take(6)

# words that are unique to hamlet_book
unique_hamlet_book = hamlet_book.\
    leftOuterJoin(twocities_book).filter(lambda x: x[1][1] is None).\
    map(lambda x: x[0])

unique_hamlet_book.count()
unique_hamlet_book.take(6)
