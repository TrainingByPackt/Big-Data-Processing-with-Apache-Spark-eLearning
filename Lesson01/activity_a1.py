words = [
    'Apache', 'Spark', 'is', 'an', 'open-source', 'cluster-computing',
    'framework', 'Apache', 'Spark', 'open-source', 'Spark'
]

# Creates a RDD from a list of words
distributed_words = sc.parallelize(words)
distributed_words.count()

# Creates a RDD by excluding words with length lower than 3 characters
filtered_words = distributed_words.filter(lambda x: len(x) > 3)
filtered_words.count()

# Creates a parallelized collection splitted in 8 partitions
distributed_words2 = sc.parallelize(words, 8)
distributed_words2.count()


# Creates a distributed dataset from a local file
local_file = sc.textFile("README.md")
local_file.count()
local_file.filter(lambda x: len(x) > 3).count()
local_file.first()

# creates a sequence of 10000 numbers
# and a RDD based on that sequence
numbers = [x for x in range(-5000, 5000)]
paralellized_numbers = sc.parallelize(numbers)
paralellized_numbers.count()
paralellized_numbers.first()
paralellized_numbers.min()
paralellized_numbers.max()

# creates a new RDD with only even
# numbers from the original dataset
even_numbers = paralellized_numbers.filter(
    lambda x: x % 2 == 0)
even_numbers.count()
even_numbers.first()
even_numbers.min()
even_numbers.max()

# creates a new RDD with only positive
# numbers from the original dataset
positive_numbers = paralellized_numbers.filter(
    lambda x: x >= 0)
positive_numbers.count()
positive_numbers.first()
positive_numbers.min()
positive_numbers.max()


def is_multiple(n):
    """
    returns true if a number is multiple of 3,
    otherwise returns false
    """
    if n % 3 == 0:
        return True
    else:
        return False


# creates a new RDD with numbers multiple of 3
multiple_numbers = paralellized_numbers.filter(
    is_multiple)
multiple_numbers.count()
multiple_numbers.first()
multiple_numbers.min()
multiple_numbers.max()



# creates a RDD from a list of words
words = [
    'Apache', 'Spark', 'is', 'an', 'open-source', 'cluster-computing',
    'framework', 'Apache', 'Spark', 'open-source', 'Spark'
]
distributed_words = sc.parallelize(words)
distributed_words.count()
distributed_words.first()
# applies a transformation that returns words RDD mapped to uppercase
upper_words = distributed_words.map(lambda x: x.upper())
upper_words.count()
upper_words.first()

# applies a reduce action that returns a string with all
# words in the dataset concatenated by commas
distributed_words.reduce(lambda w1, w2:  w1 + ','  + w2)


# simple key value examples
dataset1 = sc.parallelize(
    [('key1', 6), ('key2', 4), ('key7', 5), ('key10', 6)])
dataset2 = sc.parallelize(
    [('key1', 2), ('key3', 7), ('key8', 5), ('key10', 1)])

dataset1.first()
dataset2.first()

dataset1.join(dataset2).take(10)

dataset1.leftOuterJoin(dataset2).take(10)

dataset1.rightOuterJoin(dataset2).take(10)

dataset1.fullOuterJoin(dataset2).take(10)

# intersection  and union
set1 = sc.parallelize([1, 2, 3, 4, 5 ,6])
set2 = sc.parallelize([2, 4, 6, 8, 10])
set1.intersection(set2).collect()
set1.union(set2).collect()

# negative itersection
# cretaing a dataset with elements that are unique to dataset1
outerjoined_ds1 = dataset1.leftOuterJoin(
    dataset2).filter(lambda x: x[1][1] is None).map(lambda x: x[0])
outerjoined_ds1.count()
outerjoined_ds1.take(10)

# cretaing a dataset with elements that are unique to dataset2
outerjoined_ds2 = dataset2.leftOuterJoin(
    dataset1).filter(lambda x: x[1][1] is None).map(lambda x: x[0])
outerjoined_ds2.count()
outerjoined_ds2.take(10)
