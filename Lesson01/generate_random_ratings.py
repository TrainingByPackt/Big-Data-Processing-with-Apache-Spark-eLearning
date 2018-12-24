import random
import csv
import os

def main():
    ratings = [1, 2, 3, 4, 5]
    movies = [x for x in range(100)]
    fname = os.path.join(os.environ['SPARK_DATA'], 'act_1_lesson_1.csv')
    with open(fname, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for x in range(1000):
            row = [
                random.choice(movies),
                random.choice(ratings)
            ]
            writer.writerow(row)


if __name__ == '__main__':
    if 'SPARK_DATA' not in os.environ:
        print('Error. Please define SPARK_DATA variable')
        exit(1)

    main()
