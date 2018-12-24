import sys

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="checkPythonVersion")
    sc.setLogLevel("ERROR")
    print(
        "Python version: {}\nSpark context version: {}".format(
            sys.version, sc.version))

sc.stop()
