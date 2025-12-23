import sys
from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setAppName("MaxWordsInLine").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    if len(sys.argv) < 2:
        print("Usage: program.py <input_file>")
        sys.exit(1)

    lines = sc.textFile(sys.argv[1])

    counts = lines.map(lambda line: len(line.split()))

    if not counts.isEmpty():
        max_words = counts.max()
        print("--------------------------------------------")
        print("Max number of words: " + str(max_words))
        print("--------------------------------------------")
    else:
        print("Input file is empty")

    sc.stop()

if __name__ == "__main__":
    main()
