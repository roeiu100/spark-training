import sys
from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setAppName("WordCountRepartition").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    if len(sys.argv) < 2:
        print("Usage: program.py <input_file>")
        sys.exit(1)

    text_file = sc.textFile(sys.argv[1])

    counts = text_file.flatMap(lambda line: line.split(" ")) \
                      .repartition(5) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b) \
                      .filter(lambda x: len(x[0]) > 5)

    output = counts.collect()
    for (word, count) in output:
        print(f"{word}: {count}")

    sc.stop()

if __name__ == "__main__":
    main()
