from pyspark import SparkContext
import sys

if __name__ == "__main__":
    sc = SparkContext("local", "Word Count")
    sc.setLogLevel("ERROR")
    
    if len(sys.argv) < 2:
        print("Usage: word_count.py <file>")
        sys.exit(1)

    lines = sc.textFile(sys.argv[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)
                  
    output = counts.take(10)
    print("-------------------------------------------")
    for (word, count) in output:
        print("%s: %i" % (word, count))
    print("-------------------------------------------")
