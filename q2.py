import sys
from pyspark import SparkConf, SparkContext

def main():
    conf = SparkConf().setAppName("DistinctCountCache").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    if len(sys.argv) < 2:
        print("Usage: program.py <input_file>")
        sys.exit(1)

    text_file = sc.textFile(sys.argv[1]) 
    text_file.cache()

    distinct_count = text_file.flatMap(lambda line: line.split(" ")) \
                              .distinct() \
                              .count()

    print("--------------------------------------------")
    print("Total distinct words: " + str(distinct_count))
    print("--------------------------------------------")

    sc.stop()

if __name__ == "__main__":
    main()
