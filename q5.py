from pyspark import SparkContext
import sys

if __name__ == "__main__":
    sc = SparkContext("local", "LineWordCount")
    sc.setLogLevel("ERROR")
    
    if len(sys.argv) < 2:
        print("Usage: count_words_in_line.py <file>")
        sys.exit(1)

    lines = sc.textFile(sys.argv[1])
    
    # Map each line to its word count
    counts = lines.map(lambda line: len(line.split()))
    
    if not counts.isEmpty():
        max_words = counts.max()
        print("--------------------------------------------")
        print("Max number of words: " + str(max_words))
        print("--------------------------------------------")
