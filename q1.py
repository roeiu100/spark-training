
counts = text_file.flatMap(lambda line: line.split(" ")) \
              .repartition(5) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a + b) \
              .filter(lambda x: len(x[0])>5)
