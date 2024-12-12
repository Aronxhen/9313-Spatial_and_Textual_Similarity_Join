import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import combinations
import math
import time

class project3:
    def run(self, inputpath, outputpath, d, s):
        start_time = time.time()
        spark = SparkSession.builder.master("local").appName("project3_rdd").getOrCreate()
        sc = spark.sparkContext
        text = sc.textFile(inputpath)
        # text_show = text.collect()
        d = int(d)
        s = float(s)

        # split text
        data = text.map(lambda x: x.split("#"))
        # clean space
        data = data.map(lambda x: (x[0].strip(), x[1].strip(), x[2].strip()))
        # type transformation
        data = data.map(lambda x: (int(x[0]), tuple(map(float, x[1][1:-1].split(","))), tuple(sorted(x[2].split(" ")))))

        # prefix filtering (s)
        data = data.map(lambda x: (x[0], x[1], x[2], len(x[2]) - (len(x[2]) * s) + 1))
        data = data.map(lambda x: (x[2][0:int(x[3])], (x[0], x[1], x[2])))
        data = data.flatMap(lambda x: [(item, x[1]) for item in x[0]])

        # group by key and distinct values
        data = data.groupByKey().mapValues(set)
        filter_data = data.filter(lambda x: len(x) > 1)
        filter_data = data.mapValues(lambda y: sorted(y, key=lambda x: x[0]))

        # combination
        group_data = filter_data.mapValues(lambda x: [((cob[0][0], cob[1][0]), (cob[0][1], cob[1][1], set(cob[0][2]), set(cob[1][2]))) for cob in combinations(x, 2)])
        # calculate distance (d)
        group_data = group_data.mapValues(lambda x: [(it[0], math.sqrt((it[1][0][0] - it[1][1][0])**2 + (it[1][0][1] - it[1][1][1])**2), (it[1][2], it[1][3])) for it in x])
        # filter < d
        group_data = group_data.mapValues(lambda x: [it for it in x if it[1] <= d])
        group_data = group_data.filter(lambda x: (len(x[1]) != 0))
        # calculate similarity (s)
        group_data = group_data.mapValues(lambda x: [(it[0], (it[1], int(len(it[2][0].intersection(it[2][1]))) / int(len(it[2][0].union(it[2][1]))))) for it in x])
        # filter >= s
        group_data = group_data.mapValues(lambda x: [it for it in x if it[1][1] >= s])
        group_data = group_data.filter(lambda x: (len(x[1]) != 0))

        # distinct result
        result_data = group_data.flatMap(lambda x:  x[1])
        result_data = result_data.reduceByKey(lambda x, y: (x[0],x[1]))
        result_data = result_data.sortByKey()

        # result format transofrmation
        result = result_data.map(
            lambda x: f"({x[0][0]},{x[0][1]}):{float(f'{x[1][0]:.6f}'.rstrip('0').rstrip('.'))}, {float(f'{x[1][1]:.6f}'.rstrip('0').rstrip('.'))}"
        )
        # result save
        result.coalesce(1).saveAsTextFile(outputpath)

        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"Total execution time: {elapsed_time:.2f} seconds")

        sc.stop()
if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    

