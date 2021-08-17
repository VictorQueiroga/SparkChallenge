import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","PySpark Exemplo - Desafio Dataproc")
    words = sc.textFile("{Seu bucket}/livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (len(word), 1)).reduceByKey(lambda a,b:a+b).sortBy(lambda a:a[1], ascending=False)
    wordCounts.saveAsTextFile("{Seu bucket}/resultado")