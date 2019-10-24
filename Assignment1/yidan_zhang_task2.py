from pyspark import SparkContext
import sys
import json
import time
from operator import add

# Task1 F

numPartitions = sys.argv[3]
sc = SparkContext.getOrCreate()
outFile = sys.argv[2]

yelp_reviews = sc.textFile(sys.argv[1])

part_num_default = yelp_reviews.getNumPartitions()

#print(part_num_default)

def countNum(iterator):
    yield sum(1 for _ in iterator)

numOfItems_default = yelp_reviews.mapPartitions(countNum).collect()


start_time_default = time.time()
dis_bus_review = yelp_reviews.map(lambda x: x.split('"'))

top_bus = dis_bus_review.map(lambda x: (x[11], 1))
top_bus_reviews = top_bus.reduceByKey(add)

temp_result_bus = top_bus_reviews.sortBy(lambda x: (-x[1]))


temp_result_bus = top_bus_reviews.sortBy(lambda x: (-x[1]))


end_time_default = time.time()
time_default = end_time_default - start_time_default



#########

yelp_reviews1 = yelp_reviews.coalesce(int(numPartitions))

part_num_cus = yelp_reviews1.getNumPartitions()



numOfItems_cus = yelp_reviews1.mapPartitions(countNum).collect()

start_time_cus = time.time()

dis_bus_review1 = yelp_reviews1.map(lambda x: x.split('"'))

top_bus1 = dis_bus_review1.map(lambda x: (x[11], 1))
top_bus_reviews1 = top_bus1.reduceByKey(add)

temp_result_bus1 = top_bus_reviews1.sortBy(lambda x: (-x[1]))

end_time_cus = time.time()

time_cus = end_time_cus - start_time_cus


data = {'default':{'n_partition': part_num_default, 'n_items': numOfItems_default, 'exe_time': time_default}, 'customized':{'n_partition': part_num_cus, 'n_items': numOfItems_cus, 'exe_time': time_cus}, 'explanation':'The execution time is longer if the number of partition is small. However, when the number of partitions is too large, the execution time will be longer as well.'}
with open(outFile, 'w') as f:
    json.dump(data, f)
