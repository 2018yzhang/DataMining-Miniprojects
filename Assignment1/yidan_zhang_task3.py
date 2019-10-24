from pyspark import SparkContext
import sys
import json
import time
from operator import add


sc = SparkContext.getOrCreate()
yelp_reviews = sc.textFile(sys.argv[1])


yelp_business = sc.textFile(sys.argv[2])


dis_bus_review = yelp_reviews.map(lambda x: x.split(','))

dis_temp = dis_bus_review.map(lambda x: (x[2], x[3]))
num_temp = dis_bus_review.map(lambda x: (x[2], 1))

dis_bus = dis_temp.map(lambda x: (x[0].split('"')[3], float(x[1].split(':')[1])))

bus_star = dis_bus.reduceByKey(add)
review_num_temp = num_temp.reduceByKey(add)

review_num = review_num_temp.map(lambda x: (x[0].split('"')[3], x[1]))


ave_bus = bus_star.join(review_num).mapValues(lambda x: x[0] / x[1])


city_bus_overview = yelp_business.map(lambda x: x.split('"'))

city_bus = city_bus_overview.map(lambda x: (x[3], x[15]))

join_city_bus_ave = city_bus.join(ave_bus).map(lambda x: x[1])



score_city = join_city_bus_ave.reduceByKey(add)

num_city = join_city_bus_ave.map(lambda x: (x[0], 1)).reduceByKey(add)

avg_city = score_city.join(num_city).mapValues(lambda x: round(x[0]/x[1],1))



result_city = avg_city.sortBy(lambda x: (-x[1]))

header = sc.parallelize(['city, starts'])

result = header.union(result_city)

result.coalesce(1).saveAsTextFile(sys.argv[3])

#print(temp_result_user.take(5))



time_m1 = time.time()
print(result_city.take(10))
time_m1_end = time.time()
time1 = time_m1_end-time_m1


time_m2 = time.time()

m2_print = result_city.take(10)

print(m2_print)

time_m2_end = time.time()
time2 = time_m2_end-time_m2

data ={'m1': time1, 'm2': time2, 'explanation': 'The second method took shorter time because it only went through top 10 when it was printing.'}
outFile = sys.argv[4]
with open(outFile, 'w') as f:
    json.dump(data, f)
