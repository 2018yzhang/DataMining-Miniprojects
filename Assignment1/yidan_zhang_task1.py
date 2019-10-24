from pyspark import SparkContext
import sys
import json
import time
from operator import add


sc = SparkContext.getOrCreate()
outFile = sys.argv[2]

yelp_reviews = sc.textFile(sys.argv[1])

# A
review_num = yelp_reviews.count()


#print(review_num)
#print(yelp_reviews.take(2))

# B
review_2018 = yelp_reviews.filter(lambda x: '"date":"2018-' in x)

review_2018_num = review_2018.count()

#print(review_2018_num)

# C
dis_review = yelp_reviews.map(lambda x: x.split('"'))

dis_user = dis_review.map(lambda x: x[7])

dis_user_num = dis_user.distinct().count()


print(time.time())
#print(dis_user_num)

# D

top_user = dis_review.map(lambda x: (x[7], 1))
print(time.time())
top_user_reviews = top_user.reduceByKey(add)
print(time.time())

temp_result_user = top_user_reviews.sortBy(lambda x: (-x[1])).take(10)

print(time.time())
#print(result_user)

# E

dis_bus = dis_review.map(lambda x: x[11])

dis_bus_num = dis_bus.distinct().count()



print(time.time())
#print(dis_bus_num)

# F

top_bus = dis_review.map(lambda x: (x[11], 1))
top_bus_reviews = top_bus.reduceByKey(add)

temp_result_bus = top_bus_reviews.sortBy(lambda x: (-x[1])).take(10)


print(time.time())

#print(temp_result_bus.take(10))

data = {'n_review': review_num, 'n_review_2018': review_2018_num, 'n_user': dis_user_num, 'top10_user': temp_result_user, 'n_business': dis_bus_num, 'top10_business': temp_result_bus}
with open(outFile, 'w') as f:
    json.dump(data, f)


