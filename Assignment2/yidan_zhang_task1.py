from pyspark import SparkContext
from itertools import combinations
import sys
import time

def aApprio(data, num_basket, support):
    threshold = support * (len(data) / num_basket)

    result = []

    eleList = {}

    singleL = []

    pairList = {}

    # 1
    for basket in data:

        basket.sort()

        for pair in combinations(basket, 2):

            if frozenset(pair) not in pairList:

                pairList[frozenset(pair)] = 1

            else:

                pairList[frozenset(pair)] += 1

        for item in basket:

            if item not in eleList:

                eleList[item] = 1

            else:

                if eleList[item] < threshold:

                    eleList[item] += 1

                    if eleList[item] >= threshold:

                        if (item) not in singleL:
                            singleL.append(item)


    result.append((1, singleL))
    #print(result)
    #print(pairList)

    # 2 pair

    douL = []

    for pair in combinations(singleL, 2):

        # print(pair)

        if frozenset(pair) in pairList:

            if pairList[frozenset(pair)] >= threshold:

                if set(pair) not in douL:
                    douL.append(set(pair))



    result.append((2, douL))
    #print(douL)
    candidate = douL
    #print(result)
    set_size = 3

    while candidate:

        new_candidate = []
        for i in range(len(candidate)):

            for j in range(i + 1, len(candidate)):

                new_size = candidate[i].union(candidate[j])
                #print(new_size)

                if len(new_size) == set_size:

                    if new_size not in new_candidate:
                        new_candidate.append(new_size)


        canList = []

        for cand in new_candidate:

            cand_support = 0

            for basket in data:

                if cand.issubset(basket):

                    cand_support += 1

                    if cand_support >= threshold and cand not in canList:
                        canList.append(cand)

        checkSub = []

        for cand in canList:

            is_frequent = True

            for sub in combinations(cand, set_size - 1):

                if set(sub) not in result[set_size - 2][1]:
                    # print(subset)
                    is_frequent = False

                    break

            if is_frequent and cand not in checkSub:
                # print(cand)
                checkSub.append(cand)

        result.append((set_size, checkSub))

        candidate = checkSub

        set_size += 1

    fresult = []

    count = 1
    for eles in result:
        tuple_list = []
        for ele in eles[1]:
            if count == 1:
                ele = tuple((ele,))
            else:
                ele = tuple(sorted(tuple(ele)))
            tuple_list.append(ele)
        fresult.append((count, tuple_list))
        count += 1
    return fresult


if __name__ == '__main__':

    s_time = time.time()

    sc = SparkContext.getOrCreate()

    caseNum = int(sys.argv[1])

    support = int(sys.argv[2])

    input_file = sys.argv[3]

    output_f = sys.argv[4]

    data = sc.textFile(input_file)
    header = data.first()
    data = data.filter(lambda line: line != header)

    caseData = data


    # phase 1

    if caseNum == 1:

        caseData = data.map(lambda line: line.strip().split(',')).map(lambda x: (x[0], list(x[1:]))).reduceByKey(lambda x, y: x+y)

    elif caseNum == 2:
        caseData = data.map(lambda x: x.split(',')).map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x, y: x+y)


    #print(caseData.take(2))
    basket_values = caseData.values()
    #print(basket_values.take(2))
    #print(basket_values.getNumPartitions())
    numBasket = basket_values.count()
    #print(basket_values.getNumPartitions())

    aap = basket_values.mapPartitions(lambda x: aApprio(list(x), numBasket, support)).reduceByKey(lambda x, y: x + y)
    #print(aap.take(2))

    #grab x[1] and remove duplicate
    aap_clean = aap.map(lambda x: set(x[1])).map(lambda x: list(x))
    #print(aap_clean.take(1))

    #clean empty set
    app_reEmpty = aap_clean.filter(lambda x: len(x) != 0).sortBy(lambda x: len(x[0]))
    #print(aap_clean.filter(lambda x: len(x) != 0).take(1))

    #print(basket_values.mapPartitions(lambda x: aApprio(list(x), numBasket, support)).reduceByKey(lambda x, y: x + y).take(1))
    #print(basket_values.mapPartitions(lambda x: aApprio(list(x), numBasket, support)).reduceByKey(lambda x, y: x + y).map(lambda x: set(x[1])).take(1))
    candidate_itemsets = app_reEmpty.collect()


    #print(candidate_itemsets)

    # check subset

    def check_subset(allData, can_set):

        for candidates in can_set:
            # print(candidates)
            for cand in candidates:
                s = set()
                for tu in cand:
                    s.add(tu)
                for basket in allData:
                    if s.issubset(basket):
                        yield (cand, 1)



    son = basket_values.mapPartitions(lambda x: check_subset(list(x), candidate_itemsets)).reduceByKey(lambda x, y: x + y)
    son_frequent = son.filter(lambda x: x[1] >= support)
    son_clean = son_frequent.map(lambda x: (len(x[0]), [x[0]])).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[0]).map(lambda x:x[1])

    frequent_itemsets = son_clean.collect()

    output_file = open(output_f, 'w')

    output_file.write('Candidates:\n')


    def file_output(file, cand):
        if len(cand[0]) == 1:

            cand.sort()

            single_pair_res = ''

            for ele in cand:
                single_pair_res += '(\'' + ele[0] + '\')' + ','

            file.write(single_pair_res[0:-1])

            file.write('\n')

        else:

            sorted_list = sorted(cand)

            other_pair_res = ''

            for ele in sorted_list:
                other_pair_res += str(tuple(ele)) + ','

            file.write(other_pair_res[0:-1])

            file.write('\n')

        file.write('\n')


    for cand in candidate_itemsets:
        file_output(output_file, cand)

    output_file.write('Frequent Itemsets:\n')

    for fre in frequent_itemsets:
        #cand = fre[1]

        file_output(output_file, fre)

    e_time = time.time()

    d_time = e_time - s_time
    print("Duration: " + str(d_time))







