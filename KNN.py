#coding:utf-8

#1.计算测试数据与各个训练数据的距离
#2.按距离排序
#3.取距离最小的K个点
#4.统计K个点所在类别出现的频率（次数）
#5.返回K个点中出现频率最高的类别，作为测试数据的预测分类

from numpy import *
import operator

#给出训练数据及其对应的类别
def createDataSet():
    dataSet = array([[1.0,2.0],[1.2,0.1],[0.1,1.4],[0.3,3.5]])
    labels= ['A','A','B','B']
    return dataSet, labels

#计算欧式距离
def calcDistance(input, dataSet):
    dataSize = dataSet.shape[0]
    diff = tile(input, (dataSize,1)) - dataSet
    sqdiff = diff ** 2
    sumdiff = sum(sqdiff, axis=1)
    dist = sumdiff ** 0.5
    return dist

#统计次数
def countLabel(sortedDistIndex, label, k):
    classCount = {}
    for i in range(k):
        voteLabel = label[sortedDistIndex[i]]
        classCount[voteLabel] = classCount.get(voteLabel,0) + 1

    return classCount

#通过KNN分类
def classify(input, dataSet, label, k):
    #计算欧氏距离
    dist = calcDistance(input, dataSet)

    #对距离进行排序
    sortedDistIndex = argsort(dist)

    #统计前K个点所属类别出现的次数
    classCount = countLabel(sortedDistIndex, label, k)

    #出现次数最多的类别作为input的预测分类
    maxCount = 0
    for key,value in classCount.items():
        if value > maxCount:
            maxCount = value
            classes = key
    return classes

if __name__ == '__main__':
    dataSet, labels = createDataSet()
    input = array([1.1,0.3])
    k = 3
    output = classify(input, dataSet, labels, k)
    print("test data:", input, "classifyData:", output)
