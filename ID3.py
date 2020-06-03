#coding:utf-8

#核心是一个贪心算法
    #a.计算数据集的信息熵
    #b.按各特征属性划分数据集
    #c.计算划分之后的数据集的样本熵
    #d.信息增益 = 信息熵 - 样本熵
    #e.返回信息增益最大的特征

#1.构造决策树
    #a.递归结束条件（1：所有特征属性划分完毕（即：当前数据集只剩类别属性这一列） or 2:当前数据集所有样本均属同一类别, 不需要继续划分了）
    #b.选择信息增益最大的特征属性
    #c.循环该特征属性的各个取值
    #d.划分得到子数据集、子特征属性列表
    #e.递归

#2.使用matplotlib 注解绘制树形图

#3.测试算法

from math import log
import operator
import matplotlib.pyplot as plt

def createDataSet():
    #outlook: 0 rain    1 overcast  2 sunny
    #tem:     0 coll    1 mild      2 hot
    #hum:     0 normal  1 high
    #windy:   0 not     1 medium    2 very
    dataSet = [[1, 2, 1, 0, 'no'],
               [1, 2, 1, 2, 'no'],
               [1, 2, 1, 1, 'no'],
               [2, 2, 1, 0, 'yes'],
               [2, 2, 1, 1, 'yes'],
               [0, 1, 1, 0, 'no'],
               [0, 1, 1, 1, 'no'],
               [0, 2, 0, 0, 'yes'],
               [0, 0, 0, 1, 'no'],
               [0, 2, 0, 2, 'no'],
               [2, 0, 0, 2, 'yes'],
               [2, 0, 0, 1, 'yes'],
               [1, 1, 1, 0, 'no'],
               [1, 1, 1, 1, 'no'],
               [1, 0, 0, 0, 'yes'],
               [1, 0, 0, 1, 'yes'],
               [0, 1, 0, 0, 'no'],
               [0, 1, 0, 1, 'no'],
               [1, 1, 0, 1, 'yes'],
               [1, 2, 0, 2, 'yes'],
               [2, 1, 1, 2, 'yes'],
               [2, 1, 1, 1, 'yes'],
               [2, 2, 0, 0, 'yes'],
               [0, 1, 1, 2, 'no'],]
    return dataSet

#计算数据集的信息熵
def calcShang(dataSet):
    #数据集长度
    num = len(dataSet)
    #统计各类别个数
    labelCount = {}
    for data in dataSet:
        currentLabel = data[-1]
        labelCount[currentLabel] = labelCount.get(currentLabel, 0) + 1
    #计算信息熵
    shannonEnt = 0.0
    for key, value in labelCount.items():
        prob = float(value) / num
        shannonEnt -= prob * log(prob, 2)
    return shannonEnt

#按第i个特征属性划分数据集
def splitDataSet(dataSet, i, value): #传入参数：数据集, 第i个特征, 需要的该特征的值
    retDataSet = []
    for data in dataSet:
        if data[i] == value:
            dataNew = data[:i]         #去掉第i个特征数据
            dataNew.extend(data[i+1:])
            retDataSet.append(dataNew)
    return retDataSet

#选取信息增益最大的特征属性
def chooseBestFeature(dataSet):
    baseEnt = calcShang(dataSet)     #数据集的香农熵
    featureNum = len(dataSet[0]) - 1 #特征属性个数
    bestInfoGain = 0.0
    bestFeature = -1                 #属性位置pos
    for i in range(0, featureNum):
        newEnt = 0.0
        featureList = [example[i] for example in dataSet] #第i个特征属性的所有取值
        uniqueVals = set(featureList)                     #去重
        for value in uniqueVals:                          #按该特征属性的各个值划分数据集
            dataSetNew = splitDataSet(dataSet, i, value)
            prob = float(len(dataSetNew)) / len(dataSet)
            newEnt += prob * calcShang(dataSetNew)        #计算样本熵
        InfoGain = baseEnt - newEnt                       #计算信息增益
        if InfoGain > bestInfoGain:
            bestInfoGain = InfoGain
            bestFeature = i
    return bestFeature

#数据集已经处理了所有属性, 但叶子节点的类别依然不是唯一的，用多数表决进行分类
def majorityCnt(classList):
    classCount = {}
    for voteClass in classList:
        classCount[voteClass] = classCount.get(classCount, 0) + 1
    maxCount = 0
    for key, value in classCount.items():
        if value > maxCount:
            maxCount = value
            classes = key
    return classes

#递归创建树
def createTree(dataSet, featureLabels):
    #递归结束的条件：
    classList = [example[-1] for example in dataSet]
    #1.所有特征属性划分完毕, 数据集只剩类别那一列了, 用多数表决对该叶子节点进行分类
    if len(dataSet[0]) == 1:
        return majorityCnt(classList)
    #2.或者当前数据集所有样本属于同一类
    if classList.count(classList[0]) == len(classList):
        return classList[0]

    #选择最优划分方式
    bestFeature = chooseBestFeature(dataSet)
    bestFeatureLabel = featureLabels[bestFeature]
    myTree = {bestFeatureLabel:{}}
    del(featureLabels[bestFeature]) #从属性标签列表中删掉该属性

    featureVals = [example[bestFeature] for example in dataSet]
    uniqueValue = set(featureVals)
    for value in uniqueValue:
        subDataSet = splitDataSet(dataSet, bestFeature, value)
        subFeatureLabels = featureLabels[:]
        myTree[bestFeatureLabel][value] = createTree(subDataSet, subFeatureLabels)
    return myTree

#使用决策树分类
def classify(inputTree, featureLabels, testData): #传入参数：决策树、属性标签列表、待分类数据
    #递归结束条件：当前树没有分支了
    if type(inputTree).__name__ != 'dict':
        return inputTree

    firstStr = list(inputTree.keys())[0]
    subDict = inputTree[firstStr]
    featureIndex = featureLabels.index(firstStr)
    for key in subDict.keys():
        if testData[featureIndex] == key:
            classLabel = classify(subDict[key], featureLabels, testData)

    return classLabel

if __name__ == '__main__':
    dataSet = createDataSet()
    featureLabels = ['outlook', 'tem', 'hum', 'windy']
    featureLabelsForCreateTree = featureLabels[:]
    Tree = createTree(dataSet, featureLabelsForCreateTree)
    testData = [2, 2, 1, 0]
    classes = classify(Tree, featureLabels, testData)
    print (classes)