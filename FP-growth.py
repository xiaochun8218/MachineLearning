# coding:utf-8

from collections import defaultdict
import time
import guppy as hpy
import psutil
import os
import pymysql
from memory_profiler import profile


def getSymbolDataFormDb(gpcodeList, startDate, endDate, symbolList):
    charset = "utf8"
    srcHost = "172.16.8.110"
    srcUse = "root"
    srcPasswd = "123456"
    srcDb = "quant"

    srcDbConn = pymysql.connect(srcHost, srcUse, srcPasswd, srcDb, charset=charset)
    srcCursor = srcDbConn.cursor(cursorclass=pymysql.cursors.DictCursor)

    s1 = []
    s2 = []
    s3 = []
    count = 0
    for gpcode in gpcodeList:
        print(gpcode)
        count += 1
        sql = "SELECT f1 FROM {0} where gpcode = '{1}' and ymd >= {2} and ymd <= {3}".format("quant_stk_calc_d_B_T1",
                                                                                             gpcode, startDate, endDate)
        num = srcCursor.execute(sql)
        data = srcCursor.fetchall()
        for row in data:
            if row["f1"] is not None:
                symbol = ""
                if row["f1"] == 1:
                    symbol = "A"
                elif row["f1"] == 2:
                    symbol = "B"
                elif row["f1"] == 3:
                    symbol = "C"
                elif row["f1"] == 4:
                    symbol = "D"
                if symbol != "":
                    item = gpcode + "_" + symbol
                    if count == 1:
                        s1.append(item)
                    elif count == 2:
                        s2.append(item)
                    elif count == 3:
                        s3.append(item)

    symbolList.append(s1)
    symbolList.append(s2)
    symbolList.append(s3)


@profile
def loadDataSet():
    gpcodeList = ["SH600775", "SZ002547", "SZ300299"]
    startDate = 20181201
    endDate = 20181228
    symbolList = []
    getSymbolDataFormDb(gpcodeList, startDate, endDate, symbolList)
    # 得到3个股票数据集之后,生成事务内数据集
    swn_set = []
    if len(symbolList) > 0:
        for i in range(0, len(symbolList[0])):
            # print (symbolList[0][i])
            items = []
            for j in range(0, len(symbolList)):
                items.append(symbolList[j][i])
            swn_set.append(items)

    # 根据事务内数据集生成跨事务数据集
    dataSet = []
    for i in range(0, len(swn_set) - 1):
        # print (swn_set[i])
        items_kd = []
        for item in swn_set[i]:
            item_0 = item + "_" + "0"
            items_kd.append(item_0)
        for item in swn_set[i + 1]:
            item_1 = item + "_" + "1"
            items_kd.append(item_1)
        dataSet.append(items_kd)

    #     dataSet = [['bread', 'milk', 'vegetable', 'fruit', 'eggs'],
    #                ['noodle', 'beef', 'pork', 'water', 'socks', 'gloves', 'shoes', 'rice'],
    #                ['socks', 'gloves'],
    #                ['bread', 'milk', 'shoes', 'socks', 'eggs'],
    #                ['socks', 'shoes', 'sweater', 'cap', 'milk', 'vegetable', 'gloves'],
    #                ['eggs', 'bread', 'milk', 'fish', 'crab', 'shrimp', 'rice']]

    return dataSet


def transfer2FrozenDataSet(dataSet):
    frozenDataSet = {}
    for elem in dataSet:
        frozenDataSet[frozenset(elem)] = 1
    return frozenDataSet


class TreeNode:
    def __init__(self, nodeName, count, nodeParent):
        self.nodeName = nodeName
        self.count = count
        self.nodeParent = nodeParent
        self.nextSimilarItem = None
        self.children = {}

    def increaseC(self, count):
        self.count += count


def createFPTree(headPointItem, frozenDataSet, minSupport, array_in):
    # scan dataset at the first time, filter out items which are less than minSupport
    headPointTable = {}

    if len(array_in) == 0:
        for items in frozenDataSet:
            for item in items:
                headPointTable[item] = headPointTable.get(item, 0) + frozenDataSet[items]
        headPointTable = {k: v for k, v in headPointTable.items() if v >= minSupport}
    else:
        if array_in.has_key(headPointItem):
            headPointTable = {k: v for k, v in array_in[headPointItem].items() if v >= minSupport}

    frequentItems = set(headPointTable.keys())
    if len(frequentItems) == 0: return None, None, None

    for k in headPointTable:
        headPointTable[k] = [headPointTable[k], None]
    fptree = TreeNode("null", 1, None)
    array_out = {}
    # scan dataset at the second time, filter out items for each record
    for items, count in frozenDataSet.items():
        frequentItemsInRecord = {}
        for item in items:
            if item in frequentItems:
                frequentItemsInRecord[item] = headPointTable[item][0]
        if len(frequentItemsInRecord) > 0:
            orderedFrequentItems = [v[0] for v in
                                    sorted(frequentItemsInRecord.items(), key=lambda v: v[1], reverse=True)]
            updateArrayOut(orderedFrequentItems, array_out)
            updateFPTree(fptree, orderedFrequentItems, headPointTable, count)

    return fptree, headPointTable, array_out


def updateArrayOut(orderedFrequentItems, array_out):
    for i in range(0, len(orderedFrequentItems)):
        for j in range(i + 1, len(orderedFrequentItems)):
            # print (orderedFrequentItems[i])
            # print (orderedFrequentItems[j])
            val = 1
            if array_out.has_key(orderedFrequentItems[i]):
                if array_out[orderedFrequentItems[i]].has_key(orderedFrequentItems[j]):
                    val = array_out[orderedFrequentItems[i]][orderedFrequentItems[j]] + 1
            addtwodimdict(array_out, orderedFrequentItems[i], orderedFrequentItems[j], val)


def addtwodimdict(thedict, key_a, key_b, val):
    if key_a in thedict:
        thedict[key_a].update({key_b: val})
    else:
        thedict.update({key_a: {key_b: val}})


def updateFPTree(fptree, orderedFrequentItems, headPointTable, count):
    # handle the first item
    if orderedFrequentItems[0] in fptree.children:
        fptree.children[orderedFrequentItems[0]].increaseC(count)
    else:
        fptree.children[orderedFrequentItems[0]] = TreeNode(orderedFrequentItems[0], count, fptree)

        # update headPointTable
        if headPointTable[orderedFrequentItems[0]][1] == None:
            headPointTable[orderedFrequentItems[0]][1] = fptree.children[orderedFrequentItems[0]]
        else:
            updateHeadPointTable(headPointTable[orderedFrequentItems[0]][1], fptree.children[orderedFrequentItems[0]])
    # handle other items except the first item
    if (len(orderedFrequentItems) > 1):
        updateFPTree(fptree.children[orderedFrequentItems[0]], orderedFrequentItems[1::], headPointTable, count)


def updateHeadPointTable(headPointBeginNode, targetNode):
    while (headPointBeginNode.nextSimilarItem != None):
        headPointBeginNode = headPointBeginNode.nextSimilarItem
    headPointBeginNode.nextSimilarItem = targetNode


def mineFPTree(headPointTable, prefix, frequentPatterns, minSupport, array_in):
    # for each item in headPointTable, find conditional prefix path, create conditional fptree, then iterate until there is only one element in conditional fptree
    if headPointTable is None:
        return
    headPointItems = [v[0] for v in sorted(headPointTable.items(), key=lambda v: v[1][0])]
    if (len(headPointItems) == 0): return

    for headPointItem in headPointItems:
        newPrefix = prefix.copy()
        newPrefix.add(headPointItem)
        support = headPointTable[headPointItem][0]
        frequentPatterns[frozenset(newPrefix)] = support

        prefixPath = getPrefixPath(headPointTable, headPointItem)
        if (prefixPath != {}):
            conditionalFPtree, conditionalHeadPointTable, array_out = createFPTree(headPointItem, prefixPath,
                                                                                   minSupport, array_in)
            if conditionalHeadPointTable != None:
                mineFPTree(conditionalHeadPointTable, newPrefix, frequentPatterns, minSupport, array_out)


def getPrefixPath(headPointTable, headPointItem):
    prefixPath = {}
    beginNode = headPointTable[headPointItem][1]
    prefixs = ascendTree(beginNode)
    if ((prefixs != [])):
        prefixPath[frozenset(prefixs)] = beginNode.count

    if beginNode is None:
        return prefixPath
    while (beginNode.nextSimilarItem != None):
        beginNode = beginNode.nextSimilarItem
        prefixs = ascendTree(beginNode)
        if (prefixs != []):
            prefixPath[frozenset(prefixs)] = beginNode.count
    return prefixPath


def ascendTree(treeNode):
    prefixs = []
    if treeNode is None:
        return prefixs
    while ((treeNode.nodeParent != None) and (treeNode.nodeParent.nodeName != 'null')):
        treeNode = treeNode.nodeParent
        prefixs.append(treeNode.nodeName)
    return prefixs


def rulesGenerator(frequentPatterns, minConf, rules):
    for frequentset in frequentPatterns:
        if (len(frequentset) > 1):
            getRules(frequentset, frequentset, rules, frequentPatterns, minConf)


def removeStr(set, str):
    tempSet = []
    for elem in set:
        if (elem != str):
            tempSet.append(elem)
    tempFrozenSet = frozenset(tempSet)
    return tempFrozenSet


def getRules(frequentset, currentset, rules, frequentPatterns, minConf):
    for frequentElem in currentset:
        subSet = removeStr(currentset, frequentElem)
        if frequentPatterns.has_key(subSet):
            confidence = frequentPatterns[frequentset] / frequentPatterns[subSet]
            if (confidence >= minConf):
                flag = False
                for rule in rules:
                    if (rule[0] == subSet and rule[1] == frequentset - subSet):
                        flag = True
                if (flag == False):
                    rules.append((subSet, frequentset - subSet, confidence))

                if (len(subSet) >= 2):
                    getRules(frequentset, subSet, rules, frequentPatterns, minConf)


if __name__ == '__main__':
    # prifile = hpy()
    t = time.time()
    startTime = int(round(t * 1000))
    print("fptree:")
    dataSet = loadDataSet()
    frozenDataSet = transfer2FrozenDataSet(dataSet)
    minSupport = 3
    headPointItem = ""
    array_in = {}
    fptree, headPointTable, array_out = createFPTree(headPointItem, frozenDataSet, minSupport, array_in)
    # fptree.disp()
    frequentPatterns = {}
    prefix = set([])
    mineFPTree(headPointTable, prefix, frequentPatterns, minSupport, array_out)
    print("frequent patterns:")
    print(frequentPatterns)
    minConf = 0.6
    rules = []
    rulesGenerator(frequentPatterns, minConf, rules)
    print("association rules:")
    outRule = []
    for rule in rules:
        if len(rule) > 0:
            if len(rule[0]) == 2 and len(rule[1]) == 1:
                IsPrintRule = True
                for item in rule[0]:
                    if "_0" not in item:
                        IsPrintRule = False
                        break
                for item in rule[1]:
                    if "_1" not in item:
                        IsPrintRule = False
                        break
                if IsPrintRule:
                    print(rule)
                    outRule.append(rule)

    t = time.time()
    endTime = int(round(t * 1000))
    print(endTime - startTime)
    info = psutil.virtual_memory()
    print("内存使用：", psutil.Process(os.getpid()).memory_info().rss)
    print(u'总内存：', info.total)
    print(u'内存占比：', info.percent)
    print(u'cpu个数：', psutil.cpu_count())






