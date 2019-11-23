import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pandas as pd
from scipy.spatial import distance_matrix
import numpy as np
import math
from numpy import inf
from math import pow

spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()

sc = SparkContext.getOrCreate()

data = pd.read_csv('hep.txt', names=['County', 'X','Y','Percentage'])
queryData = spark.read.csv("hep.txt")
print(data)
a = data['County'].values
print(a)
coord = data[['X', 'Y']].values
dataframe = pd.DataFrame(coord, columns=['X', 'Y'], index=a)
distance = pd.DataFrame(distance_matrix(dataframe.values, dataframe.values), index=dataframe.index, columns=dataframe.index)
weight = 1/distance
weight[weight == inf] = 0
print('Weight: ',weight)
sumofz = np.sum(data['Percentage'].values)
print('sumofz ',sumofz)
zbar = sumofz/58
print('zbar ',zbar)
z = pd.DataFrame(data, columns = ['Percentage'])
z = z.T
print(z)
sumofwijz = 0
sumofw = 0
zofizbar = 0
sumofzofibar =0
n = z.size
for i in range(z.size):
	zofi = z.iat[0,i]
	zibar = zofi - zbar
	print('zibar: ', zibar)
	for j in range(z.size):
		zofj = z.iat[0,j]
		print('zi: ', zofi)
		print('zj: ',zofj)
		zjbar = zofj - zbar
		weightofij = weight.iat[i, j]
		sumofwijz = sumofwijz + (weightofij*zibar*zjbar)
		print('sum of w i j z: ',sumofwijz)
		sumofw = sumofw + weightofij
		print('sum of w: ', sumofw)
	sumofzofibar = sumofzofibar + pow(zibar,2)
	print('sum of z of i bar: ', sumofzofibar)
total =  sumofwijz
print('total: ',total)
total1 = sumofw * sumofzofibar
print('total 1: ', total1)
result = total/total1
result = 58* result
print("Final result")
print(result)

f = open('morans.txt','w')
f.write('{}'.format(result))


