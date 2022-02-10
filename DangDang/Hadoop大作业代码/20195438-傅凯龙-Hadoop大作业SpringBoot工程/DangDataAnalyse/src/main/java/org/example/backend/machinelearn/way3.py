# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 01:19:31 2021

@author: neufk
"""
import re
import os
import xlrd
import xlwt
import numpy as np
import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from sklearn import preprocessing
import xgboost as xgb
lbl = preprocessing.LabelEncoder()
warnings.filterwarnings('ignore')
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False



df=pd.DataFrame(columns=["ranking","imag","name","comment","recommond","author","publish","price"])
wayj=str(os.path.abspath(__file__))
ways=wayj[:-len("\src\main\java\org\example\backend\machinelearn\way3.py")]
for i in range(1,9):
    ul = ways+"book"+str(i)+".xls"
    ul=str(ul)
    readbook = xlrd.open_workbook(ul)
    table = readbook.sheet_by_index(0)
    dfs = pd.read_excel(ul,header=None,names=["ranking","imag","name","comment","recommond","author","publish","price"])
    li=dfs
    df =pd.concat([df,dfs],axis=0)
del(df['imag'])
del(df['name'])
df['publisher'] = pd.factorize(df['publish'])[0]
df['authorer'] = pd.factorize(df['author'])[0]
del(df['author'])
del(df['publish'])
df['ranking'] = lbl.fit_transform(df['ranking'].astype(str))
df['comment'] = lbl.fit_transform(df['comment'].astype(str))
df['recommond'] = lbl.fit_transform(df['recommond'].astype(str))
df['price'] = lbl.fit_transform(df['price'].astype(str))
df['publisher'] = lbl.fit_transform(df['publisher'].astype(str))
df['authorer'] = lbl.fit_transform(df['authorer'].astype(str))






params = {
    'booster': 'gbtree',
    'objective': 'multi:softmax',  # 多分类的问题
    'num_class': 500,               # 类别数，与 multisoftmax 并用
    'gamma': 0.1,                  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
    'max_depth': 12,               # 构建树的深度，越大越容易过拟合
    'lambda': 2,                   # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
    'subsample': 0.7,              # 随机采样训练样本
    'colsample_bytree': 0.7,       # 生成树时进行的列采样
    'min_child_weight': 3,
    'silent': 1,                   # 设置成1则没有运行信息输出，最好是设置为0.
    'eta': 0.007,                  # 如同学习率
    'seed': 1000,
    'nthread': 4,                  # cpu 线程数
}
uls = ways+"book"+str(12)+".xls"
dfs = pd.read_excel(uls,header=None,names=["ranking","imag","name","comment","recommond","author","publish","price"])
hah=dfs['name']
del(dfs['imag'])
del(dfs['name'])
dfs['publisher'] = pd.factorize(dfs['publish'])[0]
dfs['authorer'] = pd.factorize(dfs['author'])[0]
del(dfs['author'])
del(dfs['publish'])
del(dfs['ranking'])
dfs['comment'] = lbl.fit_transform(dfs['comment'].astype(str))
dfs['recommond'] = lbl.fit_transform(dfs['recommond'].astype(str))
dfs['price'] = lbl.fit_transform(dfs['price'].astype(str))
dfs['publisher'] = lbl.fit_transform(dfs['publisher'].astype(str))
dfs['authorer'] = lbl.fit_transform(dfs['authorer'].astype(str))


X_train = df.iloc[:,df.columns !='ranking']
trainy = df.iloc[:,df.columns =='ranking']
dtrain = xgb.DMatrix(X_train, trainy)
num_rounds = 500
model = xgb.train(params, dtrain, num_rounds)
ceshi = xgb.DMatrix(dfs)
pred = model.predict(ceshi)
dataDf=pd.DataFrame(pred)
dataDf.rename(columns={"0":"result"})
result=pd.concat([hah,dataDf],axis=1)
result.to_excel(excel_writer="result3.xls",index=False,encoding='utf-8')
