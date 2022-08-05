# Databricks notebook source
#Question 1- Cmd 33
#Question 2 - Cmd 44 & Cmd 46
#Question 3 - Months, Center and Account numbers. Cmd 44,46,38,40
#Question 4 - Yes, Cmd 51
#Most affecting SPEND Categories - Cmd 33
#Least affecting SPEND Categories - Cmd 34
#Most affecting REVENUE Categories - Cmd 32
#Least affecting REVENUE Categories - Cmd 35
#Most afftecting Locations for Spend Category- Cmd 47, Cmd 62
#Most affecting months for SPEND CATEGORY- Cmd 44, Cmd 66
#Most affecting accounts - Cmd 47 
#Poor performing categories and it's correlation with other columns - Cmd 51
#Poor performing account numbers - Cmd 54
#Poor performing account location and it's correlation with other columns - Cmd 56
#Poor performing account months and it's correlation with other columns- Cmd 59
#Question 5 - Cmd 47, Cmd 48, Cmd 51. Better rates for 'LOGISTICS OUTSIDE WAREHOUSE LABOR','EC RENTAL','GENERAL SUBCONTRACTED TRANSPORTATION EXPENSE','WAREHOUSE AND PACKING', 'GM LLP SUBCONTRACTED TRANSPORTATION', 'LINEHAUL' categories in cost centers 903,1000,619. Also, limiting expenses during the months of 202106 on account numbers 70200 and 71500 in location 6759 and 4558.

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import math
import warnings
import re
import string
import csv
from datetime import datetime

# COMMAND ----------

data = pd.read_csv("/dbfs/FileStore/FinanceDat.csv")

# COMMAND ----------

data.head()

# COMMAND ----------

data.shape

# COMMAND ----------

data.describe()

# COMMAND ----------

data.columns

# COMMAND ----------

data.nunique()

# COMMAND ----------

data['COST_CENTER_CD'].unique()

# COMMAND ----------

data['ACCOUNTING_MONTH'].unique()

# COMMAND ----------

data['ACCOUNTING_MONTH'] = pd.to_datetime(data['ACCOUNTING_MONTH'], format='%Y/%m')
data

# COMMAND ----------

data['COST_CENTER_LEVEL_2_DESC'].unique()

# COMMAND ----------

data['LEDGER_CATEGORY_LEVEL_2'].unique()

# COMMAND ----------

data['LEDGER_CATEGORY_LEVEL_3'].unique()

# COMMAND ----------

#cleaning the data
data.isnull().sum()

# COMMAND ----------

corr=data.corr()

# COMMAND ----------

c = sns.heatmap(corr, annot=True, cmap='Reds')

# COMMAND ----------

sns.pairplot(data)

# COMMAND ----------

sns.pairplot(corr)

# COMMAND ----------

data['LEDGER_CATEGORY_LEVEL_2'].value_counts().plot(title="Frequency of each category")

# COMMAND ----------

data['LEDGER_CATEGORY_LEVEL_3'].value_counts().plot(title="Frequency of categories")

# COMMAND ----------

sns.catplot(y= 'LEDGER_CATEGORY_LEVEL_2', x='USD_TRANSACTION_AMT', data=data, kind='box')

# COMMAND ----------

sns.catplot(x= 'USD_TRANSACTION_AMT', y='LEDGER_CATEGORY_LEVEL_2', data=data)

# COMMAND ----------

#Dataframe of LEDGER CATEGOR2 against Margin
df1=pd.DataFrame(columns =[data['LEDGER_CATEGORY_LEVEL_2'], data['USD_TRANSACTION_AMT']])
df1

# COMMAND ----------

data['LEDGER_CATEGORY_LEVEL_2'].value_counts()

# COMMAND ----------

data['LEDGER_CATEGORY_LEVEL_3'].value_counts()

# COMMAND ----------

sns.catplot(x= 'USD_TRANSACTION_AMT', y='LEDGER_CATEGORY_LEVEL_3', data=data)

# COMMAND ----------

#Datframe of LEDGER CATEGORY 3 against Margin
df2=pd.DataFrame(columns =[data['LEDGER_CATEGORY_LEVEL_3'], data['USD_TRANSACTION_AMT']])
df2

# COMMAND ----------

data1= data[data['LEDGER_CATEGORY_LEVEL_3'] == 'SPEND CATEGORY' ]

# COMMAND ----------

data2= data[data['LEDGER_CATEGORY_LEVEL_3'] == 'REVENUE CATEGORY' ]

# COMMAND ----------

#Margin for all the SPEND CATEGORY
df3=pd.DataFrame(columns =[data1['LEDGER_CATEGORY_LEVEL_2'], data1['USD_TRANSACTION_AMT']])
df3= df3.T
df3

# COMMAND ----------

#Margin for all the Revenue CATEGORY
df4=pd.DataFrame(columns =[data2['LEDGER_CATEGORY_LEVEL_2'], data2['USD_TRANSACTION_AMT']])
df4=df4.T
df4

# COMMAND ----------

#Revenue category sorted by Margin
df5= df4.sort_values(by="USD_TRANSACTION_AMT")
df5

# COMMAND ----------

#Spend Category sorted by Margin
df6=df3.sort_values(by="USD_TRANSACTION_AMT")
df6

# COMMAND ----------

#Least affecting Spend Categories
df6.iloc[150150:150180]

# COMMAND ----------

#Least affecting Revenue Categories
df5.iloc[60500:60520]

# COMMAND ----------

#Most affecting Revenue Category
data3= data2[data2['LEDGER_CATEGORY_LEVEL_2'] == 'WAREHOUSE AND PACKING' ]
data3

# COMMAND ----------

#Most affecting Revenue Category
data4= data2[data2['LEDGER_CATEGORY_LEVEL_2'] == 'OTHER VARIABLE REVENUE' ]
data4

# COMMAND ----------

#Months that affect the Margin the most in Revenue Category
data3['ACCOUNTING_MONTH'].value_counts()+data4['ACCOUNTING_MONTH'].value_counts()

# COMMAND ----------

#Months that affect the Margin the most in Revenue Category
(data3['ACCOUNTING_MONTH'].value_counts()+data4['ACCOUNTING_MONTH'].value_counts()).plot(title= "Months with most Margin change for Revenue Category")

# COMMAND ----------

#Most affecting Locations for REVENUE CATEGORY
d10= data4.merge(data3, on=['COST_CENTER_CD'])
d12=d10['COST_CENTER_CD'].value_counts()
d12

# COMMAND ----------

d10['COST_CENTER_CD'].value_counts().plot(title= "Locations with most Margin change for Revenue Category")

# COMMAND ----------

#Most affecting category in SPEND Category
data5= data1[data1['LEDGER_CATEGORY_LEVEL_2'] == 'LOGISTICS OUTSIDE WAREHOUSE LABOR' ]
data5

# COMMAND ----------

data6= data1[data1['LEDGER_CATEGORY_LEVEL_2'] == 'WAREHOUSE NON-SALARY' ]
data6

# COMMAND ----------

#Most affecting months for SPEND CATEGORY
data5['ACCOUNTING_MONTH'].value_counts()+data6['ACCOUNTING_MONTH'].value_counts()

# COMMAND ----------

(data5['ACCOUNTING_MONTH'].value_counts()+data6['ACCOUNTING_MONTH'].value_counts()).plot(title= "Months with most Margin change for Spend Category")

# COMMAND ----------

#Accounts that are affected by Spend category the most
d14= data5.append(data6)
d14.plot(x='LEDGER_ACCOUNT_CD', y='USD_TRANSACTION_AMT')

# COMMAND ----------

#Most affected accounts in the spend category
d14['LEDGER_ACCOUNT_CD'].value_counts()

# COMMAND ----------

#Most afftecting Locations for Spend Category
d11= data5.merge(data6, on=['COST_CENTER_CD'])
d13=d11['COST_CENTER_CD'].value_counts()
d13

# COMMAND ----------

(d11['COST_CENTER_CD'].value_counts()).plot(title= "Locations with most Margin change for Spend Category")

# COMMAND ----------

#Correlation of location and Margin for Spend Category
d14= data5.append(data6)
d14.plot(x='COST_CENTER_CD', y='USD_TRANSACTION_AMT')

# COMMAND ----------

#Poor Performing Accounts
data7= data[data['LEDGER_CATEGORY_LEVEL_2']. isin (['LOGISTICS OUTSIDE WAREHOUSE LABOR','EC RENTAL','GENERAL SUBCONTRACTED TRANSPORTATION EXPENSE','WAREHOUSE AND PACKING', 'GM LLP SUBCONTRACTED TRANSPORTATION', 'LINEHAUL'])]
data7

# COMMAND ----------

data7['LEDGER_ACCOUNT_DESC'].value_counts()

# COMMAND ----------

#Poor Performing Accounts
data7.describe()

# COMMAND ----------

data7['LEDGER_ACCOUNT_CD'].value_counts()

# COMMAND ----------

#Majority of Poor performing account number
data8= data7[data['LEDGER_ACCOUNT_CD']. isin ([51700,5200,99700,40200])]
data8

# COMMAND ----------



# COMMAND ----------

data7['COST_CENTER_LEVEL_3_DESC'].value_counts()

# COMMAND ----------

#Poor performing account numbers
data8.describe()

# COMMAND ----------

#Poor performing by categories
data7['COST_CENTER_CD'].value_counts()

# COMMAND ----------

data8['COST_CENTER_CD'].value_counts()

# COMMAND ----------

#Cost Center for poor performing accounts
p1= data[data['COST_CENTER_CD'].isin ([ 1000,903,619,673])]
p1

# COMMAND ----------

p1['LEDGER_CATEGORY_LEVEL_2'].value_counts()

# COMMAND ----------

data7['ACCOUNTING_MONTH'].value_counts()

# COMMAND ----------

data8['ACCOUNTING_MONTH'].value_counts()

# COMMAND ----------

#Poor Performing account number's plot
data8.plot(x='LEDGER_ACCOUNT_CD', y='USD_TRANSACTION_AMT')

# COMMAND ----------

#Poor performing account number vs cost center
data8.plot(x='COST_CENTER_CD', y='USD_TRANSACTION_AMT')

# COMMAND ----------

#Locating the most affecting spend category 
c1= data[data['COST_CENTER_CD'] == 6759]
c1

# COMMAND ----------

c1['LEDGER_CATEGORY_LEVEL_2'].value_counts()

# COMMAND ----------

c1['ACCOUNTING_MONTH'].value_counts()

# COMMAND ----------

c1.plot(x='LEDGER_ACCOUNT_CD', y='USD_TRANSACTION_AMT', title='Most affective location-6759 impact on Margin')

# COMMAND ----------

#Most affecting month
c2= data[data['ACCOUNTING_MONTH'] == 202106]
c2

# COMMAND ----------

c2['COST_CENTER_LEVEL_3_DESC'].value_counts()

# COMMAND ----------

c2['LEDGER_CATEGORY_LEVEL_2'].value_counts()

# COMMAND ----------

c2['COST_CENTER_CD'].value_counts()

# COMMAND ----------

c2.plot(x='LEDGER_ACCOUNT_CD', y='USD_TRANSACTION_AMT' , title='Most affective month-202106 impact on Margin')