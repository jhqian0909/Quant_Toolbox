# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 13:53:27 2018

@author: jhqian
"""
from tqdm import tqdm
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import os 
import re
import pandas as pd 
import numpy as np
from dateutil.parser import parse
import pickle
import datetime

#use WIND as data provider
from WindPy import *
w.start()

import pymysql
from sqlalchemy import create_engine,MetaData,types

#use Mysql to store data
engine = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/quantstock?charset=utf8")
meta = MetaData(engine)

#get the renew date
import datetime
time=datetime.datetime.now()
if time.time()>datetime.time(17,0,0,0):
    TODAY=time.date().strftime('%Y%m%d')
else:
    TODAY=(time.date()-datetime.timedelta(days=1)).strftime('%Y%m%d')

#renew the trading date list
startday="2001-01-01"
endday=TODAY
trading_day=w.tdays(startday, endday, "").Data[0]
all_day=pd.date_range(start=startday, end=endday)
trade_day=pd.Series(data=np.zeros(len(all_day)),index=all_day).add(pd.Series(data=np.ones(len(trading_day)),index=trading_day),fill_value=0)
trade_day.name='indicator'

#loan to Mysql
trade_day_tosql=trade_day.copy()
trade_day_tosql=pd.DataFrame(trade_day_tosql)
trade_day_tosql.reset_index(inplace=True)
trade_day_tosql.columns=['TRADING_DAY','INDICATOR']
trade_day_tosql['TRADING_DAY']=list(map(lambda x:x.strftime('%Y%m%d'),list(trade_day_tosql['TRADING_DAY'])))
trade_day_tosql.to_sql(name = 'trading_day',con = engine,if_exists = 'replace',index = False,index_label = False,dtype={'TRADING_DAY':types.VARCHAR(15),'INDICATOR':types.INT()})

#expand 30 days 
endday=parse(TODAY)+datetime.timedelta(days=30)
trading_day=w.tdays(startday, endday, "").Data[0]
all_day=pd.date_range(start=startday, end=endday)
forward_trade_day=pd.Series(data=np.zeros(len(all_day)),index=all_day).add(pd.Series(data=np.ones(len(trading_day)),index=trading_day),fill_value=0)
forward_trade_day.name='indicator'
trade_day_tosql=pd.DataFrame(forward_trade_day)
trade_day_tosql.reset_index(inplace=True)
trade_day_tosql.columns=['TRADING_DAY','INDICATOR']
trade_day_tosql['TRADING_DAY']=list(map(lambda x:x.strftime('%Y%m%d'),list(trade_day_tosql['TRADING_DAY'])))
trade_day_tosql.to_sql(name = 'forward_trading_day',con = engine,if_exists = 'replace',index = False,index_label = False,dtype={'TRADING_DAY':types.VARCHAR(15),'INDICATOR':types.INT()})

#renew the delisted stocks list
delisted_stock=w.wset('delistsecurity','field=wind_code,sec_name,delist_date,reorganize_code,reorganize_name')
condition1=[i for i in delisted_stock.Data[0] if (i[0]=='0' or i[0]=='3' or i[0]=='6' or i[0]=='T')&(i[-3:]=='.SZ' or i[-3:]=='.SH')]
result=pd.DataFrame({'STOCK_NAME':delisted_stock.Data[1],'STOCK_CODE':delisted_stock.Data[0],'DELISTED_DATE':delisted_stock.Data[2],'NEW_STOCK_NAME':delisted_stock.Data[4],'NEW_STOCK_CODE':delisted_stock.Data[3]})
result=result[result['STOCK_CODE'].isin(condition1)].reset_index(drop=True)
for i in range(len(result)):
    code=result.ix[i,'NEW_STOCK_CODE']
    try:
        if not (code[0]=='0' or code[0]=='3' or code[0]=='6' or code[0]=='T')&(code[-3:]=='.SZ' or code[-3:]=='.SH'):
            code=np.nan
            result.ix[i,'NEW_STOCK_NAME']=np.nan
    except:
        pass
result.dropna(inplace=True)
result['DELISTED_DATE']=[i.strftime('%Y%m%d') for i in result['DELISTED_DATE'] ]
result.replace({'':None},inplace=True)
result=result[result['DELISTED_DATE']>'20040319']
result['UPDATE_TIME']=TODAY
result.to_sql(name = 'delisted_stock_list',con = engine,if_exists = 'replace',index = False,index_label = False)
print('delisted_stock_list renew succeed!')

#renew the listed stocks today
all_stock=w.wset("sectorconstituent","date={};sectorid={}".format(TODAY, 'a001010100000000'))
db = pymysql.connect("127.0.0.1", "root", "root", "quantstock",charset='gbk') 
try:
    sql="SELECT STOCK_CODE,STOCK_NAME FROM history_all_stock_list"
    info=pd.read_sql(sql,con=db)
    sql2="SELECT STOCK_CODE,STOCK_NAME FROM delisted_stock_list"
    info2=pd.read_sql(sql2,con=db)
except:  
    print("Error: unable to fecth data") 


# add the today's newly listed stocks
new_data=pd.DataFrame({'STOCK_CODE':all_stock.Data[1],'STOCK_NAME':all_stock.Data[2]})[~pd.Series(all_stock.Data[1]).isin(info['STOCK_CODE'])]
new_data.to_sql(name = 'history_all_stock_list',con = engine,if_exists = 'append',index = False,index_label = False)
db = pymysql.connect("127.0.0.1", "root", "root", "quantstock",charset='gbk')
try:
    sql3="SELECT STOCK_CODE,STOCK_NAME FROM history_all_stock_list"
    info3=pd.read_sql(sql3,con=db)
except:  
    print("Error: unable to fecth data")

# add the delisted stocks to all stocks
new_data2=info2[~info2['STOCK_CODE'].isin(info3['STOCK_CODE'])]
if len(new_data2)!=0:
    print('need special care')
    new_data2.to_sql(name = 'history_all_stock_list',con = engine,if_exists = 'append',index = False,index_label = False)
else:
    pass
print('history_all_stock_list renew succeed')

#renew trading data
db = pymysql.connect("127.0.0.1", "root", "root", "quantstock",charset='gbk') 
try:
    sql="SELECT DISTINCT TRADING_DAY FROM stock_data ORDER BY TRADING_DAY"
    info=pd.read_sql(sql,con=db)
except:  
    print("Error: unable to fecth data")

#get the last renew date
last_renew_date=str(info.values[-1][0])
last_index=int(np.argwhere(trade_day.index==parse(last_renew_date)))
need_to_renew=trade_day[last_index:][trade_day[last_index:]==1][1:]
try:
    sql="SELECT STOCK_CODE FROM history_all_stock_list"
    info=pd.read_sql(sql,con=db)
except:  
    print("Error: unable to fecth data")
code=list(info['STOCK_CODE'])
for i in need_to_renew.index:
    THISDAY=i.strftime('%Y-%m-%d')
    stock_data=w.wss(code,'close,total_shares,float_a_shares,free_float_shares,open,high,low,adjfactor,volume,amt,turn,susp_days,maxupordown,vwap',"tradeDate={};priceAdj=U;cycle=D".format(THISDAY))
    stock_new_data=pd.DataFrame({'TRADING_DAY':THISDAY[:4]+THISDAY[5:7]+THISDAY[8:],'STOCK':code,'CLOSE_PRICE':stock_data.Data[0],'TOTAL_SHARE':stock_data.Data[1],'AFLOATS':stock_data.Data[2],'FREE_FLOAT_SHARE':stock_data.Data[3],'OPEN_PRICE':stock_data.Data[4],'HIGH_PRICE':stock_data.Data[5],'LOW_PRICE':stock_data.Data[6],'ADJUSTED_FACTOR':stock_data.Data[7],'TURNOVER_VOLUME':stock_data.Data[8],'TURNOVER_VALUE':stock_data.Data[9],'TURNOVER_RATIO':stock_data.Data[10],'TRADE_HALT_DAYS':stock_data.Data[11],'LIMIT_STATUS':stock_data.Data[12],'VWAP':stock_data.Data[13]})
    stock_new_data.dropna(subset=['LIMIT_STATUS'],inplace=True)#drop the delisted
    stock_new_data.to_sql(name = 'stock_data',con = engine,if_exists = 'append',index = False,index_label = False)
print('stock_data renew succeed')

# renew industry index data 非银拆成券商、保险、多元以及白酒
try:
    sql="SELECT DISTINCT TRADING_DAY FROM industry_index_data ORDER BY TRADING_DAY"
    info=pd.read_sql(sql,con=db)
except:  
    print("Error: unable to fecth data")

last_renew_date=str(info.values[-1][0])
last_index=int(np.argwhere(trade_day.index==parse(last_renew_date)))
need_to_renew=trade_day[last_index:][trade_day[last_index:]==1][1:]
index=['801010.SI','801020.SI','801030.SI','801040.SI','801050.SI','801080.SI','801110.SI','801120.SI','801130.SI','801140.SI','801150.SI','801160.SI','801170.SI','801180.SI','801200.SI'
       ,'801210.SI','801230.SI','801710.SI','801720.SI','801730.SI','801740.SI','801750.SI','801760.SI','801770.SI','801780.SI','801880.SI','801890.SI','851231.SI','801191.SI','801193.SI','801194.SI']
for i in need_to_renew.index:
    THISDAY=i.strftime('%Y%m%d')
    index_data=w.wss(index, "sec_name,open,high,low,close,volume,amt","tradeDate={};priceAdj=U;cycle=D".format(THISDAY))
    index_new_data=pd.DataFrame(index_data.Data,index=index_data.Fields,columns=index_data.Codes).T
    index_new_data.columns=['INDEX_NAME','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','TURNOVER_VOLUME','TURNOVER_VALUE']
    index_new_data['TRADING_DAY']=THISDAY
    index_new_data['INDEX']=index
    index_new_data.to_sql(name = 'industry_index_data',con = engine,if_exists = 'append',index = False,index_label = False)
print('industry_index_data renew succeed')

#renew index data
try:
    sql="SELECT DISTINCT TRADING_DAY FROM index_data ORDER BY TRADING_DAY"
    info=pd.read_sql(sql,con=db)
except:  
    print("Error: unable to fecth data")

last_renew_date=str(info.values[-1][0])
last_index=int(np.argwhere(trade_day.index==parse(last_renew_date)))
need_to_renew=trade_day[last_index:][trade_day[last_index:]==1][1:]
index_name=['000001.SH','000016.SH','000300.SH','000905.SH','000982.SH','000984.SH','000985.CSI','399101.SZ','399102.SZ','399106.SZ','881001.WI','H00300.CSI','H00905.CSI','H00985.CSI']
for i in need_to_renew.index:
    THISDAY=i.strftime('%Y-%m-%d')
    index_data=w.wss(index_name, "open,high,low,close,volume,amt","tradeDate={};priceAdj=U;cycle=D".format(THISDAY))
    index_new_data=pd.DataFrame({'TRADING_DAY':THISDAY[:4]+THISDAY[5:7]+THISDAY[8:],'INDEX':index_name,'OPEN_PRICE':index_data.Data[0],'HIGH_PRICE':index_data.Data[1],'LOW_PRICE':index_data.Data[2],'CLOSE_PRICE':index_data.Data[3],'TURNOVER_VOLUME':index_data.Data[4],'TURNOVER_VALUE':index_data.Data[5]})
    index_new_data.to_sql(name = 'index_data',con = engine,if_exists = 'append',index = False,index_label = False)
print('index_data renew succeed')

#renew industry data 
try:
    sql="SELECT DISTINCT TRADING_DAY FROM industry_data ORDER BY TRADING_DAY"
    info=pd.read_sql(sql,con=db)
except:  
    print("Error: unable to fecth data")
# 获取上次更新日期
last_renew_date=str(info.values[-1][0])
last_index=int(np.argwhere(trade_day.index==parse(last_renew_date)))
need_to_renew=trade_day[last_index:][trade_day[last_index:]==1][1:]
#读取本地industry_code列表
fr = open('D:\\Quant\\Factors_Pickled\\IndustryList_SW.txt','rb')
industry_name = pickle.load(fr)
fr.close()
for i in need_to_renew.index:
    THISDAY=i.strftime('%Y-%m-%d')
    industry_data=w.wss(list(industry_name+'.SI'), "open,high,low,close,volume,amt","tradeDate={};priceAdj=F;cycle=D".format(THISDAY))
    industry_new_data=pd.DataFrame({'TRADING_DAY':THISDAY[:4]+THISDAY[5:7]+THISDAY[8:],'INDUSTRY':list(industry_name+'.SI'),'OPEN_PRICE':industry_data.Data[0],'HIGH_PRICE':industry_data.Data[1],'LOW_PRICE':industry_data.Data[2],'CLOSE_PRICE':industry_data.Data[3],'TURNOVER_VOLUME':industry_data.Data[4],'TURNOVER_VALUE':industry_data.Data[5]})
    industry_new_data.to_sql(name = 'industry_data',con = engine,if_exists = 'append',index = False,index_label = False)
print('industry_data更新成功')
#%% 更新shibor
try:
    sql="SELECT TRADING_DAY FROM shibor_data ORDER BY TRADING_DAY"
    info=pd.read_sql(sql,con=db)
except:  
    print("Error: unable to fecth data")
# 获取上次更新日期
last_renew_date=str(info.values[-1][0])
last_index=int(np.argwhere(trade_day.index==parse(last_renew_date)))
need_to_renew=trade_day[last_index:][trade_day[last_index:]==1][1:]
shibor_code=['SHIBOR1Y.IR','SHIBOR6M.IR','SHIBOR3M.IR','SHIBOR1M.IR','SHIBOR1W.IR','SHIBORON.IR']
for i in need_to_renew.index:
    THISDAY=i.strftime('%Y-%m-%d')
    shibor_data=w.wss(shibor_code, "close","tradeDate={};priceAdj=U;cycle=D".format(THISDAY))
    shibor_new_data=pd.DataFrame({'TRADING_DAY':THISDAY[:4]+THISDAY[5:7]+THISDAY[8:],'1Y':shibor_data.Data[0][0],'6M':shibor_data.Data[0][1],'3M':shibor_data.Data[0][2],'1M':shibor_data.Data[0][3],'1W':shibor_data.Data[0][4],'ON':shibor_data.Data[0][5]},index=[0])
    shibor_new_data.to_sql(name = 'shibor_data',con = engine,if_exists = 'append',index = False,index_label = False)
print('shibor更新成功')
#%% 更新申万三个级别的行业信息SW_industry_category pickle存为本地文件
#这里的path为之前申万三级行业pickle文件所在
path='D:\Quant\SW_Industry_Category_Pickled'
def file_name(file_dir): 
    for root, dirs, files in os.walk(file_dir):
        L=[]
        for file in files:
            L.append(re.compile(r'\d+').findall(file)[0])
    return L
# 获取上次更新日期
SW3_last_renew_date=max(file_name(path))

file_sw_path='D:\\Quant\\SW_Industry_Category_Pickled\\'
season_to_renew=[]
season_date=pd.date_range(start=parse(SW3_last_renew_date)+datetime.timedelta(days=1), end=TODAY)
for i in season_date:
    if (str(i)[5:10]=='03-31') or (str(i)[5:10]=='06-30') or (str(i)[5:10]=='09-30') or (str(i)[5:10]=='12-31'):
        season_to_renew.append(i)
if len(season_to_renew)==0:
    print('不需要更新sw三级行业')
else:
    print('需要更新sw三级行业')
    for i in season_to_renew:
        #这里只取了在市的所有股票
        all_stock_SW=w.wset("sectorconstituent","date={};sectorid={};field=wind_code".format(i.strftime('%Y-%m-%d'), 'a001010100000000')).Data[0] 
        industry_first=w.wss(all_stock_SW,"industry_sw,industry_swcode,indexcode_sw","industryType=1;tradeDate={}".format(i.strftime('%Y%m%d')))
        industry_second=w.wss(all_stock_SW,"industry_sw,industry_swcode,indexcode_sw","industryType=2;tradeDate={}".format(i.strftime('%Y%m%d')))
        industry_third=w.wss(all_stock_SW,"industry_sw,industry_swcode,indexcode_sw","industryType=3;tradeDate={}".format(i.strftime('%Y%m%d')))
        SW3_1=pd.DataFrame({'STOCK_CODE':all_stock_SW,'FIRST_INDUSTRY_CODE':industry_first.Data[2],'FIRST_INDUSTRY_NAME':industry_first.Data[0]})
        SW3_2=pd.DataFrame({'STOCK_CODE':all_stock_SW,'SECOND_INDUSTRY_CODE':industry_second.Data[2],'SECOND_INDUSTRY_NAME':industry_second.Data[0]})
        SW3_3=pd.DataFrame({'STOCK_CODE':all_stock_SW,'THIRD_INDUSTRY_CODE':industry_third.Data[2],'THIRD_INDUSTRY_NAME':industry_third.Data[0]})
        SW_final=SW3_1.merge(SW3_2,on='STOCK_CODE').merge(SW3_3,on='STOCK_CODE')
        SW_final=SW_final[['STOCK_CODE','FIRST_INDUSTRY_CODE','FIRST_INDUSTRY_NAME','SECOND_INDUSTRY_CODE','SECOND_INDUSTRY_NAME','THIRD_INDUSTRY_CODE','THIRD_INDUSTRY_NAME']]
        fw = open(file_sw_path+'SWIndustryCategory_'+i.strftime('%Y%m%d')+'.txt','wb')
        pickle.dump(SW_final, fw)
        fw.close()
        print('更新sw三级行业成功')
#%% 更新 index_component pickle存为本地文件
'''
IndexInfo={'000905.SH',20070115;
           '000300.SH',20050408;
           '000985.CSI',20050408
           };

new_index={'000906.SH',20041231;
            }

#如需加入新的指数，调用以下函数,index_code,startdate如示例所示，分别为str和int,第一次加入新的指数后，以后使用常规更新程序更新已有指数，需将指数代码加入更新list
def add_new_index_component(index_code,startdate):
    db = pymysql.connect("127.0.0.1", "root", "root", "quantstock",charset='gbk') 
    try:
        sql="SELECT STOCK_CODE FROM history_all_stock_list"
        info=pd.read_sql(sql,con=db)
    except:  
        print("Error: unable to fecth data")
    code=list(info['STOCK_CODE'])
    result=pd.DataFrame(columns=code)
    start_index=int(np.argwhere(trade_day.index==parse(str(startdate))))
    need_to_renew=trade_day[start_index:][trade_day[start_index:]==1]
    
    for s in need_to_renew.index:
        bench=w.wset("indexconstituent","date={};windcode={}".format(s.strftime('%Y%m%d'), index_code))
        if len(bench.Data[1])==0:
            pass
        else:
            new_index_component=pd.DataFrame(columns=bench.Data[1])
            new_index_component.loc[s.strftime('%Y%m%d')]=1
            result=pd.concat([result,new_index_component])
    result.replace(np.nan,0,inplace=True)

    fw = open('D:\\Quant\\Index_Component&Weight\\component_'+index_code[:6]+'.txt','wb')
    pickle.dump(result, fw)
    fw.close()
    db.close()
    print('更新新增指数成份成功')
'''
#更新已有指数成份
index_code=['000300.SH','000905.SH','000985.CSI'] #可以继续加入指数代码
for i in index_code:
    #这里的地址为之前pickle文件所在地
    fr = open('D:\\Quant\\Index_Component&Weight\\component_'+i[:6]+'.txt','rb') 
    index_component = pickle.load(fr)
    fr.close()
    # 获取上次更新日期
    index_component_last_renew_date=index_component.index[-1]
    last_index=int(np.argwhere(trade_day.index==parse(index_component_last_renew_date)))
    need_to_renew=trade_day[last_index:][trade_day[last_index:]==1][1:]
    for s in need_to_renew.index:
        #ic为指数成分股
        ic=pd.Series(w.wset("sectorconstituent","date={};windcode={}".format(s.strftime('%Y%m%d'), i)).Data[1])
        new_component_stock=ic[~ic.isin(index_component.columns)]
        if len(new_component_stock)==0:
            print('没有新加入指数的票')
        else:
            for j in new_component_stock:
                #新票之前的值都为0
                index_component[j]=0
                print('指数有新票加入')
        #将日期s的是否成分股情况加上
        indicator=np.zeros(len(index_component.columns))
        indicator[index_component.columns.isin(ic)]=1
        index_component.loc[s.strftime('%Y%m%d')]=list(indicator)
    #更新pickle文件
    fw = open('D:\\Quant\\Index_Component&Weight\\component_'+i[:6]+'.txt','wb')
    pick=pickle.Pickler(fw)
    pick.clear_memo()
    pick.dump(index_component)
    fw.close()
    print('更新已有指数成份成功')
#%% 更新index_component_weight  

'''
IndexInfo={'000905.SH',20070115;
           '000300.SH',20050408;
           '000985.CSI',20050408
           };

rew_index={'000906.SH',20041231;
            }

#如需加入新的指数，调用以下函数,index_code,startdate如示例所示，分别为str和int,第一次加入新的指数后，以后使用常规更新程序更新已有指数，需将指数代码加入更新list
def add_new_index_component_weight(index_code,startdate):
    db = pymysql.connect("127.0.0.1", "root", "root", "quantstock",charset='gbk')
    try:
        sql="SELECT STOCK_CODE FROM history_all_stock_list"
        info=pd.read_sql(sql,con=db)
    except:  
        print("Error: unable to fecth data")
    code=list(info['STOCK_CODE'])
    result=pd.DataFrame(columns=code)
    start_index=int(np.argwhere(trade_day.index==parse(str(startdate))))
    need_to_renew=trade_day[start_index:][trade_day[start_index:]==1]

    for s in tqdm(need_to_renew.index[:20]):
        bench=w.wset("indexconstituent","date={};windcode={}".format(s.strftime('%Y-%m-%d'), index_code))
        if len(bench.Data[1])==0:
            pass
        else:
            new_index_component_weight=pd.DataFrame(columns=bench.Data[1])
            new_index_component_weight.loc[s.strftime('%Y%m%d')]=bench.Data[3]
            result=pd.concat([result,new_index_component_weight])
            if result.loc[s.strftime('%Y%m%d')].sum()>=100:
                print('不需要分配剩余权重')
            else:
                print('需要分配剩余权重')#之前报错是因为to_fill_stock为空
                rest_weight=100-result.loc[s.strftime('%Y%m%d')].sum()
                to_fill_stock=new_index_component_weight.columns[new_index_component_weight.loc[s.strftime('%Y%m%d')].isnull()]
                if len(to_fill_stock)==0:
                    pass
                else:
                    sql="SELECT FREE_FLOAT_SHARE,CLOSE_PRICE,STOCK FROM stock_data WHERE TRADING_DAY='{}' AND STOCK in %(L)s".format(s.strftime('%Y%m%d'))
                    info=pd.read_sql(sql,con=db,params={'L':list(to_fill_stock)}).replace(np.nan,0)
                    weight_to_allocate=rest_weight*(info['FREE_FLOAT_SHARE']*info['CLOSE_PRICE'])/(info['FREE_FLOAT_SHARE']*info['CLOSE_PRICE'].sum())
                    for k in range(len(weight_to_allocate)):
                        result.loc[s.strftime('%Y%m%d'),info['STOCK'][k]]=weight_to_allocate[k]
    fw = open('D:\\Quant\\Index_Component&Weight\\component_weight_'+index_code[:6]+'.txt','wb')
    pickle.dump(result, fw)
    fw.close()
    db.close()
    print('更新新增指数权重成功')
'''
##更新已有指数权重
index_code=['000300.SH','000905.SH','000985.CSI','000903.SH'] #可以继续加入指数代码 ,'000903.SH'
db = pymysql.connect("127.0.0.1", "root", "root", "quantstock",charset='gbk')
for i in index_code:
    #这里的地址为之前pickle文件所在地
    fr = open('D:\\Quant\\Index_Component&Weight\\component_weight_'+i[:6]+'.txt','rb')
    index_component_weight = pickle.load(fr)
    fr.close()
    index_component_weight_last_renew_date=index_component_weight.index[-1]
    last_index=int(np.argwhere(trade_day.index==parse(index_component_weight_last_renew_date)))
    need_to_renew=trade_day[last_index:][trade_day[last_index:]==1][1:]
    for s in tqdm(need_to_renew.index):
        icw=w.wset("indexconstituent","date={};windcode={}".format(s.strftime('%Y%m%d'), i))
        #ic为指数成分股
        ic=pd.Series(icw.Data[1])
        new_component_stock=ic[~ic.isin(index_component_weight.columns)]
        if len(new_component_stock)==0:
            print('没有新加入指数的票')
        else:
            for j in new_component_stock:
                #新成分股之前的权重为nan
                index_component_weight[j]=np.nan
                print('指数有新票加入')
        #将日期s成分股权重加上
        new=pd.DataFrame(columns=icw.Data[1])
        #服务变了 100是3 其他是4?
        new.loc[s.strftime('%Y%m%d')]=icw.Data[3]
        index_component_weight=pd.concat([index_component_weight,new])
        if (index_component_weight.loc[s.strftime('%Y%m%d')].sum()) >= 100:
            print('不需要分配剩余权重')
        else:
            print('需要分配剩余权重')
            rest_weight=100-index_component_weight.loc[s.strftime('%Y%m%d')].sum()
            #需要分配权重的是那些成分股中权重为nan的,然而成份股中基本没有为nan的,即最后也没分配权重
            to_fill_stock=new.columns[new.loc[s.strftime('%Y%m%d')].isnull()]
            if len(to_fill_stock)==0:
                pass
            else:
                sql="SELECT FREE_FLOAT_SHARE,CLOSE_PRICE,STOCK FROM stock_data WHERE TRADING_DAY='{}' AND STOCK in %(L)s".format(s.strftime('%Y%m%d'))
                #replace(np.nan,0)的意思是若FREE_FLOAT_SHARE,CLOSE_PRICE中有nan,则在之后的分配中权重为0
                info=pd.read_sql(sql,con=db,params={'L':list(to_fill_stock)}).replace(np.nan,0)
                weight_to_allocate=rest_weight*(info['FREE_FLOAT_SHARE']*info['CLOSE_PRICE'])/(info['FREE_FLOAT_SHARE']*info['CLOSE_PRICE'].sum())
                for k in range(len(weight_to_allocate)):
                    index_component_weight.loc[s.strftime('%Y%m%d'),info['STOCK'][k]]=weight_to_allocate[k]
 
    fw = open('D:\\Quant\\Index_Component&Weight\\component_weight_'+i[:6]+'.txt','wb')
    pick=pickle.Pickler(fw)
    pick.clear_memo()
    pick.dump(index_component_weight)
    fw.close()
    print('更新已有指数权重成功')
#%%
db.close()

#%% 处理之前遗留的票，需单独更 就T00018.SH(000542退市较早, 000405从最开始就为nan，故不更新这两个),SW3可能还需要更新T00018的数据
''' 
from sqlalchemy import create_engine,MetaData
engine = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/quantstock?charset=utf8")
meta = MetaData(engine)
def func(day):
    return day.strftime('%Y%m%d')
def missing_stock(code,startday,endday):#示例code="T00018.SH",startday="2004-01-01",endday="2018-06-29",endday最好为统一最近一次更新的日期
    T=w.wsd(code, 'close,total_shares,float_a_shares,free_float_shares,open,high,low,adjfactor,volume,amt,turn,susp_days,maxupordown,vwap', startday, endday, "")
    result=pd.DataFrame({'TRADING_DAY':list(map(func,T.Times)),'STOCK':code,'CLOSE_PRICE':T.Data[0],'TOTAL_SHARE':T.Data[1],'AFLOATS':T.Data[2],'FREE_FLOAT_SHARE':T.Data[3],'OPEN_PRICE':T.Data[4],'HIGH_PRICE':T.Data[5],'LOW_PRICE':T.Data[6],'ADJUSTED_FACTOR':T.Data[7],'TURNOVER_VOLUME':T.Data[8],'TURNOVER_VALUE':T.Data[9],'TURNOVER_RATIO':T.Data[10],'TRADE_HALT_DAYS':T.Data[11],'LIMIT_STATUS':T.Data[12],'VWAP':T.Data[13]})
    result.dropna(subset=['LIMIT_STATUS'],inplace=True)#去除已经退市的数据
    result.to_sql(name = 'stock_data',con = engine,if_exists = 'append',index = False,index_label = False)
#%% 处理更新错误，删除数据库中的data BETWEEN是包括首尾的
import pymysql
import pandas as pd
db = pymysql.connect("127.0.0.1", "root", "root", "quantstock",charset='gbk') 
#sql="DELETE FROM stock_data WHERE TRADING_DAY BETWEEN '20180803' AND '20180810'"
sql="DELETE FROM cash_flow WHERE UPDATE_TIME>='20181214'"
cur=db.cursor()
sta=cur.execute(sql)
db.commit()
cur.close()    
db.close()
#%% 
'''
