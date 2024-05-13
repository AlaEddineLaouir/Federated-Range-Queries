
from Connection import Connection 

import sql_data
from Index import sql_index

import numpy as np
import scipy.stats as stats
from typing import Dict
from time import time
import random



def __range_to_sql__(query:Dict[str,np.array])->str:
    """Convert a dictionary of ranges to SQL format"""
    sql_range_array =[str(key)+" between "+str(val[0])+ " and " +str(val[1]) for key,val in query.items()]
    
    return " and ".join(sql_range_array)
def __range_to_sql_index__(query:Dict[str,np.array],operation:str)->str:
    operator = ""
    index =0
    if operation == "min":
        operator = " >= "
        index =1
    else:
        operator = " <= "
    sql_range_array =[str(key)+operation+operator+str(val[index]) for key,val in query.items()]
    return " and ".join(sql_range_array)
    
    
def __range_condition_on_index__(index_query:Dict[str,np.array],operation:str) ->str:
    index =0
    if operation == "min": ## to compute the End
        operator = " < "
        index =1
    else:                  ## to compute the Begin
        operator = " > "
    condition = ""

    size = len(index_query.keys())
    items = list(index_query.items())
    for i in range(size):
        key,val = items[i]
        if i + 1 == size:
            condition = condition + "(("+str(key)+operation+operator+str(val[index])+" and "+str(key)+operation+" != "+str(val[index])+" ) or ("+str(key)+operation+" = "+str(val[index])
        else:
            condition = condition + "(("+str(key)+operation+operator+str(val[index])+" and "+str(key)+operation+" != "+str(val[index])+" ) or ("+str(key)+operation+" = "+str(val[index])+" and "

    condition = condition + ")"*size*2
    return condition

def __range_condition_on_cluster__(index_query:Dict[str,np.array]) ->str:
    
    conditions = []
    size = len(index_query.keys())
    items = list(index_query.items())
    for i in range(size):
        key,val = items[i]
        conditions.append("("+str(key)+"max"+" >= "+str(val[0])+" and "+str(key)+"min"+" <= "+str(val[1])+")")
    condition = " and ".join(conditions)

    return condition
        

def __range_on_index__(index_query:Dict[str,np.array]) ->str:
    conditions=[]
    for key,val in index_query.items():
        condition = "("+str(key)+"max >= "+str(val[0])+" and "+str(key)+"min <= "+str(val[1])+")"
        conditions.append(condition)
    return " and ".join(conditions)

def get_data(sql_db,dataset_name,columns,target):
    cnx = Connection(sql_db)
    cur = cnx.conn.cursor()

    query = sql_data.get_data(dataset_name,columns,target)
    cur.execute(query)

    rows = cur.fetchall()
    X =[]
    y=[]
    for row in rows:
        X.append(list(row[:-1]))
        y.append(int(row[-1]))
    return X,y
def _size_database_(sql_db,dataset):
    cnx = Connection(sql_db)
    cur = cnx.conn.cursor()
    
    query_strata = sql_data.__size__(dataset)
    cur.execute(query_strata)
    x = cur.fetchone()[0]
    

    return x

def __one_table_query__(sql_db,dataset:str,operator:str,query:Dict[str,np.array]):
    sql_range = __range_to_sql__(query)
    
    cnx = Connection(sql_db)
    cur = cnx.conn.cursor()
    
    query_strata = sql_data.__execute_query_one_table__(dataset,operator,sql_range)
    cur.execute(query_strata)
    x = cur.fetchone()[0]
    

    return x

def __regular_query_execution__(sql_db,dataset:str,operator:str,query:Dict[str,np.array],index_query:Dict[str,np.array]):
    sql_range = __range_to_sql__(query)
    
    cnx = Connection(sql_db)
    cur = cnx.conn.cursor()
    
    
    query_stratas_nums,_ = __get_clusters_Q__(sql_db,dataset,query,index_query)
    res = 0
    res_s =[]
    count =0
    for i in query_stratas_nums:#range(number_stratas):
        query_strata = sql_data.__execute_query_strata__(dataset,operator,i,sql_range)
        cur.execute(query_strata)
        x = cur.fetchone()[0]
        if x:
            count+=1
            res+= int(x)
            res_s.append(int(x))

    return res,res_s

def __get_clusters_Q__(sql_db,dataset:str,query:Dict[str,np.array],index_query:Dict[str,np.array]):
    
    sql_index_range_ = __range_condition_on_cluster__(index_query)
    
    cnx = Connection(sql_db)
    cur = cnx.conn.cursor()
    # execute the index to get all stratified samples that satisfy the given conditions
    
    query_nums_stratas = sql_index.__clusters_in_range__(dataset,sql_index_range_)
    cur.execute(query_nums_stratas)
    rows = cur.fetchall()
    
    num_,weight_ = __strata_weight_with_inclusion_probability(sql_db,dataset,rows,query)
    return num_,weight_
    
def __get_clusters__(sql_db,dataset:str,query:Dict[str,np.array],index_query:Dict[str,np.array]):
    
    sql_index_range_min = __range_condition_on_index__(index_query,"min")
    sql_index_range_max = __range_condition_on_index__(index_query,"max")
    
    cnx = Connection(sql_db)
    cur = cnx.conn.cursor()
    # execute the index to get all stratified samples that satisfy the given conditions
    
    query_nums_stratas = sql_index.__stratas_in_range__(dataset,sql_index_range_min,sql_index_range_max)
    cur.execute(query_nums_stratas)
    rows = cur.fetchall()
    
    num_,weight_ = __strata_weight_with_inclusion_probability(sql_db,dataset,rows,query)
    return num_,weight_
    



def __sampling__(sql_db,dataset_name,operator,nums,weights,query,epsilon,allocation,delta_p):
    if len(nums) == 0:
        return 0
    if allocation >=len(weights):
        allocation = len(weights) - 1 
    if allocation <= 0:
        allocation = 1
        
    # CHECK Here i removed the N_min so no need for joint decision
    s_R = np.sum(weights)
    
    ###########################################################################
    ## This is the part that was added compared to the basic AQP to make it DP
    ###########################################################################
    em_probabilities =[]
    for weight in weights:
        temp = (epsilon/allocation) * (weight/s_R) # Here divided by b be cause we will make b choices
        temp = temp / (2 * delta_p) 
        probability = np.exp(temp)
        em_probabilities.append(probability)
        
    # Normalize
    em_probabilities = [float(pro/np.linalg.norm(em_probabilities, ord=1)) for pro in em_probabilities]
    ###########################################################################

    # Creating cumulative ranges require Sorting
    sorted_num =[]
    sorted_w =[]
    sorted_R = []
    sorted_em_probabilities = []
    for  i in sorted(enumerate(em_probabilities),key=lambda x:x[1]):
        sorted_num.append(nums[i[0]])
        sorted_R.append(weights[i[0]])
        sorted_w.append(weights[i[0]]/s_R)
        sorted_em_probabilities.append(i[1])   
    
    # create cumulative ranges
    ranges=[]
    for i in range(len(sorted_em_probabilities)):
        w = sorted_em_probabilities[i]
        r = np.sum(sorted_em_probabilities[0:i]) + w
        if r > w:
            ranges.append(r)
        else:
            ranges.append(w)
    
    results_each_cluster =[]
    R_clusters_sampled=[]
    A_clusters_sampled= []
    
    for _ in range(allocation):
        x = random.random()
            
        cluster_index = [num for num,range in enumerate(ranges) if range > x ][0]
        R_clusters_sampled.append(sorted_R[cluster_index])
        
        sql_range = __range_to_sql__(query)

        # FIXME operator fixed to count
        v =__result_query_strata__(sql_db,dataset_name,operator,sorted_num[cluster_index],sql_range)   
        
        if v:
            if sorted_w[0] != 0:
                p = sorted_w[cluster_index]
                v_= float(v)/p
                results_each_cluster.append(v_)
            else:

                results_each_cluster.append(int(float(v)))
            A_clusters_sampled.append(v)
        else:
            A_clusters_sampled.append(0)
            results_each_cluster.append(0)
    
    
    results = np.mean(results_each_cluster)

    return results,results_each_cluster,R_clusters_sampled,A_clusters_sampled



def __result_query_strata__(sql_db,name:str,operator:str,strata:int,sql_range:str):
    cnx = Connection(sql_db)
    cur = cnx.conn.cursor()
    query_strata = sql_data.__execute_query_strata__(name,operator,strata,sql_range)
    cur.execute(query_strata)
    return cur.fetchone()[0]

def __strata_weight_with_inclusion_probability(sql_db,table_name:str,rows,query):
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()
    num_=[]
    weight_ =[]
    for row in rows:
        num_s = row[0]
        p = 1.0
        for dim,range_ in query.items():
            q = sql_index.__get_probability_dim_value_greater__(table_name,num_s,dim,str(range_[0]))
            cursor.execute(q)
            p_g = float(cursor.fetchone()[0])
            q = sql_index.__get_probability_dim_value_greater__(table_name,num_s,dim,str(range_[1]+1))
            cursor.execute(q)
            p_g_2 = float(cursor.fetchone()[0])
            p= p * (p_g- p_g_2)
        if p>0:
            num_.append(num_s)
            weight_.append(p)
    return num_,weight_

