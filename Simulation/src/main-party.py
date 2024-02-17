# SM packages
from mpyc.runtime import mpc
from mpyc.runtime import mpc

import pandas as pd
import numpy as np
import os
import time

from workload.workload_generator import direct_random_range_queries
from workload.domain import Domain



def __run_workload__(data_num:int):
    
    path = os.getcwd()+"/src/Data/data_"+str(data_num)+".csv"
    data = pd.read_csv(path)

    dims = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]
    dims = (str(dim) for dim in dims)
    full_domain = (74, 9, 100, 16, 16, 7, 15, 6, 5,2, 100,100,99,42, 2)

    data_domain = Domain(("0","1","2","3","4","5","6","7","8","9","10","11","12","13","14"),full_domain)
    workloads = direct_random_range_queries(domain=data_domain, size=5, dim_size=1, seed=0)
    
    workload = []
    for w in workloads[1]:
        workload.append(w[0])
    #sizes = [10,50,100,500,1000,10000]
    time_share_res_eval =[]
    time_share_res_mpc =[]
    time_share_rows_eval= []
    time_share_rows_mpc= []
    query_size =[]
    for query in workload:
        res = _get_query_result_(query,data,False)
        query_size.append(res)
        if res:
            time_mpc,time_eval = __execute_query__(query,data)
            time_share_res_mpc.append(time_mpc)
            time_share_res_eval.append(time_eval)
            time_mpc,time_eval = __execute_query_rows__(query,data)
            time_share_rows_eval.append(time_eval)
            time_share_rows_mpc.append(time_mpc)
            # time_share_rows.append(time_saqe)
            # speed_up_r.append(time_saqe/time_normal)
    data ={
        "QS":query_size,
        "TSRes_eval":time_share_res_eval,
        "TSRes_mpc":time_share_res_mpc,
        "TSR_mpc":time_share_rows_mpc,
        "TSR_eval":time_share_rows_eval
    }
    df = pd.DataFrame(data)
    # df.to_csv("res_simu_smc.csv",index=False)



def __execute_query__(query,data):
    start_time = time.time()
    res = _get_query_result_(query,data,False)
    res_cost_time= time.time() -  start_time
    # print(res)
    start_time = time.time()
    mpc.run(share_res(res))
    # print("here2")
    return time.time() - start_time,res_cost_time

def __execute_query_rows__(query,data):
    start_time = time.time()
    res = _get_query_result_(query,data,True)
    time_eval = time.time() - start_time
    start_time = time.time()
    mpc.run(share_rows(res))
    return time.time() - start_time, time_eval

async def share_res(res):
    secint = mpc.SecInt(64)

    await mpc.start()

    sec_res = secint(int(res))
    secured_res_s = mpc.input(sec_res)
    # print("here")
    total_response = secint(0)
    for x in secured_res_s:
        total_response = total_response + x
    
    # print('Total age:', await mpc.output(total_response))

    await mpc.shutdown()

async def share_rows(rows):
    secint = mpc.SecInt(16)

    await mpc.start()

    sec_rows = [secint(x) for x in rows]
    secured_rows = mpc.input(sec_rows)

    total_response = secint(0)
    for x in secured_rows:
        for  y in x:
            total_response = total_response + y
    
    print('Total res:', await mpc.output(total_response))
    #print('Maximum age:', await mpc.output(max_age))
    #print('Number of "elderly":', await mpc.output(above_avg))

    await mpc.shutdown()


def _get_query_result_(query,data,rows:bool):
    boolean_index = (data[query.conditions[0].attribute] >= query.conditions[0].start) & (data[query.conditions[0].attribute] <= query.conditions[0].end)
    for cond in query.conditions[1:]:
        boolean_index = boolean_index & (data[cond.attribute] >= cond.start) & (data[cond.attribute] <= cond.end )
    if rows :
        r_ = [x for x in boolean_index if x>0]
        
        return r_
    else:
        return np.sum(boolean_index)

__run_workload__(0)