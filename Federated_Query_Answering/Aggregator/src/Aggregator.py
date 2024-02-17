import Pyro5.api
from workload.domain import Domain
from workload.workload_generator import direct_random_range_queries

import pandas as pd
from tqdm import tqdm
import numpy as np
import math
from time import time,sleep

from threading import Thread 
import asyncio
import argparse

from mpyc.runtime import mpc
# from mpyc.runtime import mpc
from mpyc.sectypes import SecInt,SecFxp
import mpyc.numpy as np_s

from ortools.linear_solver import pywraplp


class Aggregator:
    def __init__(self,dataset_name:str,domain,number_data_providers:int,ip):
        self.dataset_name= dataset_name
        self.dataset_domain= domain

        self.number_data_provider = number_data_providers
        # Lunch the remote object for handling a query
        daemon = Pyro5.server.Daemon(host=ip)     
        uri = daemon.register(Query_Aggregator(self,number_data_providers))    
        ns = Pyro5.api.locate_ns()             
        
        ns.register("query_aggregator", uri)
        
        # Keep un instance
        self.query_aggregator = Pyro5.api.Proxy("PYRONAME:query_aggregator")

        #Starting the demon does stop the current thread
        Thread(target=daemon.requestLoop).start()
        
    def __start__(self):

        self.__create_workload__()
        self.__sampling_rate__()
        self.__epsilon__()

    async def __get_smc_results__(self,epsilon):
        
        
        secint = mpc.SecInt(32)
        time_ext_1 = time()
        
        time_ext_1 = time_ext_1 - time()
        sec_sens_est = [secint(x) for x in [0,0]] # won't affect the results
        print("before sharing")
        sec_sens_est =  mpc.input(sec_sens_est)
        print("after sharing")


        total_est = secint(0)
        total_sens = secint(0)
        sensis = []
        # print("received inputs len: "+str(len(secured_sens_est))+" -------")
        print("received inputs len: "+str(len(sec_sens_est))+" -------")
        for x in sec_sens_est:
            total_est = total_est + x[1]
            print("looping")
            sensis.append(x[0])
        
        total_sens = mpc.max(sensis)
        # total_sens = int(await mpc.output(total_sens))
        # loop2 = asyncio.get_event_loop()
        print("after selection the max")
        max_sens =0
        try:
            max_sens = await mpc.output(total_sens)
            total_sum = await mpc.output(total_est)
        except Exception as e:
            print(e)
        print("max_sens est: ", max_sens)
        print("total_est est: ", total_sum)
        # input("is result ok ?")
        # print("here max sensi: ",mpc.output([total_sens,total_est]))
        # noise = np.random.laplace(total_sens,0)
        noise = np_s.np.random.laplace(0,max_sens/epsilon)
        total_dp = total_sum + noise
        time_ext_2 = time()
        
        # print("Here: after adding noise")
        # total_est = total_est + noise
        # est_dp  = await  mpc.output(total_est)
        # print(est_dp)
        
        # mpc.run(mpc.shutdown())
        # time_ext_2 = time_ext_2 - time()
        print("done: ",max_sens)
        return total_dp,noise,total_sum
        

    
    def log_approximation(self,x, terms=10):
        result = mpc.SecFxp(0, x.frac_length)  # Initialize result with the same precision as x

        for n in range(1, terms + 1):
            n = mpc.SecFxp(n, x.frac_length)
            term = ((mpc.SecFxp(-1, x.frac_length)) ** (n + mpc.SecFxp(-1, x.frac_length))) * ((x - mpc.SecFxp(1, x.frac_length)) ** n) / n
            result += term

        return result
    def laplace_noise(self,scale,loc):
        u = mpc.random.uniform(sectype=SecFxp(32),a=0,b=1) - 0.5
        sign = mpc.sgn(u - 0.5)
        scale_fxp = mpc.convert(scale,SecFxp(32))
        r_n = loc - scale_fxp * sign * self.log_approximation(1 - 2 * mpc.abs(u - 0.5))
    
        return r_n

    def laplace_noise_2(self,scale,loc):
        
        secint = mpc.SecInt(32)
        
        u = np.random.uniform(0, 1, 1)
        
       
        x =np.sign(u - 0.5) * np.log(1 - 2 * np.abs(u - 0.5))
        x = mpc.SecFxp(x[0])
        y = mpc.SecFxp(loc)
        print("this value: ",np.sign(u - 0.5) * np.log(1 - 2 * np.abs(u - 0.5)))
        scale_fxp = mpc.convert(scale,SecFxp(32),)
        laplace_sample = y - scale_fxp *  x
        return  laplace_sample

    def __query__(self,query_num,num_dim,iteration):
        sampling_rate= 0.05
        epsilon = 1
         
        
        name= self.dataset_name
        
            
        workload_gen= direct_random_range_queries(domain=self.dataset_domain, size=100, dim_size=num_dim, seed=0)
        # Concat queries into one array
        workload =[]
        for w in workload_gen[1]:
            workload.extend(w)
        
        w_query = workload[query_num]
        
        operator = "count"
        
        
        query,query_index = w_query.__query_dicts__()
        
        self.query_aggregator.__is_query_good__(name,operator,query,query_index)
        goods = self.query_aggregator.__get_goods__()
        
        if goods == self.number_data_provider :
            
            
            
                
            start_time = time()
            self.query_aggregator.__request_metas__(name,query,query_index,epsilon)
        
            self.query_aggregator.__send_allocation__(sampling_rate)
            
            # time_collab = time() - start_time
            # while self.query_aggregator.__not_done__(): # waiting for data providers to send their query results
            #     pass

            self.query_aggregator.__send_ARQP__(name,operator,query,epsilon)
            r_est = self.query_aggregator.__get_result__()
            r_dp = self.query_aggregator.__get_result_dp__()
            noise =self.query_aggregator.__get_noise__()
            
            t_approx = time() - start_time
                
            
            # To get the accurate answer
            self.query_aggregator.__send_query__(name,operator,query,query_index,"Simple",epsilon)
            r_exact = self.query_aggregator.__get_result__()

            # To get the accurate execution time on clustered table
            start_time = time()
            self.query_aggregator.__send_query__(name,operator,query,query_index,"Clustered",epsilon)
            _ = self.query_aggregator.__get_result__()

            t_exact = time() - start_time

            

            data_ = {
                        "res" : [r_exact],
                        "res_est":[r_est],
                        "res_dp" : [r_dp],
                        "noise": [noise],
                        "time_a" : [t_approx],
                        "time_e" : [t_exact]
                        }
            df = pd.DataFrame(data_)
            df.to_csv("normal_query_"+str(query_num)+"_"+str(iteration)+"_"+name+"_sce_2_"+str(sampling_rate)+"_"+"dims_"+str(num_dim)+"_"+operator+".csv",index=False)
            
    def __query_mpc__(self,query_num,num_dim,iteration):
        sampling_rate= 0.05
        epsilon = 1
         
        print("here")
        name= self.dataset_name
        
            
        workload_gen= direct_random_range_queries(domain=self.dataset_domain, size=100, dim_size=num_dim, seed=0)
        # Concat queries into one array
        workload =[]
        for w in workload_gen[1]:
            workload.extend(w)
        
        w_query = workload[query_num]
        
        operator = "count"
        
        
        query,query_index = w_query.__query_dicts__()
        
        self.query_aggregator.__is_query_good__(name,operator,query,query_index)
        goods = self.query_aggregator.__get_goods__()
        
        if goods == self.number_data_provider :
            
            
            
                
            start_time = time()
            self.query_aggregator.__request_metas__(name,query,query_index,epsilon)
        
            self.query_aggregator.__send_allocation__(sampling_rate)
            
            # time_collab = time() - start_time
            # while self.query_aggregator.__not_done__(): # waiting for data providers to send their query results
            #     pass

            self.query_aggregator.__async_ARQP__(name,operator,query,epsilon)
            loop = asyncio.get_event_loop()
            r_dp,noise,r_est  = loop.run_until_complete(self.__get_smc_results__(0.8))
            
            t_approx = time() - start_time
                
            
            # To get the accurate answer
            self.query_aggregator.__send_query__(name,operator,query,query_index,"Simple",epsilon)
            r_exact = self.query_aggregator.__get_result__()

            # To get the accurate execution time on clustered table
            start_time = time()
            self.query_aggregator.__send_query__(name,operator,query,query_index,"Clustered",epsilon)
            _ = self.query_aggregator.__get_result__()

            t_exact = time() - start_time

            

            data_ = {
                        "res" : [r_exact],
                        "res_est":[r_est],
                        "res_dp" : [r_dp],
                        "noise": [noise],
                        "time_a" : [t_approx],
                        "time_e" : [t_exact]
                        }
            df = pd.DataFrame(data_)
            df.to_csv("smc_query_"+str(query_num)+"_"+str(iteration)+"_"+name+"_sce_2_"+str(sampling_rate)+"_"+"dims_"+str(num_dim)+"_"+operator+".csv",index=False)
            


    def __mpc_test__(self):
        
        sampling_rate= 0.2
        epsilon = 1
         
        number_dims =len(self.dataset_domain.attrs)
        name= self.dataset_name
        operators = ["sum","count"]
        runs = 10
        for num_dim in tqdm(np.arange(2,np.min([number_dims,number_dims])),"Dimensions loop : ",leave=True):
            
            workload_gen= direct_random_range_queries(domain=self.dataset_domain, size=100, dim_size=num_dim, seed=0)
            # Concat queries into one array
            workload =[]
            for w in workload_gen[1]:
                workload.extend(w)
            
            
            for operator in tqdm(operators,"operator loop : ",leave=True):
                results_est = []
                results_dp = []
                results_ex = []
                times_approx = []
                times_normal = []

                for i in tqdm(np.arange(0,len(workload)), " Query loop : ",leave=True):
                    w_query=workload[i]
                    
                    query,query_index = w_query.__query_dicts__()
                    
                    self.query_aggregator.__is_query_good__(name,operator,query,query_index)
                    goods = self.query_aggregator.__get_goods__()
                    
                    if goods == self.number_data_provider :
                        
                        
                        res_est=0
                        res_dp =0
                        res_ex = 0 
                        time_e_approx=0
                        time_e_exact=0

                        for _ in tqdm (np.arange(0,runs), " Iterations loop : "):
                        
                            
                            start_time = time()
                            self.query_aggregator.__request_metas__(name,query,query_index,epsilon)
                        
                            self.query_aggregator.__send_allocation__(sampling_rate)
                        
                            # while self.query_aggregator.__not_done__(): # waiting for data providers to send their query results
                            #     pass

                            self.query_aggregator.__async_ARQP__(name,operator,query,epsilon)
                            loop = asyncio.get_event_loop()
                            r_dp,t_extra  = loop.run_until_complete(self.__get_smc_results__())
                            
                            
                            t_approx = time() - start_time - t_extra

                            
                            if r_dp == r_dp:
                                res_dp+= int(r_dp/runs)
                            else:
                                res_dp+= 0
                            res_est+= 0
                            
                            
                            time_e_approx += t_approx / runs
                            
                        
                        # To get the accurate answer
                        self.query_aggregator.__send_query__(name,operator,query,query_index,"Simple",epsilon)
                        r_exact = self.query_aggregator.__get_result__()

                        # To get the accurate execution time on clustered table
                        start_time = time()
                        self.query_aggregator.__send_query__(name,operator,query,query_index,"Clustered",epsilon)
                        _ = self.query_aggregator.__get_result__()

                        t_exact = time() - start_time

                        time_e_exact += t_exact
                        res_ex+= r_exact

                        results_dp.append(res_dp)
                        results_est.append(res_est)
                        results_ex.append(res_ex)
                        times_approx.append(time_e_approx)
                        times_normal.append(time_e_exact)
                    # else:
                    #     print("Not good query:" + str(goods) +" num data providers : " + str(self.number_data_provider))
                    #     input("press any key to test other query")

                data_ = {
                            "res" : results_ex,
                            "res_est":results_est,
                            "res_dp" : results_dp,
                            "time_a" : times_approx,
                            "time_e" : times_normal
                            }
                df = pd.DataFrame(data_)
                df.to_csv("dims_sens_"+name+"_sce_2_"+str(sampling_rate)+"_"+"dims_"+str(num_dim)+"_"+operator+".csv",index=False)
                

    def __create_workload__(self):
        
        sampling_rate= 0.05
        epsilon = 1
         
        number_dims =len(self.dataset_domain.attrs) #-1
        name= self.dataset_name
        operators = ["sum","count"]
        runs = 5
        for num_dim in tqdm(np.arange(5,number_dims+1),"Dimensions loop : ",leave=True):
            
            workload_gen= direct_random_range_queries(domain=self.dataset_domain, size=100, dim_size=num_dim, seed=0)
            # Concat queries into one array
            workload =[]
            for w in workload_gen[1]:
                workload.extend(w)
            
            
            for operator in tqdm(operators,"operator loop : ",leave=True):
                results_est = []
                results_dp = []
                results_ex = []
                times_approx = []
                times_normal = []

                for i in tqdm(np.arange(0,len(workload)), " Query loop : ",leave=True):
                    w_query=workload[i]
                    try:
                        query,query_index = w_query.__query_dicts__()
                        
                        self.query_aggregator.__is_query_good__(name,operator,query,query_index)
                        goods = self.query_aggregator.__get_goods__()
                        
                        if goods == self.number_data_provider :
                            
                            
                            res_est=0
                            res_dp =0
                            res_ex = 0 
                            time_e_approx=0
                            time_e_exact=0

                            for _ in tqdm (np.arange(0,runs), " Iterations loop : "):
                            
                                
                                start_time = time()
                                self.query_aggregator.__request_metas__(name,query,query_index,epsilon)
                            
                                self.query_aggregator.__send_allocation__(sampling_rate)
                            
                                # while self.query_aggregator.__not_done__(): # waiting for data providers to send their query results
                                #     pass
                            
                                self.query_aggregator.__send_ARQP__(name,operator,query,epsilon)

                                r_est = self.query_aggregator.__get_result__()
                                r_dp = self.query_aggregator.__get_result_dp__()
                                
                                t_approx = time() - start_time

                                
                                if r_dp == r_dp:
                                    res_dp+= int(r_dp/runs)
                                else:
                                    res_dp+= 0
                                if r_est == r_est:
                                    res_est+= int(r_est/runs)
                                else:
                                    res_est+= 0
                                
                                
                                time_e_approx += t_approx / runs
                                
                            
                            # To get the accurate answer
                            self.query_aggregator.__send_query__(name,operator,query,query_index,"Simple",epsilon)
                            r_exact = self.query_aggregator.__get_result__()

                            # To get the accurate execution time on clustered table
                            start_time = time()
                            self.query_aggregator.__send_query__(name,operator,query,query_index,"Clustered",epsilon)
                            _ = self.query_aggregator.__get_result__()

                            t_exact = time() - start_time

                            time_e_exact += t_exact
                            res_ex+= r_exact

                            results_dp.append(res_dp)
                            results_est.append(res_est)
                            results_ex.append(res_ex)
                            times_approx.append(time_e_approx)
                            times_normal.append(time_e_exact)
                    except:
                        print("Error in running query ")
                    # else:
                    #     print("Not good query:" + str(goods) +" num data providers : " + str(self.number_data_provider))
                    #     input("press any key to test other query")

                data_ = {
                            "res" : results_ex,
                            "res_est":results_est,
                            "res_dp" : results_dp,
                            "time_a" : times_approx,
                            "time_e" : times_normal
                            }
                df = pd.DataFrame(data_)
                df.to_csv("dims_sens_"+name+"_sce_2_"+str(sampling_rate)+"_"+"dims_"+str(num_dim)+"_"+operator+".csv",index=False)
                
    def __sampling_rate__(self):
        #sampling_rates =[0.5,.1]
        epsilon = 1

        name= self.dataset_name
        operators = ["sum","count"]
        runs = 5
        for sampling_rate in tqdm(np.arange(0.05,0.25,0.05),"Sampling rate loop : ",leave=True):
            
            workload_gen= direct_random_range_queries(domain=self.dataset_domain, size=100, dim_size=4, seed=0)
            # Concat queries into one array
            workload =[]
            for w in workload_gen[1]:
                workload.extend(w)
            
            
            for operator in tqdm(operators,"operator loop : ",leave=True):
                results_est = []
                results_dp = []
                results_ex = []
                times_approx = []
                times_normal = []

                for i in tqdm(np.arange(0,len(workload)), " Query loop : ",leave=True):
                    w_query=workload[i]
                    try:
                        query,query_index = w_query.__query_dicts__()
                        
                        self.query_aggregator.__is_query_good__(name,operator,query,query_index)
                        goods = self.query_aggregator.__get_goods__()
                        
                        if goods == self.number_data_provider :
                            
                            
                            res_est=0
                            res_dp =0
                            res_ex = 0 
                            time_e_approx=0
                            time_e_exact=0

                            for _ in range(runs):
                            
                                
                                start_time = time()
                                self.query_aggregator.__request_metas__(name,query,query_index,epsilon)
                            
                                self.query_aggregator.__send_allocation__(sampling_rate)
                            
                                # while self.query_aggregator.__not_done__(): # waiting for data providers to send their query results
                                #     pass
                            
                                self.query_aggregator.__send_ARQP__(name,operator,query,epsilon)

                                r_est = self.query_aggregator.__get_result__()
                                r_dp = self.query_aggregator.__get_result_dp__()
                                
                                t_approx = time() - start_time

                                
                                if r_dp == r_dp:
                                    res_dp+= int(r_dp/runs)
                                else:
                                    res_dp+= 0
                                if r_est == r_est:
                                    res_est+= int(r_est/runs)
                                else:
                                    res_est+= 0
                                
                                
                                time_e_approx += t_approx / runs
                                
                            
                            # To get the accurate answer
                            self.query_aggregator.__send_query__(name,operator,query,query_index,"Simple",epsilon)
                            r_exact = self.query_aggregator.__get_result__()

                            # To get the accurate execution time on clustered table
                            start_time = time()
                            self.query_aggregator.__send_query__(name,operator,query,query_index,"Clustered",epsilon)
                            _ = self.query_aggregator.__get_result__()

                            t_exact = time() - start_time

                            time_e_exact += t_exact
                            res_ex+= r_exact

                            results_dp.append(res_dp)
                            results_est.append(res_est)
                            results_ex.append(res_ex)
                            times_approx.append(time_e_approx)
                            times_normal.append(time_e_exact)
                    except:
                        print("Error in running query ")

                data_ = {
                            "res" : results_ex,
                            "res_est":results_est,
                            "res_dp" : results_dp,
                            "time_a" : times_approx,
                            "time_e" : times_normal
                            }
                df = pd.DataFrame(data_)
                df.to_csv("sampling_sens_"+name+"_sce_2_"+str(sampling_rate)+"_"+"dims_4_"+operator+".csv",index=False)
        
    def __epsilon__(self):
        #sampling_rates =[0.5,.1]
        sampling_rate = 0.05

        name= self.dataset_name
        operators = ["sum","count"]
        runs = 5
        for epsilon in tqdm(np.arange(0.1,1.5,0.2)," Epsilon loop : ",leave=True):
            
            workload_gen= direct_random_range_queries(domain=self.dataset_domain, size=100, dim_size=4, seed=0)
            # Concat queries into one array
            workload =[]
            for w in workload_gen[1]:
                workload.extend(w)
            
            
            for operator in tqdm(operators,"operator loop : ",leave=True):
                results_est = []
                results_dp = []
                results_ex = []
                times_approx = []
                times_normal = []

                for i in tqdm(np.arange(0,len(workload)), " Query loop : ",leave=True):
                    w_query=workload[i]
                    try:
                        query,query_index = w_query.__query_dicts__()
                        
                        self.query_aggregator.__is_query_good__(name,operator,query,query_index)
                        goods = self.query_aggregator.__get_goods__()
                        # print(goods)
                        if goods == self.number_data_provider :
                            
                            
                            res_est=0
                            res_dp =0
                            res_ex = 0 
                            time_e_approx=0
                            time_e_exact=0

                            for _ in range(runs):
                            
                                
                                start_time = time()
                                self.query_aggregator.__request_metas__(name,query,query_index,epsilon)
                            
                                self.query_aggregator.__send_allocation__(sampling_rate)
                            
                                # while self.query_aggregator.__not_done__(): # waiting for data providers to send their query results
                                #     pass
                            
                                self.query_aggregator.__send_ARQP__(name,operator,query,epsilon)

                                r_est = self.query_aggregator.__get_result__()
                                r_dp = self.query_aggregator.__get_result_dp__()
                                
                                t_approx = time() - start_time

                                
                                if r_dp == r_dp:
                                    res_dp+= int(r_dp/runs)
                                else:
                                    res_dp+= 0
                                if r_est == r_est:
                                    res_est+= int(r_est/runs)
                                else:
                                    res_est+= 0
                                
                                
                                time_e_approx += t_approx / runs
                                
                            
                            # To get the accurate answer
                            self.query_aggregator.__send_query__(name,operator,query,query_index,"Simple",epsilon)
                            r_exact = self.query_aggregator.__get_result__()

                            # To get the accurate execution time on clustered table
                            start_time = time()
                            self.query_aggregator.__send_query__(name,operator,query,query_index,"Clustered",epsilon)
                            _ = self.query_aggregator.__get_result__()

                            t_exact = time() - start_time

                            time_e_exact += t_exact
                            res_ex+= r_exact

                            results_dp.append(res_dp)
                            results_est.append(res_est)
                            results_ex.append(res_ex)
                            times_approx.append(time_e_approx)
                            times_normal.append(time_e_exact)
                    except:
                        print("Error with this query")

                data_ = {
                            "res" : results_ex,
                            "res_est":results_est,
                            "res_dp" : results_dp,
                            "time_a" : times_approx,
                            "time_e" : times_normal
                            }
                df = pd.DataFrame(data_)
                df.to_csv("epsilon_sens_"+name+"_sce_2_"+str(sampling_rate)+"_"+"_ep_"+str(epsilon)+"_dims_4_"+operator+".csv",index=False)
    


                
                

class Query_Aggregator:
    def __init__(self,query_luncher,number_data_providers:int):
        # To identify data providers
        self.urls =[]
        self.ids = []

        self.query_luncher = query_luncher

        # For the allocation 
        self.weights = [0]*number_data_providers
        self.sizes = [0]*number_data_providers
        self.allocations = [0]*number_data_providers
        self.sampling_rate=0

        # Results
        self.results_dp = [0]*number_data_providers
        self.results = [0]*number_data_providers

        # To create a workload
        self.goods = [0]*number_data_providers


        # To make sure it received all needed messages
        #// manage to reset the counter after each cycle
        self.number_data_providers=number_data_providers
        self.messages_count = 0
    
    @Pyro5.api.expose
    def __not_done__(self):
        return self.messages_count == self.number_data_providers
    
    @Pyro5.api.expose
    def __get_id_data_provider__(self) ->int:
        id = len(self.ids)
        self.ids.append(id)
        return id
    
    @Pyro5.api.expose
    def __set_url_data_provider__(self,id,url_dt:str):
        # id = len(self.urls)

        # CHECK The providers should come in the order
        self.urls.append(url_dt)
        # self.ids[id] = id
        self.messages_count+=1
        print("Data providers : "+ str(id+1))
        # if self.messages_count == self.number_data_providers:
        #     self.query_luncher.__start__()
    
    
    def __set_weight_size_data_provider__(self,id_dt_pr:int,weight:float,size:int):
        self.weights[id_dt_pr] = weight
        self.sizes[id_dt_pr] =size
        self.messages_count+=1

    def __set_good__(self,id_dt_pr:int,bool_good:int):
        #print("data provider "+str(id_dt_pr)+" said : "+str(bool_good)+" good")
        self.goods[id_dt_pr] = bool_good
    
    @Pyro5.api.expose
    def __get_goods__(self):
        print(self.goods)
        return int(np.sum(self.goods))



    @Pyro5.api.expose
    def __is_query_good__(self,name,operator,query,query_index):
        while self.number_data_providers != len(self.urls):
           pass
        for _,url in enumerate(self.urls):
            redo = True
            while redo:
                try:
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    id,bool_good = data_provider.__is_query_good__(name,operator,query,query_index)
                    self.__set_good__(id,bool_good)
                    redo = False
                except Exception  as e:
                    print(url)
                    # print(self.urls)
                    print(e)
                    input("Problem is good")


    @Pyro5.api.expose
    def __request_metas__(self,name,query,query_index,epsilon):
        for _,url in enumerate(self.urls):
            redo = True
            while redo:
                try:
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    id,avg_R_cluster_DP,count_cluster_DP = data_provider.__get_weight_and_size__(name,query,query_index,epsilon)
                    self.__set_weight_size_data_provider__(id,avg_R_cluster_DP,count_cluster_DP)
                    redo = False
                except:
                    print(url)
                    # print(self.urls)
                    input("Problem metas")

    
    @Pyro5.api.expose
    def __async_ARQP__(self,name,operator,query,epsilon):
        for i,url in enumerate(self.urls):
            redo  =True
            while redo:
                try:
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)

                    data_provider.__ARQP_SMC__(name,operator,query,epsilon)
                    # id,res_dp,est = data_provider.__ARQP__(name,operator,query,epsilon)
                    # self.__set_results_dp__(id,res_dp)
                    # self.__set_results__(id,est)
                    redo = False
                except Exception as e :
                    print(e)
                    # print(url)
                    # print(self.urls)
                    print("Problem hna ?")


    @Pyro5.api.expose
    def __send_ARQP__(self,name,operator,query,epsilon):
        for i,url in enumerate(self.urls):
            redo  =True
            while redo:
                try:
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    id,res_dp,est = data_provider.__ARQP__(name,operator,query,epsilon)
                    self.__set_results_dp__(id,res_dp)
                    self.__set_results__(id,est)
                    redo = False
                except:
                    print("")
    
    @Pyro5.api.expose
    def __send_query__(self,name,operator,query,query_index,strategy,epsilon):
        id  =-1 
        res = 0
    
        for _,url in enumerate(self.urls):
            re_do = True
            while re_do:
                try: 
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    id,res = data_provider.__execute_query__(name,operator,query,query_index,strategy,epsilon)
                    self.__set_results__(id,res)
                    re_do = False
                except Exception as e:
                    print(e)

        

    def __solve_allocation_optimization__(self,sampling_rate:float):
        B = int(np.sum(self.sizes)*sampling_rate)
        
        solver = pywraplp.Solver.CreateSolver("SAT")
        
        # Creating LP variables (Allocations)
        allocations =[]
        for i in range(len(self.weights)):
            #CHECK here i set the upper(lower) bound to 20% (5%) from sampling rate of 10%
            
            l = 2
            u = int(self.sizes[i])
            allocations.append(solver.IntVar(l,u,"a_"+str(i)))
        
        # Constraint : Sum(bi) = B <==>  B >= Sum(bi) <= B
        cons = solver.Constraint(B, B)
        for i in range(len(allocations)):
            cons.SetCoefficient(allocations[i], 1)
        

        # Objective function
        minimize_objective = solver.Objective()
        for i in range(len(allocations)):
            minimize_objective.SetCoefficient(allocations[i],-self.weights[i])
        
        minimize_objective.SetMinimization()

        solver.Solve()


        self.allocations = [np.max([int(np.ceil(allocation.SolutionValue())),1]) for allocation in allocations]

        
    
    @Pyro5.api.expose
    def __get_result__(self):
        return int(np.sum(self.results))
    
    @Pyro5.api.expose
    def __get_noise__(self):
        dp_r  = np.asarray(self.results_dp)
        _r = np.asarray(self.results)
        noises = dp_r - _r
        print(noises)
        return int(np.sum(noises))
    
    @Pyro5.api.expose
    def __get_result_dp__(self):
        return int(np.sum(self.results_dp))

    @Pyro5.api.expose
    def __send_allocation__(self,sampling_rate):
        self.__solve_allocation_optimization__(sampling_rate)
        for id_dt,url in enumerate(self.urls):
            redo = True
            while redo:
                try:
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    allocation =int(self.allocations[id_dt])
                    data_provider.__set_allocation__(allocation)
                    redo = False
                except:
                    print("")
            
    
    
    def __set_results__(self,id_dt_pr:int,result:float):
        self.results[id_dt_pr] = result

    def __set_results_dp__(self,id_dt_pr:int,result:float):
        self.results_dp[id_dt_pr] = result
    



    
