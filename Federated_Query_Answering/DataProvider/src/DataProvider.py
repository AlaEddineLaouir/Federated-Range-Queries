import Pyro5.api
import uuid
import query_processing as qp
import numpy as np

import pickle5 as pickle
import os
import sys
import argparse

from threading import Thread 
from mpyc.runtime import mpc
from mpyc.runtime import mpc

from time import time
import asyncio
from adult_preprocessing import __main_adult_preprocessing__
from amazon_preprocessing import __main_amazon_preprocessing__

class Main_Data_Provider:
    def __init__(self,sql_db:str,data_version:int,ip):

        name = "data_provider_"+str(uuid.uuid1())
        print("My name is: "+ name)
        true_data = data_version == 0
        query_aggregator = Pyro5.api.Proxy("PYRONAME:query_aggregator")
        id =0
        redo = True
        while redo:
            try:
                id = query_aggregator.__get_id_data_provider__()
                redo = False
            except:
                print("didnt find aggregator")
                redo = True
        dt_pr = Data_Provider(id,name,sql_db,data_version,true_data)
 
        # starting the deamon does stop the current thread
        daemon = Pyro5.server.Daemon(host=ip )         
        ns = Pyro5.api.locate_ns()             
        uri = daemon.register(dt_pr)
        
        ns.register(name, uri)
        
        Thread(target=daemon.requestLoop).start()
        
        

class Data_Provider:
    def __init__(self,id:int,name:str,sql_db:str,data_version:int,true_data=False):
        
        
        N_min_size = .15 # set the N_min to 30% adulr 15 % amazon
        self.lp_ratio_R = 0.05
        self.lp_ratio_n = 0.05
        self.em_ratio = 0.1
        self.lap_ratio = 0.8


        self.id = id
        self.name = name

        # The boolean serves for the waiting loop
        self.nums =[]
        self.weights=[]
        self.received_allocation = False
        self.allocation=0

        # Keep un instance
        self.query_aggregator = Pyro5.api.Proxy("PYRONAME:query_aggregator")
        
        # Pre-process the datasets 
        self.sql_db= sql_db
        self.datasets =[]
        cluster_size = [0.01,0.005]
        for i,dataset in enumerate(["adult","amazon"]):
            
            # You can run this creation once, and then comment this function
            S,N,D,domain = globals()["__main_"+dataset+"_preprocessing__"](cluster_size[i],sql_db,data_version,not true_data)
            

            # Uncomment this only it you had already created the databases in previous test and it didn't get lost
            # domain =0
            # directory_path = os.getcwd()+"/src/Data/Adult/"+str(data_version)
            # file_path = os.path.join(directory_path, dataset+"_meta.metadata")
            # with open(file_path, 'rb') as f:
            #     data = pickle.load(f)
            #     S = data[0]
            #     N = data[1]
            #     D = data[2]
            #     domain = data[3]

            if i ==0:
                dataset = "adult_synth"
            
            

            N_min_ = int(N * N_min_size)
            delta_avg_R = np.abs(((N_min_ -1)/(N_min_*(N_min_+1)))-(D/(S-D)))
            delta_p = 1/(N_min_*(N_min_+1))
            delta_N_q = 1
            delta_est_sum = 1
            delta_est_count = 1
            self.datasets.append([dataset,[delta_avg_R,delta_N_q,delta_p,delta_est_count,delta_est_sum,S,N_min_,N]])

        # This must be done after pre-processing the data to signal its available to be queried
        redo  = True
        while redo:
            try:
                self.query_aggregator.__set_url_data_provider__(id,self.name)
                redo = False
            except:
                print("didnt find aggregator")
                redo = True
    
    
    
            
    def __get_delta_p__(self,dataset:str):
        for i,ds in enumerate(self.datasets):
            if ds[0] ==dataset:
                return self.datasets[i][1][2]

    
    def __get_S__(self,dataset:str):
        for i,ds in enumerate(self.datasets):
            if ds[0] ==dataset:
                return self.datasets[i][1][5]
    def __get_N_min__(self,dataset:str):
        for i,ds in enumerate(self.datasets):
            if ds[0] ==dataset:
                return self.datasets[i][1][6]
    def __get_N__(self,dataset:str):
        for i,ds in enumerate(self.datasets):
            if ds[0] ==dataset:
                return self.datasets[i][1][7]
    
    
   
   
    
    def __ls_distance_before_2__(self,est,A,R,sum_R,N_Q,u,S,D,k):
        
        
        delta_R = k*(1 - (1 - (1/S))**D)
        a = delta_R
        x= R+delta_R
        p_nei = x/(sum_R + a )
        est_nei = (k*u + A)/p_nei
        
        return np.abs(est - est_nei)

    def __ls_distance_after_2__(self,est,A,R,sum_R,N_Q,S,D,k):
        
        
        delta_R = k*(1 - (1 - (1/S))**D)
        sum_R_=(sum_R + delta_R)
        p_nei = R/sum_R_
        est_nei = A/p_nei
        
        return np.abs(est - est_nei)

    
    

    def __get_smooth_sensitivity_estimator_2__(self,dataset_name,epsilon,operator,est_s,R_s,A_s,D):
        
        u = 1
        
        gamma = 0.001
        beta = epsilon/(2* np.log(2/gamma))

        N_Q = len(self.weights)
        S =self.__get_S__(dataset_name)
        sum_R = np.sum(self.weights)
        
        # For each cluster compute smooth
        distance_max = 0

        for i,R in enumerate(R_s):
            est = est_s[i]
            A = A_s[i]
            
            dist_max = 0

            ## I'm not testing which distance is bigger am just taking the max distance after computation
            growing = True
            k = 0
            dist_prev = 0
            while growing:
                    x = np.exp((-beta)*k)
                    
                    dist = x * self.__ls_distance_before_2__(est,A,R,sum_R,N_Q,u,S,D,k)
                    
                    if dist_max <= dist :
                        dist_max = dist
                    elif dist_prev > dist :
                        growing = False
                    k+=1
                    dist_prev = dist
                    #print("distance at k = "+str(k)+" : "+str(dist))
            #else:
            growing = True
            k =0
            dist_prev = 0
            while growing:
                    x = np.exp((-beta)*k)
                    dist1  = x * self.__ls_distance_after_2__(est,A,R,sum_R,N_Q,S,D,k)
                    if dist_max <= dist1 :
                        dist_max = dist1
                    elif dist_prev > dist1:
                        growing = False
                    k+=1
                    dist_prev = dist1
                    #print("distance at k = "+str(k)+" : "+str(dist1))
            
            # if distance_max <= dist_max:
            #     distance_max = dist_max
            

            # New distance
            growing = True
            k =0
            p  = R/ np.sum(R_s)
            dist_prev = 0
            while growing:
                    x = np.exp((-beta)*k)
                    B = (k/p)
                    dist2  = x * B
                    if dist_max <= dist2 :
                        dist_max = dist2
                    elif dist_prev > dist2 :
                        growing = False
                    k+=1
                    dist_prev = dist2
            
            distance_max+=dist_max

        
        return int(distance_max/len(R_s))
    
    
        
    
    @Pyro5.api.expose
    def __set_allocation__(self,allocation:int):
        self.allocation = allocation
        self.received_allocation = True

    @Pyro5.api.expose
    def __get_weight_and_size__(self,dataset_name,query,query_index,epsilon):
        D = len(query)
        nums,weights =qp.__get_clusters_Q__(self.sql_db,dataset_name,query,query_index)
        
        # Save them for sampling
        self.nums = nums
        self.weights = weights
            
        

        # Make the number of cluster DP
        N_q_dp = len(nums)+np.random.laplace(0,1/epsilon*self.lp_ratio_n)
        
        N = self.__get_N__(dataset_name)
        S =self.__get_S__(dataset_name)
        N_min = self.__get_N_min__(dataset_name)
        
       
        
        count_cluster_DP = N_q_dp
        
        delta_R = (1 - (1 - (1/S))**D)
        sens_avg_1 = ( 1 - 1 / (S**D) ) / ( N_min + 1)
        sens_avg_2 = delta_R / N_min

        sens_avg = np.max([sens_avg_1,sens_avg_2])

        scale_avg_R_lp = sens_avg/(epsilon*self.lp_ratio_R)
            
        
        avg_R_cluster_DP = np.mean(weights)+np.random.laplace(0,scale_avg_R_lp)
        return self.id,avg_R_cluster_DP,count_cluster_DP


    @Pyro5.api.expose
    def __execute_query__(self,dataset_name,operator,query,query_index,strategy,epsilon):
        
        if strategy == "Simple":
            res = qp.__one_table_query__(self.sql_db,dataset_name,operator,query_index)
            if res is not None:
                res_dp = res 
            else:
                res_dp = 0 
            return self.id, res_dp
        elif strategy == "Clustered":
            res,_ = qp.__regular_query_execution__(self.sql_db,dataset_name,operator,query,query_index)
            if res is not None:
                res_dp = res 
            else:
                res_dp = 0 
            return self.id, res_dp
    
    @Pyro5.api.expose
    def __is_query_good__(self,dataset_name,operator,query,query_index):
        try:
            print(self.sql_db)
            nums,_ =qp.__get_clusters__(self.sql_db,dataset_name,query,query_index)
            _,res_s = qp.__regular_query_execution__(self.sql_db,dataset_name,operator,query,query_index)

            N_min = self.__get_N_min__(dataset_name)
            small_res = len([r for r in res_s  if r < 50])
        except Exception as e:
            print(e)

        # Basically a query is good, if it trigger the approximation and each cluster has a significant answer
        return self.id, (len(nums)>=N_min and small_res == 0)



    
    @Pyro5.api.expose
    @Pyro5.api.oneway
    def __ARQP_SMC__(self,dataset_name,operator,query,epsilon):
        
        
        D = len(query)
        
        N_min = self.__get_N_min__(dataset_name)
        delta_p = 1/(N_min*(N_min + 1))
        

        res_dp =0
        if len(self.nums) != 0 :
            est,est_s,R_s,A_s = qp.__sampling__(self.sql_db,dataset_name,operator,self.nums,self.weights,query,epsilon*self.em_ratio,self.allocation,delta_p)
            
            epsilon = self.lap_ratio*epsilon
            smooth_sensitivity = self.__get_smooth_sensitivity_estimator_2__(dataset_name,epsilon,operator,est_s,R_s,A_s,D)
            scale = 2*smooth_sensitivity/epsilon
            print("scale : "+str(scale))
            print("est : "+str(est))
            self.__send_data_smc__(scale,est)
         
            
    
    def __send_data_smc__(self,sens,est):
        print("start data sharing!")
        
        secint = mpc.SecInt(32)
        time_ext_1 = time()
        time_ext_1 = time_ext_1 - time()
        
        print("before sharing")
        try:
            
            sec_sens_est = [secint(x) for x in [int(sens),int(est)]]
            sec_sens_est =  mpc.input(sec_sens_est)
        except Exception as e:
            print(e)

        sensis = []
        print("before looping")
        total_est = secint(0)
        try:
            for x in sec_sens_est:
                total_est = total_est + x[1]
                print("looping")
                sensis.append(x[0])
        except Exception as e:
            print("Error in Looping",e)
        
        total_sens = mpc.max(sensis)
        
        try:
            res = mpc.output(total_sens)
            res2 = mpc.output(total_est)
        except Exception as e:
            print(e)
        print("max_sens est: ", res)
        print("data sent!")
        time_ext_2 = time()
        mpc.run(mpc.shutdown())
        time_ext_2 = time_ext_2 - time()
    
    @Pyro5.api.expose
    def __ARQP__(self,dataset_name,operator,query,epsilon):
        
        D = len(query)
        
        N_min = self.__get_N_min__(dataset_name)
        delta_p = 1/(N_min*(N_min + 1))
        

        res_dp =0
        if len(self.nums) != 0 :
            est,est_s,R_s,A_s = qp.__sampling__(self.sql_db,dataset_name,operator,self.nums,self.weights,query,epsilon*self.em_ratio,self.allocation,delta_p)
            
            epsilon = self.lap_ratio*epsilon
            smooth_sensitivity = self.__get_smooth_sensitivity_estimator_2__(dataset_name,epsilon,operator,est_s,R_s,A_s,D)
            scale = 2*smooth_sensitivity/epsilon
            res_dp = est+ np.random.laplace(0,scale)
            return self.id,res_dp,est
        return self.id,0,0

        
#CHECK uncomment this for SMC tests 
if __name__ == "__main__":
    args  = sys.argv

    db = args[1]
    ver = int(args[2])
    ip = args[3]
    if ver > 0:
        db+= str(ver+1)
        
    #TODO uncomment these for MPC tests
    
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(mpc.start())
    
    Main_Data_Provider(db,ver,ip)







