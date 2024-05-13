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
from ortools.linear_solver import pywraplp
from workload.query import QueryCondition,Query


class NaiveBayes:
  def __init__(self,dataset_name,columns,domain,target_column,target_column_domain,number_data_providers,ip,method,eps_g=10):
    # data related
    self.dataset_name= dataset_name
    self.domain = domain
    self.columns = columns
    self.target_column = target_column
    self.target_column_domain=target_column_domain
    if "COUNT" in method :
       self.op = "count"
    else:
       self.op="sum"

    total_queries = 0
    for d in domain:
       total_queries += d
    total_queries = total_queries * target_column_domain + target_column_domain + 1

    # HINT: if advanced composition, uncomment 
    if "Sequential" in method :
        self.epsilon = eps_g / total_queries
        self.ad_ep = eps_g / total_queries 
        self.gamma = (10**-3) / total_queries 

    elif "Coalition" in method :
        self.epsilon = eps_g
        self.ad_ep = eps_g 
        self.gamma = (10**-3) 
    else:
        self.epsilon = eps_g# / total_queries
        self.ad_ep = eps_g/(2 * np.sqrt(2 * total_queries*np.log(1/(5 * 10**-4))) )
        self.gamma = (5 * 10**-4 )/total_queries



    # Bayes Model

    self.likelihoods= {}
    self.class_priors = {}
    self.pred_priors ={}

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

  
  def __set_eps__(self,eps):
    total_queries = 0
    for d in self.domain:
       total_queries += d
    total_queries = total_queries * self.target_column_domain + self.target_column_domain + 1

    self.epsilon = eps / total_queries
     
  def __fit__(self):
    self.class_priors = {}
    columns = self.columns
    self.query_aggregator.__get_data_size__(self.dataset_name,self.ad_ep) # First count query
    self.rows_count = self.query_aggregator.__get_result__()
    
    print("         Getting the size            ")

    for ic, col in enumerate(columns):
      self.likelihoods[col] = {}
      self.pred_priors[col] = {}

      for member in range(self.domain[ic]):
        self.pred_priors[col].update({member: 0})
        
        for target_member in range(self.target_column_domain):
          
          self.likelihoods[col].update({str(member)+'_'+str(target_member):0})
					
          self.class_priors.update({target_member: 0})

    print(" Start Prior ")
    self._calc_class_prior_()
    print(" End Prior ")
	
    print(" Start Likelihoods ")
    self._calc_likelihoods_()
    print(" End Likelihoods ")

    # print(" Start PrePrior ")
    # self._calc_predictor_prior_()
    # print(" End PrePrior ")

  def _create_query_for_one_cond_(self,col,val):
     q = Query([QueryCondition(col,val,val)])
     return q.__query_dicts__()
  
  def _create_query_for_two_cond_(self,col,val,col2,val2):
     q = Query([QueryCondition(col,val,val),QueryCondition(col2,val2,val2)])
     return q.__query_dicts__()
     
  
  def _run_query_(self,query,query_index,epsilon,gamma):
    self.query_aggregator.__request_metas__(self.dataset_name,query,query_index,epsilon)
                            
    self.query_aggregator.__send_allocation__(.05)

    self.query_aggregator.__send_ARQP__(self.dataset_name,self.op,query,epsilon,gamma)

    r_dp = self.query_aggregator.__get_result_dp__()
    return r_dp
      
  def _count_rows_with_(self,target_col,member,epsilon,gamma):
    query,query_index = self._create_query_for_one_cond_(target_col,member)
    return self._run_query_(query,query_index,epsilon,gamma)
  
  def _get_count_where_(self,col,col_member,target_column,member,epsilon,gamma):
    query,query_index = self._create_query_for_two_cond_(col,col_member,target_column,member)
    return self._run_query_(query,query_index,epsilon,gamma)
     
        
     
  def _calc_class_prior_(self):
    for member in range(self.target_column_domain):
      value_count = self._count_rows_with_(self.target_column,member,self.ad_ep,self.gamma)
    #   self.class_priors.update({member: value_count / self.rows_count})
      self.class_priors[member] = value_count / self.rows_count

  def _calc_likelihoods_(self):
    
    for ic,col in enumerate(self.columns):
    #   print(col)
      
      for member in range(self.target_column_domain):
      
        outcome_count = self.class_priors[member] * self.rows_count # No need for more queries
        
        feat_likelihood = {}
        
        for col_member in range(self.domain[ic]):
          print(col," - ",member," - ",col_member)
          count_like = self._get_count_where_(col,col_member,self.target_column,member,self.ad_ep,self.gamma) # Another query
          feat_likelihood[col_member] = count_like
          
          #Predictor prior
          self.pred_priors[col][col_member]+=count_like/self.rows_count
        

        for feat_val, count in feat_likelihood.items():
        #   self.likelihoods[col].update({str(feat_val)+'_'+str(member):count/outcome_count}) 
          self.likelihoods[col][str(feat_val )+ '_' + str(member)] = count/outcome_count


  def _calc_predictor_prior_(self):
    for ic, col in enumerate(self.columns):
        feat_vals= {}
        for col_member in range(self.domain[ic]):
           feat_vals[col_member] = self._count_rows_with_(col,col_member,self.ad_ep,self.gamma)
      
        for feat_val, count in feat_vals.items():
            self.pred_priors[col].update({feat_val: count/ self.rows_count})	
            #self.pred_priors[col][feat_val] = count/ self.rows_count
  
  
  def _get_data_(self,column,target):
     self.query_aggregator.__get_data__(self.dataset_name,column,target)
     X,y = self.query_aggregator.__collect_data__()
     return X,y

  def valid_input(self,X):
    for i,x in enumerate(X):
       if x >= self.domain[i]:
          return False 
    return True   
  def predict(self, X,y):

    results = []
    X = np.array(X)
    y = np.array(y)
    y_clean = []

    for i,query in enumerate(X):
      if self.valid_input(query):
        y_clean.append(y[i])
        probs_outcome = {}
        for member in range(self.target_column_domain):
            prior = self.class_priors[member]
            likelihood = 1
            evidence = 1

            for feat, feat_val in zip(self.columns, query):
                likelihood *= self.likelihoods[feat][str(int(feat_val) )+ '_' + str(member)]
                evidence *= self.pred_priors[feat][feat_val]

            posterior = (likelihood * prior) / (evidence)

            probs_outcome[member] = posterior

        result = max(probs_outcome, key = lambda x: probs_outcome[x])
        results.append(result)

    return np.array(results),y_clean
  
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

        #Data
        self.Xs = [0]*number_data_providers
        self.Ys = [0]*number_data_providers

        # To create a workload
        self.goods = [0]*number_data_providers


        # To make sure it received all needed messages
        #// manage to reset the counter after each cycle
        self.number_data_providers=number_data_providers
        self.messages_count = 0
    
    
    @Pyro5.api.expose
    def __get_data_size__(self,name,epsilon):
        while self.number_data_providers != len(self.urls):
           pass
        for i,url in enumerate(self.urls):
            re_do = True
            while re_do:
                try: 
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    id,res = data_provider.__get_data_size__(name,epsilon)
                    self.__set_results__(id,res)
                    re_do = False
                except Exception as e:
                    print(e)
           
       
    
    @Pyro5.api.expose
    def __not_done__(self):
        return self.messages_count == self.number_data_providers
    
    
    @Pyro5.api.expose
    def __get_data__(self,name,column,target):
        while self.number_data_providers != len(self.urls):
           pass
        for _,url in enumerate(self.urls):
            redo = True
            while redo:
                try:
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    id,X,y = data_provider.get_data(name,column,target)
                    self.Xs[id] = X
                    self.Ys[id] = y
                    redo = False
                except Exception  as e:
                    print(url)
                    # print(self.urls)
                    print(e)
                    # input("Problem get data")

       
       
    
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
                    # input("Problem is good")


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
                    # input("Problem metas")

    
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
    def __send_ARQP__(self,name,operator,query,epsilon,gamma=10**-3):
        for i,url in enumerate(self.urls):
            redo  =True
            while redo:
                try:
                    data_provider = Pyro5.api.Proxy("PYRONAME:"+url)
                    id,res_dp,est = data_provider.__ARQP__(name,operator,query,epsilon,gamma)
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
        B = np.max([B,self.number_data_providers])
        if self.number_data_providers > 1:
            solver = pywraplp.Solver.CreateSolver("SAT")
            
            # Creating LP variables (Allocations)
            allocations =[]
            for i in range(len(self.weights)):
                #CHECK here i set the upper(lower) bound to 20% (5%) from sampling rate of 10%
                
                l = 2
                u = int(self.sizes[i])
                allocations.append(solver.IntVar(l,u,"a_"+str(i)))
            
            # Constraint : Sum(bi) = B <==>  B >= Sum(bi) <= B
            cons = solver.Constraint(float(B), float(B))
            for i in range(len(allocations)):
                cons.SetCoefficient(allocations[i], 1)
            

            # Objective function
            minimize_objective = solver.Objective()
            for i in range(len(allocations)):
                minimize_objective.SetCoefficient(allocations[i],-self.weights[i])
            
            minimize_objective.SetMinimization()

            solver.Solve()


            self.allocations = [np.max([int(np.ceil(allocation.SolutionValue())),1]) for allocation in allocations]
        else:
           self.allocations = [B]

        
    
    @Pyro5.api.expose
    def __get_result__(self):
        return int(np.sum(self.results))
    
    @Pyro5.api.expose
    def __collect_data__(self):
       X =[]
       y =[]
       for i,x in enumerate(self.Xs):
          X = X + x
          y = y + self.Ys[i]
       return X,y
    
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