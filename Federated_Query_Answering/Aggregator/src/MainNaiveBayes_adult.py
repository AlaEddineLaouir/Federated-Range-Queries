from workload.domain import Domain
from NaiveBayes_Aggregator import NaiveBayes
from workload.query import Query

import pandas as pd

import pickle

import sys




if __name__ == "__main__":
    args  = sys.argv

    number_data_providers = int(args[1])
    ip      = str(args[2])
    # ip="127.0.0.1"
    nb_dim = int(args[2])
    method= str(args[3])



    dims = [ 'D' , 'E' , 'G', 'N' ,'A' , 'C' , 'K' , 'M' , 'L' ]
    doms = [15,15,14,41,73,99,99,98,99]

    dims = dims[:nb_dim]
    doms = doms[:nb_dim]
    classes = 100
    d = len(dims)
    ep_g = 100
    c = NaiveBayes("adult_synth",dims,doms,'L',99,number_data_providers,ip,method,eps_g=ep_g)
        

    c.__fit__()

    with open("NB_model_sum_"+str(d)+"_"+"L"+"_epsilon_"+str(ep_g)+"_.pkl", 'wb') as outp:
        pickle.dump(c, outp, pickle.HIGHEST_PROTOCOL)
    with open("NB_model_sum_"+str(d)+"_"+"L"+"_epsilon_"+str(ep_g)+"_.pkl", 'rb') as inp:
        c2 = pickle.load(inp)

    columns,target = Query.__columns_names__(dims,'L')
    X =0
    y = 0
    if X == 0 or y ==0:
        X,y = c._get_data_(columns,target )
    y_pred,y = c2.predict(X,y)

    count_miss= 0
    for i,y_p in enumerate(y_pred):
        if y_p != y[i]:
            count_miss+=1
    print("Total ",len(y_pred))
    print("Miss ",count_miss)

    acc = 1 - (count_miss/ len(y_pred))

    data_ = {
            "eps" : [ep_g],
            "acc":[acc],
            }
    df = pd.DataFrame(data_)
    df.to_csv("learning_attack_sum_dims_"+str(d)+"_eps_"+str(ep_g)+"_classes_"+str(classes)+".csv",index=False)

