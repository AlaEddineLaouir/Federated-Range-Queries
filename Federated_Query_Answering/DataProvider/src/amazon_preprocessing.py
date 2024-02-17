from data_preprocessing import __aggregate_table_to_temp__,__recompute_meta_stratas__,__insert_synth_data__,__clean_db__,__dims_declaration_with_types__,__dims_aggregation__,__dims_declaration__,__dims_cast_name__,__create_index__,__create_stratas__,__load_data_file__,__add_dbms_indexes_to_tables__, __dims_concat_pre_suf_fix__
import numpy as np
import math
import pickle5 as pickle
import os




def __dump_meta_metadata_to_file__(dataset_name,S,N,D,domain,version):
    directory_path = os.getcwd()+"/src/Data/Amazon/"+str(version)
    with open(directory_path+"/"+dataset_name+"_meta.metadata", 'wb') as outp:
        pickle.dump([S,N,D,domain], outp, pickle.HIGHEST_PROTOCOL)




def __main_amazon_preprocessing__(strata_portion,sql_db,data_version:int,synth):
    
    name ="amazon"

    
    size = 213 * 10**6
    amazon_data_size= 13647978
    
    strata_size = int(strata_portion*amazon_data_size)
    path = "/Data/Amazon/amazon_review.csv"
    dims = ["overall","date","vote","age","category","country"]
    aggregation_dims =  ["overall","vote","age","category","country"]#,"date"
    
    #,"date"
    
    full_domain =[5,10**4,10,70,28,196]
    reduced_domain= []
    domain_agg = [5,10,70,28,196]

    # converting dims to strings for sql queries
    dim_dec_ss = __dims_aggregation__(dims,"str","","_")
    dims_declaration  = __dims_declaration__(dims,"str","","_")
    agg_dims_declaration = __dims_declaration__(aggregation_dims,"str","","_")
    ### For the index 
    agg_dims_declaration_min = __dims_declaration__(aggregation_dims,"str","","_min")
    agg_dims_declaration_max = __dims_declaration__(aggregation_dims,"str","","_max")
    # for rdbms index
    agg_dims_min = " , ".join(__dims_concat_pre_suf_fix__(aggregation_dims,"str","","_min"))
    agg_dims_max = " , ".join(__dims_concat_pre_suf_fix__(aggregation_dims,"str","","_max"))
    ### End for index
    agg_dims = __dims_aggregation__(aggregation_dims,"str","","_")
    # For stratas
    agg_dims_declaration_s = __dims_declaration__(aggregation_dims,"str","_","_")
    agg_dims_s = __dims_aggregation__(aggregation_dims,"str","_","_")
    dims_passage_temp_to_strata = __dims_cast_name__([aggregation_dims,aggregation_dims],["str","str"],["","_"],["_","_"])


    ### Main Data preprocessing
    # Clean First
    __clean_db__(sql_db,name)
    
    if synth:
        __insert_synth_data__(sql_db,name,dims_declaration,dim_dec_ss,full_domain,size)
    else:
        __load_data_file__(sql_db,name,dims_declaration,path,",","HEADER")
    
    max_count = __aggregate_table_to_temp__(sql_db,name,agg_dims_declaration,agg_dims)

    __create_index__(sql_db,name,agg_dims_declaration_min,agg_dims_declaration_max)

    number_stratas = __create_stratas__(sql_db,name,strata_size,agg_dims_declaration_s,dims_passage_temp_to_strata,agg_dims_s,domain_agg,True,True)

    __dump_meta_metadata_to_file__(name,strata_size,number_stratas,len(aggregation_dims),max_count,data_version)

    return strata_size,number_stratas,len(aggregation_dims),max_count



