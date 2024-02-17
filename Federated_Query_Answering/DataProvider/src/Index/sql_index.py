def __create_index__(dataset_name:str,dims_agg_declarations_min:str,dims_agg_declarations_max:str) ->str:
    return """
    create table stratas_index_"""+dataset_name+""" (num int, """+dims_agg_declarations_min+""","""+dims_agg_declarations_max+""", avg decimal,spar decimal);
"""
def __drop_index__(name:str)->str:
    return "drop TABLE stratas_index_"+name+" ;"

def __create_strata_list_holder__(dataset_name:str)->str:
    return "create table "+dataset_name+"_list (num int);"
def __add_strata_num__(dataset_name:str,num:int)->str:
    return "insert into "+dataset_name+" values ("+str(num)+");"
def __get_max_strata_num__(dataset_name:str)->str:
    return "select max(num) from "+dataset_name+"_list ;"

def __create_meta_strata__(dataset_name:str,num_strata:int) -> str:
    return " create table "+dataset_name+"_"+str(num_strata)+"""_meta (
	col VARCHAR,
	value_ int,
	p_l decimal,
	p_g decimal
);"""

def __add_to_strata_meta__(dataset_name:str,num_strata:int,column:str,value_:str,p_l:float,p_g:float) ->str:
    return "insert into "+dataset_name+"_"+str(num_strata)+"_meta values ('"+column+"',"+value_+","+str(p_l)+","+str(p_g)+") ;"

def __compute_frequency_dim_value_greater__(dataset_name:str,num_strata:int,dim:str,value_:str)->str:
    return "select count(*) from "+dataset_name+"_"+str(num_strata)+" where "+dim+" >= "+value_+" ; "
def __compute_frequency_dim_value_lower__(dataset_name:str,num_strata:int,dim:str,value_:str)->str:
    return "select count(*) from "+dataset_name+"_"+str(num_strata)+" where "+dim+" <= "+value_+" ; "

def __get_probability_dim_value_greater__(dataset_name:str,num_strata:int,dim:str,value_min:str)->str:
    return "select p_g from "+dataset_name+"_"+str(num_strata)+"_meta  where col='"+dim+"' and value_ = "+value_min+" ;"

def __get_probability_dim_value_lower__(dataset_name:str,num_strata:int,dim:str,value_max:str)->str:
    return "select p_l from "+dataset_name+"_"+str(num_strata)+"_meta  where col='"+dim+"' and value_ = "+value_max+" ;"

def __add_strata_to_index__(dataset_name:str,num:int,min_dim:str,max_dim:str,avg:str,spar:str)-> str:
    return "insert into stratas_index_"+dataset_name+" values( "+str(num)+","+str(min_dim)+","+str(max_dim)+","+avg+","+spar+" ); "
def __number_of_stratas__(dataset_name:str) ->str:
    return "select count(*) from stratas_index_"+dataset_name+";"

def __stratas_in_range__(dataset_name:str,range_sql_min:str,range_sql_max:str)->str:
    return "(select num,avg from stratas_index_"+dataset_name+" where "+range_sql_min+") intersect (select num,avg from stratas_index_"+dataset_name+" where "+range_sql_max+");"

def __clusters_in_range__(dataset_name:str,range_sql:str)->str:
    return "select num,avg from stratas_index_"+dataset_name+" where "+range_sql+";"

def __stratas__(dataset_name:str)->str:
    return "select num,avg from stratas_index_"+dataset_name+" ;"
def __stratas_all__(dataset_name:str)->str:
    return "select * from stratas_index_"+dataset_name+" ;"

def __stratas_in_range_with_dims__(dataset_name:str,range_sql_min:str,range_sql_max:str)->str:
    return "(select * from stratas_index_"+dataset_name+" where "+range_sql_min+") intersect (select * from stratas_index_"+dataset_name+" where "+range_sql_max+");"


def __stratas_in_range2__(dataset_name:str,range_sql:str):
    #how to crete pl sql function that returns a list of pair values in postgres?
    return "select num,avg from stratas_index_"+dataset_name+" where "+range_sql+";"
 