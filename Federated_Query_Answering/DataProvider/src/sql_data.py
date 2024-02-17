def __create_table__(name:str,dims_declarations:str)->str:
    return "CREATE TABLE "+name+" ("+dims_declarations+");"

def __drop_table__(name:str)->str:
    return "drop TABLE "+name+" ;"


def __create_table_temp__(name:str,dims_declarations:str)->str:
    return "CREATE TABLE "+name+"_temp (id_row serial,"+dims_declarations+", agg_count int);"

def __adjust_count_temp__(name:str,max:int) -> str:
    return "update  "+name+"_temp  set agg_count = (random() * "+str(max)+")::integer ; "
def __drop_table_temp__(name:str)->str:
    return "drop TABLE "+name+"_temp ;"


def __aggregate_and_sort_by_dim_in_temp__(name:str,dims_names:str)->str:
    return """insert into """+name+"""_temp ("""+dims_names+""",agg_count)
        select """+dims_names+""", count(*) as agg_count from """+name+"""  
        group by ("""+dims_names+""") 
        order by ("""+dims_names+""");
        """
def __aggregate_in_temp__(name:str,dims_names:str)->str:
    return """insert into """+name+"""_temp ("""+dims_names+""",agg_count)
        select """+dims_names+""", count(*) as agg_count from """+name+"""  
        group by ("""+dims_names+""");
        """
def __aggregate_and_sort_by_count_in_temp__(name:str,dims_names:str)->str:
    return """insert into """+name+"""_temp ("""+dims_names+""",agg_count)
        select """+dims_names+""", count(*) as agg_count from """+name+"""  
        group by ("""+dims_names+""") 
        order by (agg_count);
        """
def __create_strata__(name:str,num:int,dims_declarations:str)->str:
    return "CREATE TABLE "+name+"_" +str(num)+" (id_row serial ,"+dims_declarations+", agg_count int);"
def __create_strata_temp__(name:str,num:int,dims_declarations:str)->str:
    return "CREATE TABLE "+name+"_temp_" +str(num)+" (id_row serial ,"+dims_declarations+", agg_count int);"
def __drop_strata_temp__(name:str,num:int)->str:
    return "drop TABLE "+name+"_temp_" +str(num)+" "

def __delete_strata__(dataset:str,num:int)->str:
    return "drop TABLE "+dataset+"_" +str(num)+";"
def __delete_strata_meta__(dataset:str,num:int)->str:
    return "drop TABLE "+dataset+"_" +str(num)+"_meta;"
def __delete_strata_temp__(dataset:str,num:int)->str:
    return "drop TABLE "+dataset+"_temp_" +str(num)+";"

def __populate_strata__(name:str,num:int,size:int,dims_agg:str,dims_agg_s:str)->str:
    return "insert into "+name+"_"+str(num)+"""  ("""+dims_agg_s+""",agg_count)
        (select """+dims_agg+""" From """+name+"""_temp where id_row between """+ str(num*size)+ """ and """+ str(num*size+size)+ """);
        """
def __populate_strata_sort__(name:str,num:int,size:int,dims_agg:str,dims_agg_s:str)->str:
    return "insert into "+name+"_"+str(num)+"""  ("""+dims_agg_s+""",agg_count)
        (select """+dims_agg_s+""", agg_count From """+name+"_temp_"+str(num)+""" order by """+dims_agg_s+"""
        );
        """
def __populate_temp_strata__(name:str,num:int,size:int,dims_agg:str,dims_agg_s:str)->str:
    return "insert into "+name+"_temp_"+str(num)+"""  ("""+dims_agg_s+""",agg_count)
        (select """+dims_agg+""" From """+name+"""_temp where id_row between """+ str(num*size)+ """ and """+ str(num*size+size)+ """);
        """
def __avg_strata__(name:str,num:int)->str:
    return "select AVG(agg_count) from "+name+"_"+str(num)+";"


def __values_dims__(name:str, num_cluster:int, dims) ->str:
    return "select "+dims+" from "+name+"_"+str(num_cluster)+" ;"


def __min_row_strata_sorted_by_dims__(name:str,num:int,dims_agg_s:str) ->str:
    return "select "+dims_agg_s+" from "+name+"_"+str(num)+" where id_row = 1 ;"
def __max_row_strata_sorted_by_dims__(name:str,num:int,dims_agg_s:str) ->str:
    return "select "+dims_agg_s+" from "+name+"_"+str(num)+" where id_row = (SELECT MAX(id_row) FROM "+name+"_"+str(num)+") ;"
def __get_size_index__(name:str)->str:
    return "select count(*) as size from stratas_index_"+name+";"
def __get_size_temp__(name:str)->str:
    return "select count(*) as size from "+name+"_temp;"
    
def __get_dim_members__(table_name,strata_num,dim):
    return " select distinct "+dim+" from "+table_name+"_"+str(strata_num)+"  ;"

def __insert_data_from_file__(name:str,path:str,sep:str,header="HEADER")->str:
    return "copy "+name+" FROM '"+path+"' DELIMITER '"+sep+"' CSV "+header+" NULL '';"

def __add_b_tree__(name:str, columns:str,index_name="index")->str:
    return "CREATE INDEX "+index_name+"_"+name+" ON "+name+" ("+columns+");"

def __execute_query_strata__(name:str,operator:str,num:int,range_sql:str):
    return "select "+operator+"(agg_count) as res from "+name+"_"+str(num)+" where "+range_sql+";"
def __execute_query_one_table__(name:str,operator:str,range_sql:str):
    return "select "+operator+"(agg_count) as res from "+name+"_temp where "+range_sql+";"

def __get_max__(name:str):
    return "select max(agg_count) as res from "+name+"_temp ;"

def __set_work_memory__(gigas:int)->str:
    return "SET work_mem TO '"+str(gigas)+" GB';"
def __set_max_lock_tran__(max_:int):
    return "ALTER SYSTEM set max_locks_per_transaction = "+str(max_)+" ;"

def __insert_dummy_rows__(name:str,dims_agg_s:str,dims_domains:str,number_rows:int):
    return """
            INSERT INTO """+name+""" ("""+dims_agg_s+""")
            SELECT 
                """+dims_domains+"""
            FROM 
                generate_series(1, """+str(number_rows)+""");
    """
