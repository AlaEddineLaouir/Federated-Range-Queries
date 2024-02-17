from Connection import Connection
import sql_data
from Index import sql_index

import numpy as np
from tqdm import tqdm



# attributes to sql queries
def __dims_declaration_with_types__(dims:np.array,types:np.array,prefix:str,suffix:str) ->str:
    concatenated = __dims_concat_pre_suf_fix__(dims,"str",prefix,suffix)
    typed = [ concatenated[i]+" "+types[i]  for i in range(len(concatenated))]
    dims_declaration = " , ".join(typed)
    return dims_declaration

def __dims_domain_random__(domain):
    dims_dom = ["(random() * "+str(x)+")::integer" for x in domain]
    dims_dom = ",".join(dims_dom)
    return dims_dom


def __dims_declaration__(dims:np.array, type_:str,prefix:str,suffix:str) ->str:
    suffix = suffix+" real"
    dims_declaration = " , ".join(__dims_concat_pre_suf_fix__(dims,type_,prefix,suffix))
    return dims_declaration

def __dims_aggregation__(dims:np.array, type_:str,prefix:str,suffix:str) ->str:
    dims_agg = " , ".join(__dims_concat_pre_suf_fix__(dims,type_,prefix,suffix))
    return dims_agg

def __dims_cast_name__(dims:np.array,types:np.array,prefixes:np.array,suffixes:np.array) ->str:
    if len(dims) < 3: 
        
        agg_dims =[]
        for  i  in range(len(dims)):
            agg_dims.append(__dims_concat_pre_suf_fix__(dims[i],types[i],prefixes[i],suffixes[i]))

        dims_casted =[ agg_dims[0][i]+" as "+agg_dims[1][i] for i in range(len(dims[0]))]
        dims_casted.append("agg_count ")
        dims_casted = " , ".join(dims_casted)
        
        return dims_casted
    else:
        return None

def __dims_concat_pre_suf_fix__(dims:np.array, type_:str,prefix:str,suffix:str) -> np.array:
    dims_concat =[]
    if type_ == "int":
        dims_concat =[prefix+str(chr(dim + 65))+suffix for dim in dims]
    else:
        dims_concat =[prefix+str(dim)+suffix  for dim in dims]
    return dims_concat

# Convert the dimensions of a tensor to numerical values.
# To be completed
def __to_numerical_dims__(sql_db) ->None:
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()
    query1 = "SELECT count(*) FROM adult"
    result1=cursor.execute(query1)

# Function to clean DB during dev
def __clean_db__(sql_db,table_name:str):
    cnx = Connection(sql_db)
    try:
        cnx = Connection(sql_db)
        cursor = cnx.conn.cursor()
        cursor.execute(sql_data.__drop_table__(table_name))
        cnx.conn.commit()
        cursor.execute(sql_data.__drop_table_temp__(table_name))
        cnx.conn.commit()
        cursor.execute(sql_data.__get_size_index__(table_name))
        number_stratas = int(cursor.fetchone()[0])
        cursor.execute(sql_index.__drop_index__(table_name))
        cnx.conn.commit()
        for strata_num in range(1000+1):
            query = sql_data.__delete_strata__(table_name,strata_num)
            cursor.execute(query)
            cnx.conn.commit()
            #with meta
            query  = sql_data.__delete_strata_meta__(table_name,strata_num)
            cursor.execute(query)
            cnx.conn.commit()
        
        
    except:
        return

def __remove_stratas__(sql_db,table_name:str,strata_size:int):
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()
    query = sql_data.__get_size_temp__(table_name)
    cursor.execute(query)
    size = float(cursor.fetchone()[0])/ float(strata_size)
    size = int(size)
    for strata_num in range(size):
        query = sql_data.__delete_strata__(table_name,strata_num)
        cursor.execute(query)
        #with meta
        query  = sql_data.__delete_strata_meta__(table_name,strata_num)
        cursor.execute(query)
        print("Stratage {} deleted".format(strata_num))
        cnx.conn.commit()
    cnx.conn.commit()

# Load test data from csv to DB table
def __load_data_file__(sql_db,table_name:str,dims_declarations:str,path:str,separator:str,header:str):
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()

    query = sql_data.__create_table__(table_name,dims_declarations)
    cursor.execute(query)

    query = sql_data.__insert_data_from_file__(table_name,path,separator,header)
    cursor.execute(query)

    cnx.conn.commit()


# Generate synthetic data
def __insert_synth_data__(sql_table,table_name:str,dims_declarations:str,dims_agg_s:str,domain,number_rows:int):
    cnx = Connection(sql_table)
    cursor = cnx.conn.cursor()
    
    query = sql_data.__create_table__(table_name,dims_declarations)

    cursor.execute(query)

    dims_doms = __dims_domain_random__(domain)
    query = sql_data.__insert_dummy_rows__(table_name,dims_agg_s,dims_doms,number_rows)
    
    cursor.execute(query)
    cnx.conn.commit()




#Create tensor by aggregation
def __aggregate_table_to_temp__(sql_db,table_name:str,dims_agg_declarations:str,dims_agg:str):
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()

    cursor.execute(sql_data.__set_work_memory__(1))
    # cnx.conn.commit()
    # cursor.execute(sql_data.__set_max_lock_tran__(2**12))
    cnx.conn.commit()
    
    # Create table temp where to store aggregated of data
    query_temp = sql_data.__create_table_temp__(table_name,dims_agg_declarations)
    cursor.execute(query_temp)
    query_agg = sql_data.__aggregate_in_temp__(table_name,dims_agg)
    cursor.execute(query_agg)

    query = sql_data.__get_max__(table_name)
    cursor.execute(query)
    max_count =  cursor.fetchone()[0]
    
    if int(max_count) == 1:
        if table_name == "amazon":
            max = 34
        else:
            max = 9
        query = sql_data.__adjust_count_temp__(table_name,max)
        cursor.execute(query)
        
        query = sql_data.__get_max__(table_name)
        cursor.execute(query)
        max_count =  cursor.fetchone()[0]

        
    cnx.conn.commit()

    return int(max_count)




# Create the index table
def __create_index__(sql_db,table_name:str,agg_dims_declaration_min:str,agg_dims_declaration_max:str):
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()
    query = sql_index.__create_index__(table_name,agg_dims_declaration_min,agg_dims_declaration_max)
    cursor.execute(query)
    cnx.conn.commit()

# Create the different stratas 
def __create_stratas__(sql_db,table_name:str,strata_size:int,agg_dims_declaration_s:str,dim_agg:str,dims_agg_s:str,domain,with_meta=True,with_sort=False)->int:
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()
    query = sql_data.__get_size_temp__(table_name)
    cursor.execute(query)
    size = cursor.fetchone()[0]
    number_stratas = int(size/strata_size) + (1*(size%strata_size>0))

    for strata_num in tqdm(np.arange(number_stratas), " Stratas loop : ",leave=True):
        cnx = Connection(sql_db)
        cursor = cnx.conn.cursor()

        query = sql_data.__create_strata__(table_name,strata_num,agg_dims_declaration_s)
        cursor.execute(query)
            
        query = sql_data.__populate_strata__(table_name,strata_num,strata_size,dim_agg,dims_agg_s)
        cursor.execute(query)
        cnx.conn.commit()
        
        # meta works with Domain should be changed to get distinct to reduce the search space
        __metadata_strata__(sql_db,table_name,strata_size,dims_agg_s,strata_num,domain)

        _cluster_BE_dim_index__(sql_db,table_name,strata_size,dims_agg_s,strata_num,domain)

    
        
        #print("Stratage {} created".format(strata_num))

        cnx.conn.commit()
    return number_stratas

# FIXME : This function to be deleted 
def __recompute_meta_stratas__(sql_db,table_name:str,strata_size:int,dims_agg_s:str,domain):
    try:
        cnx = Connection(sql_db)
        cursor = cnx.conn.cursor()

        cursor.execute(sql_data.__get_size_index__(table_name))
        number_stratas = int(cursor.fetchone()[0])
        for strata_num in range(number_stratas+1):
            query  = sql_data.__delete_strata_meta__(table_name,strata_num)
            cursor.execute(query)
            __metadata_strata__(sql_db,table_name,strata_size,dims_agg_s,strata_num,domain)
    except:
        pass




def __add_dbms_indexes_to_tables__(sql_db,table_name:str,agg_dims:str,dims_min:str,dims_max:str,dims_agg:str):
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()
    # table to test
    query = sql_data.__add_b_tree__(table_name,agg_dims,"temp_index_1")
    cursor.execute(query)
    cnx.conn.commit()
    # # temp_table to test
    query = sql_data.__add_b_tree__(table_name+"_temp",agg_dims,"temp_index_1")
    cursor.execute(query)
    # index_structure
    query = sql_data.__add_b_tree__("stratas_index_"+table_name,dims_min,"min_index_1")
    cursor.execute(query)

    query = sql_data.__add_b_tree__("stratas_index_"+table_name,dims_max,"max_index_1")
    cursor.execute(query)
    cnx.conn.commit()
    # Stratas
    query = sql_index.__number_of_stratas__(table_name)
    cursor.execute(query)
    count = cursor.fetchone()[0]
    for i in range(count):
        query = sql_data.__add_b_tree__(table_name+"_"+str(i),dims_agg)
        cursor.execute(query)
        query = sql_data.__add_b_tree__(table_name+"_"+str(i)+"_meta","col, value_")
        cursor.execute(query)
        cnx.conn.commit()
    
    #cnx.conn.commit()



def __metadata_strata__(sql_db,table_name:str,strata_size:int,dims_agg_s,strata_num:int,domain):
    cnx = Connection(sql_db)
    cursor = cnx.conn.cursor()

    cursor.execute(sql_index.__create_meta_strata__(table_name,strata_num))
    dims_agg_s = dims_agg_s.split(' , ')
    for index_dim,dim in enumerate(dims_agg_s):
        # cursor.execute(sql_data.__get_dim_members__(table_name,strata_num,dim))
        # members = cursor.fetchall()
        # members = [str(x[0]) for x in members]
        # members.append("-1")
        # members.append(str(domain[index_dim]+2))
        for member in range(-1,domain[index_dim]+1):
            
            cursor.execute(sql_index.__compute_frequency_dim_value_greater__(table_name,strata_num,dim,str(member)))
            p_g =  cursor.fetchone()[0]/strata_size
            
            cursor.execute(sql_index.__compute_frequency_dim_value_lower__(table_name,strata_num,dim,str(member)))
            p_l = cursor.fetchone()[0]/strata_size

            cursor.execute(sql_index.__add_to_strata_meta__(table_name,strata_num,dim,str(member),p_l,p_g))

    cnx.conn.commit()



def _cluster_BE_dim_index__(sql_db,table_name:str,cluster_size:int,dims_agg_s:str,strata_num:int,domain):
        cnx = Connection(sql_db)
        cursor = cnx.conn.cursor()

        query = sql_data.__avg_strata__(table_name,strata_num)
        cursor.execute(query)
        x =cursor.fetchone()[0]
        avg_strata = str(x)
       
        min_dims =[]
        max_dims=[]
        for dim in dims_agg_s.split(','):
            min_dims.append("min("+str(dim)+")")
            max_dims.append("max("+str(dim)+")")
        
        query = sql_data.__values_dims__(table_name,strata_num,",".join(min_dims))
        cursor.execute(query)
        
        min_dim_v=[]
        for value in cursor.fetchone():
            min_dim_v.append(str(value))

        query = sql_data.__values_dims__(table_name,strata_num,",".join(max_dims))
        cursor.execute(query)
        
        max_dim_v=[]
        for value in cursor.fetchone():
            max_dim_v.append(str(value))
        
        query = sql_index.__add_strata_to_index__(table_name,strata_num,",".join(min_dim_v),",".join(max_dim_v),avg_strata,str(0))
        cursor.execute(query)
        cnx.conn.commit()


def __strata_BE_index__(sql_db,table_name:str,strata_size:int,dims_agg_s:str,strata_num:int,domain):
        
        cnx = Connection(sql_db)
        cursor = cnx.conn.cursor()
        query = sql_data.__avg_strata__(table_name,strata_num)
        cursor.execute(query)
        x =cursor.fetchone()[0]
        avg_strata = str(x)
        # to compute the info for the index we need : min,max, spar = size/ volume(max-min), avg
        query = sql_data.__min_row_strata_sorted_by_dims__(table_name,strata_num,dims_agg_s)
        cursor.execute(query)
        min_dim = ""
        min_dim_v=[]
        for value in cursor.fetchone():
            min_dim_v.append(value)
            min_dim = min_dim+str(value)+","
        min_dim = min_dim[:-1]

        query = sql_data.__max_row_strata_sorted_by_dims__(table_name,strata_num,dims_agg_s)
        cursor.execute(query)
        
        max_dim = ""
        max_dim_v=[]
        for value in cursor.fetchone():
            max_dim_v.append(value)
            max_dim = max_dim+str(value)+","
        max_dim = max_dim[:-1]
        
        #Volume = V1 and Vj = 
        # IF j < n
        #   {|Sj -Ej|*V[j+1]*dom[j+1]} 
        # IF j = n 
        #   {|Sj -Ej|}

        v = np.abs(np.array(max_dim_v) - np.array(min_dim_v))
        v[v == 0] = 1
        domain.append(1)
        v = [ v_*domain[i+1] for i,v_ in enumerate(v)]
        v = np.prod(v)
        sparsity  = strata_size / v

        query = sql_index.__add_strata_to_index__(table_name,strata_num,min_dim,max_dim,avg_strata,str(sparsity))
        cursor.execute(query)
        cnx.conn.commit()
