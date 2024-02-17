CREATE OR REPLACE TYPE Query_Ranges AS VARRAY(number_of_dims * 2) OF number;

CREATE OR REPLACE FUNCTION DPnoisefunction (query Query_Ranges)
RETURNS Double
DECLARE
	volume integer := 0;
	TYPE columns_names IS TABLE OF VARCHAR2(50);
  	v_columns columns_names;
	looping_index number := 0;
	sum_noise double := 0;
	
	cursor AT_R is SELECT * FROM AT_R_table;
BEGIN
	SELECT column_name
  	BULK COLLECT INTO v_columns
  	FROM all_tab_columns 
 	WHERE table_name = 'AT_R_table';
	FOR sub_region in AT_R LOOP
	   WHILE(looping_index < v_columns.count-1)LOOP
	      IF (query(looping_index) <= sub_region[v_columns(looping_index)] AND
		    rquery(looping_index) >= sub_region.[v_columns(looping_index)]) 
		   OR (query(looping_index) >= sub_region[v_columns(looping_index)] AND
		       query(looping_index) <= sub_region[v_columns(looping_index + 1)]) THEN
	             
	                 volume =: volume * ABS(GREATEST(sub_region[v_columns(looping_index)],query(looping_index))
				 - LEAST(sub_region[v_columns(looping_index+1)],query(looping_index+1)));
			ELSEIF
				volume := 0;
				looping_index : = v_columns.count;
			END IF;
			
			looping_index := looping_index + 2
		END LOOP;
		
		sum_noise := sum_noise + volume * sub_region.noise;
		volume := 0;
		looping_index := 0;
	END LOOP;
	RETURN sum_noise;
	
END;




select SUM(count_) as res from adult_25 where _c_ between 3 and 47 and _d_ between 3 and 8 and _e_ between 7 and 7;


-- Here the plsql code for SAPPROX impl
CREATE OR REPLACE TYPE Query_Ranges AS VARRAY(number_of_dims * 2) OF number;
CREATE OR REPLACE TYPE Dims AS VARRAY(number_of_dims * 2) OF VARCHAR2;

CREATE OR REPLACE FUNCTION inclusion_probability (VARCHAR2 strata_meta, query Query_Ranges,dims Dims)
RETURNS Double
DECLARE

	probability double := 1;
	probability_l double := 1;
	probability_g double := 1;
	volume integer := 0;
	TYPE columns_names IS TABLE OF VARCHAR2(50);
  	v_columns columns_names;
	looping_index number := 0;
	sum_noise double := 0;
BEGIN
	
	   WHILE(looping_index < dims.count-1)LOOP
	    select f_col_p INTO probability_g from strata_meta where column = dims(looping_index) col = v_col_min - 1
	    probability =: probability * probability_g * probability_l    
	    volume =: volume * ABS(GREATEST(sub_region[v_columns(looping_index)],query(looping_index))
				 - LEAST(sub_region[v_columns(looping_index+1)],query(looping_index+1)));
			
			looping_index := looping_index + 2
		END LOOP;
		
		sum_noise := sum_noise + volume * sub_region.noise;
		volume := 0;
		looping_index := 0;
	RETURN sum_noise;
	
END;


create strata_index_0 (
	column VARCHAR2,
	value_ int,
	p_l double,
	p_g double
);


-- insert special values for -1 and dom + 1

select f_col_p from si_i where col = v_col_min - 1
select f_col_m from si_i where col = v_col_max + 1