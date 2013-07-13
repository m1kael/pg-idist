
-- returns the value column from the matching unique key query
CREATE OR REPLACE FUNCTION InfoGet(key_in varchar) RETURNS text AS $$
DECLARE
    --temp RECORD;
    val_out varchar = NULL;
BEGIN
    select val from info where info.key = key_in into val_out;
    if not found then
        raise notice 'no entry found for key %', key_in;
    end if;
    --SELECT * FROM info WHERE info.key like key_in INTO temp;
--    EXECUTE 'SELECT * FROM info WHERE info.key like $1' INTO temp USING key;
--    EXECUTE 'SELECT * FROM tt WHERE tt.id = $1' INTO rec USING x;

    --RETURN temp.val;
    
    raise notice 'val_out = %', val_out;
    
    return val_out;
END;
$$ LANGUAGE plpgsql;



-- sets the value column given a key val pair
CREATE OR REPLACE FUNCTION InfoSet(key_in varchar, val_in varchar) RETURNS void AS $$
DECLARE
    --id_out int := -1;
    cur_val varchar := '';
BEGIN
    select val from info where info.key = key_in into cur_val;
    if found then
        raise notice 'key % already exists with value %', key_in, cur_val;
        update info set val = val_in where key = key_in;
    else
        insert into info(key, val) values (key_in, val_in);
    end if;
        
    
/*
    INSERT INTO info (key, val) VALUES (key_in, val_in) RETURNING id into id_out;
    IF FOUND THEN
        RAISE NOTICE '    Value inserted!';
    ELSE
        RAISE NOTICE '    Value not inserted!';
    END IF;
    RETURN id_out;
*/
END;
$$ LANGUAGE plpgsql;




--------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION Test() RETURNS void AS $$
DECLARE

BEGIN

    execute 'truncate table info';
    execute Test_InsertData();
    execute Test_InsertRefs();
    
END;
$$ LANGUAGE plpgsql;




-- populates half-points in reference table for explicit 2D case
CREATE OR REPLACE FUNCTION Test_InsertRefs() RETURNS void AS $$
DECLARE
    points real[][];
    pt real[];
    i integer;
    tmpVal varchar;
BEGIN
    points := '{{0.0, 0.5}, {0.5, 0.0}, {1.0, 0.5}, {0.5, 1.0}}';
    select array_length(points, 1) into tmpVal;
    
    RAISE NOTICE 'Inserting test ref points = %', points;
    RAISE NOTICE 'num refs = %', tmpVal;
    --RAISE NOTICE 'num dims = %', array_length(points, 2);
    -- better make sure dims match in future
    
    --set this info for later
    perform InfoSet('num_refs', tmpVal);
    
    -- using manual counter for ID values
    i := 0;
    FOREACH pt SLICE 1 IN ARRAY points LOOP
        RAISE NOTICE 'ref[%] = %', i, pt;
        INSERT into refs (id, dims) VALUES (i, pt);
        i := i + 1;
    END LOOP;
    
    /*
    -- manual inserts
    EXECUTE 'insert into refs (id, dims) VALUES (0, ARRAY[0.0,0.5])';
    EXECUTE 'insert into refs (id, dims) VALUES (1, ARRAY[0.5,0.0])';
    EXECUTE 'insert into refs (id, dims) VALUES (2, ARRAY[1.0,0.5])';
    EXECUTE 'insert into refs (id, dims) VALUES (3, ARRAY[0.5,1.0])';
    */  
END;
$$ LANGUAGE plpgsql;



/*

  
  number_partitions = number_dimensions*2;
  reference_points = new double[number_partitions*number_dimensions];
  
  //initialize everything to 0.5
  for(int i = 0; i < number_partitions * number_dimensions; i++)
  {
    reference_points[i] = 0.5;
  }
   
  //set 0, 1 values
  //half_offset is to the start of the second half of ref points
  //the dim^2 is because i want dim*(.5*partss) and parts = 2*dim
  int half_offset = number_dimensions*number_dimensions;
  for(int i=0; i < number_dimensions; i++) //for each dimension
  {
      //set only its points to 0 and 1, 
      //walking diagonally through array in both spots at once
      reference_points[(i*number_dimensions) + i] = 0;
      reference_points[(half_offset) + (i*number_dimensions) + i] = 1;
  }
  
  
*/


/* Test 2D dataset from existing idistance codebase

0, 0.0, 0.8
1, 0.475, 0.5
2, 0.3, 0.35
3, 0.4, 0.2
4, 0.5, 0.05
5, 0.8, 0.1
6, 0.8, 0.6
7, 0.4, 0.65
8, 0.4, 0.8
9, 0.6, 0.8

*/

-- populates test data
CREATE OR REPLACE FUNCTION Test_InsertData() RETURNS void AS $$
DECLARE
    points real[][];
    pt real[];
    i integer;
    tmpVal varchar;
BEGIN
    points := '{{0.0, 0.8}, {0.475, 0.5}, {0.3, 0.35}, {0.4, 0.2}, {0.5, 0.05}, {0.8, 0.1}, {0.8, 0.6}, {0.4, 0.65}, {0.4, 0.8}, {0.6, 0.8}}';
    
    select array_length(points, 1) into tmpVal;
    execute InfoSet('num_points', tmpVal);
    select array_length(points, 2) into tmpVal;
    execute InfoSet('num_dims', tmpVal);
    
    RAISE NOTICE 'Inserting test data points = %', points;
    RAISE NOTICE 'num pts = %', array_length(points, 1);
    RAISE NOTICE 'num dims = %', array_length(points, 2);
    
    
    -- using manual counter for ID values
    -- might want to change this later if data already has explicit IDs
    i := 0;
    FOREACH pt SLICE 1 IN ARRAY points LOOP
        RAISE NOTICE 'point[%] = %', i, pt;
        INSERT into data (id, dims) VALUES (i, pt);
        i := i + 1;
    END LOOP;
    
    /*
    -- playing around
    RAISE NOTICE 'points = %', points;
    RAISE NOTICE 'array dims of points = %', array_dims(points);
    
    RAISE NOTICE 'points[1][1] = %', points[1][1];
    RAISE NOTICE 'points[1] = %', points[1];
    RAISE NOTICE 'points[1][1:2] = %', points[1][1:2];
    */
    
    
    /*
    -- first try (still a 2D array index, which is screwy...)
    -- but makes for significantly cleaner insert code
    RAISE NOTICE 'points[1:1] = %', points[1:1];
    select points[1:1] into pt;
    RAISE NOTICE 'pt = %', pt;
    RAISE NOTICE 'array dims of pt = %', array_dims(pt);
    RAISE NOTICE 'pt = % , %', pt[1][1], pt[1][2];
    
    -- then to insert the only way i found was slicing to get 1D arrays out
    -- but that meant i had to keep an explicit loop variable too, wasteful!
    i := 0;
    FOREACH pt SLICE 1 IN ARRAY points LOOP
        INSERT into data (id, dims) VALUES (i, pt);
        i := i + 1;
    END LOOP;
    */
    
    
    
    /*
    -- second try, found the right way to get 1D array
    RAISE NOTICE 'points[1:1] = %', ARRAY(SELECT unnest(points[1:1]));
    -- unpacks the 2D array into single values, then take them all and
    -- pack them back up as a new array (which is now 1D)
    select ARRAY(SELECT unnest(points[1:1])) into pt;
    RAISE NOTICE 'pt = %', pt;
    RAISE NOTICE 'array dims of pt = %', array_dims(pt);
    RAISE NOTICE 'pt = % , %', pt[1], pt[2];
    
    
    --now just a regular loop to insert
    --but messy indexing inside
    RAISE NOTICE 'num pts = %', array_length(points, 1);
    RAISE NOTICE 'num dims = %', array_length(points, 2);
    
    FOR i in 1..(array_length(points,1)) LOOP
        select ARRAY(SELECT unnest(points[i:i])) into pt;
        RAISE NOTICE 'points[i] = %', pt;
        INSERT into data (id, dims) VALUES (i, pt);
    END LOOP;
    */
    
    --very first attempt
    --EXECUTE 'insert into data (id, dims) VALUES (1, ARRAY[0.3,0.1])';
    --EXECUTE 'insert into data (id, dims) VALUES (2, ARRAY[0.8,0.4])';
    --EXECUTE 'insert into data (id, dims) VALUES (3, ARRAY[0.2,0.9])';
    

END;
$$ LANGUAGE plpgsql;


-- calculates euclidean distance between two real-valued arrays
CREATE OR REPLACE FUNCTION distance(a real[], b real[], OUT dist real) AS $$
DECLARE
    ndims integer;
    sum real := 0.0;
BEGIN
    EXECUTE 'SELECT array_length($1,1)' INTO ndims USING a;
--    ndims := SELECT array_length(a) INTO ndims;
    FOR i in 1..ndims LOOP
        sum = sum + ((a[i] - b[i]) ^ 2);
    END LOOP;
    dist = |/ sum;
END;
$$ LANGUAGE plpgsql;


-- automatically calculate constant c separator between partitions
-- given data dimensionality and normal unit-space

/*

  //added so we can dynamically set c based on what config file refs-dist is
  //space given extra e (default is e=0, no affect)
  double edist = (2 * e) + 1;
  
  //since each dim is max length of 1
  //multiply by 2 for safety 
  double diag = 2 * sqrt( (edist * d));
  
  //old
//  double diag = sqrt(d);
  
  //added ONLY for Tim's EM algorithm 4/25/2012
  //diag = diag * 3;
  

  //round up
  int c = (int)(diag + 0.5);

  //cout << "calculate c given d= " << d << ", e = " << e << " ; c = " << c << endl;
  
*/



-- return all reference points as a set of records from the ref table
CREATE  OR REPLACE FUNCTION collect_refs() RETURNS SETOF refs AS $$
DECLARE
  r refs%rowtype;
BEGIN
    FOR r in SELECT * FROM REFS
    LOOP
        RETURN NEXT r;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;


-- cycle through data and refs building index
CREATE OR REPLACE FUNCTION BuildIndex() RETURNS void AS $$
DECLARE
  r refs%rowtype;
  p data%rowtype;
  ids integer[];
  dims real[];
  counter integer := 0;
  nrefs integer;
  dist real := 0;
  ref real[];
  curdist real := 0;
  curid integer := -1;
  const_c real;
  index_y real;
  old_distmax real;
  
BEGIN

    const_c := get_info('constant_c');
    RAISE NOTICE 'const_c = %', const_c;
    
    FOR r in SELECT * FROM refs
    LOOP
        RAISE NOTICE 'counter = %', counter;
        IF counter = 0 THEN
            dims := ARRAY[r.dims];
            ids := ARRAY[r.id];
        ELSE
            ids := ids || r.id;
            dims := dims || r.dims;
        END IF;
        counter := counter + 1;
    END LOOP;
    
    RAISE NOTICE 'ids = %', ids;
    RAISE NOTICE 'dims = %', dims;
    
    RAISE NOTICE 'array dims = %', array_dims(dims);
    RAISE NOTICE 'dims = %', dims[1:4][1:2];
    RAISE NOTICE 'dims = %', dims[2:2][1:2];
    
    RAISE NOTICE 'counter = %', counter;
    SELECT array_length(ids, 1) INTO nrefs;
    RAISE NOTICE 'ids length = %', array_length(ids, 1);
    RAISE NOTICE 'dims length = %', array_length(dims, 1);
    RAISE NOTICE 'dims2 length = %', array_length(dims, 2);
    
    
    EXIT;
    
    FOR p in SELECT * FROM data LOOP
    
        RAISE NOTICE 'point p has id % with dims: %', p.id, p.dims;
        
        counter := 1;
        FOREACH ref SLICE 1 IN ARRAY dims
        LOOP
            RAISE NOTICE 'row = %', ref;
            dist := distance(ref, p.dims);
            RAISE NOTICE '    distance = %', dist;
            IF dist < curdist OR curid = -1 THEN
                curid := ids[counter];
                curdist := dist;
            END IF;
            counter := counter + 1;
        END LOOP;
        
        RAISE NOTICE '    closest ref % with dist %', curid, curdist;
        
        --update point partition id
        
        UPDATE data SET pid = curid WHERE id = p.id;
        
        --calculate index value
        index_y := (curid * const_c) + curdist;
        RAISE NOTICE '    index value => %', index_y;
        
        --check partition distmaxes
        
        SELECT refs.distmax FROM refs WHERE refs.id = curid INTO old_distmax;
        
        RAISE NOTICE '    old distmax = %', old_distmax;
        
        IF old_distmax < curdist OR old_distmax IS NULL THEN
            RAISE NOTICE 'replacing distmax';
            UPDATE refs SET distmax = curdist, distmaxid = p.id WHERE id = curid;
            
        END IF;
        
        
        
        
    /*
        FOR i in 1..nrefs LOOP
            RAISE NOTICE 'ref % is id % with dims: %', i, ids[i], dims[1:2];
            
            dist := distance(dims[i:i][1:2], p.dims);
            RAISE NOTICE '    distance = %', dist;
          
        END LOOP;
    */ 
    
    END LOOP;
    

    
END;
$$ LANGUAGE plpgsql;

