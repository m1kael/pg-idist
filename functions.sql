
-----------------------------------------------------------------------
-- HELPER FUNCTIONS
-----------------------------------------------------------------------

-- returns the value column from the matching unique key query
CREATE OR REPLACE FUNCTION InfoGet(key_in varchar) RETURNS text AS $$
DECLARE
    --temp RECORD;
    val_out varchar = NULL;
BEGIN
    select val from info where info.key = key_in into val_out;
    if not found then
        raise exception 'no entry found for key %', key_in;
    end if;
    
    --SELECT * FROM info WHERE info.key like key_in INTO temp;
--    EXECUTE 'SELECT * FROM info WHERE info.key like $1' INTO temp USING key;
--    EXECUTE 'SELECT * FROM tt WHERE tt.id = $1' INTO rec USING x;
    --RETURN temp.val;
    
    --raise notice 'val_out = %', val_out;
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


-- calculates the constant spacing separator value for partitions
-- automatically does this using an upper bound approximation
CREATE OR REPLACE FUNCTION CalculateC() RETURNS real AS $$
DECLARE
    e real := 0.0; --not needed right now, but maybe later    
    d int; -- number dims
    val real := 0.0;
BEGIN

    select InfoGet('num_dims') into d;
    
    val := (2 * e) + 1; -- max distance in one dimension
    val := sqrt(val * d); -- over all dimensions
    val := (val * 1.2) + 0.51; -- then just buffer it for assurance
    val := round(val); -- we rounded to an int, but not necessary
    
    raise notice 'c = %', val;
    return val;
    
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------
-- TEST FUNCTIONS
-----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION Test() RETURNS void AS $$
DECLARE

BEGIN

    execute 'truncate table info';
    execute Test_InsertData();
    execute Test_InsertRefs();
    execute InitOptions();
    execute BuildIndex();
    execute Test_IndexGet();
    
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



CREATE OR REPLACE FUNCTION SeeAll() RETURNS void AS $$
DECLARE
    tmp RECORD;
BEGIN

    raise notice '----- INFO TABLE -----';
    
    FOR tmp in SELECT * FROM info LOOP
        raise notice '%', tmp;
    END LOOP;
    
    raise notice '----- REFS TABLE -----';
    
    FOR tmp in SELECT * FROM refs LOOP
        raise notice '%', tmp;
    END LOOP;
    
    raise notice '----- DATA TABLE -----';
    
    FOR tmp in SELECT * FROM data LOOP
        raise notice '%', tmp;
    END LOOP;
    
    raise notice '----- INDEX TABLE -----';
    
    FOR tmp in SELECT * FROM index LOOP
        raise notice '%', tmp;
    END LOOP;
    
END;
$$ LANGUAGE plpgsql;


/*
-- return all reference points as a set of records from the ref table
CREATE OR REPLACE FUNCTION collect_refs() RETURNS SETOF refs AS $$
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
*/


-- simple wrapper for any initializations required prior to index building
CREATE OR REPLACE FUNCTION InitOptions() RETURNS void AS $$
DECLARE
    c_val_str varchar;
BEGIN
    
    select CalculateC() into c_val_str;
    execute InfoSet('c_val', c_val_str);
    
END;
$$ LANGUAGE plpgsql;



-- cycle through data and refs building index
CREATE OR REPLACE FUNCTION BuildIndex() RETURNS void AS $$
DECLARE

    --global vars to retrieve
    num_refs integer;
    num_dims integer;
    c_val real := 0.0;
    
    --vars for indexing
        -- refs and points
    r refs%rowtype;
    p data%rowtype;
    counter integer;
    ref_ids integer[];
    ref_dims real[];
        --vars for assignment
    dist real := 0; 
    ref real[];
    curdist real; 
    curid integer;
    old_distmax real;
    index_y real;
    
    dbg boolean := TRUE;
    
BEGIN

    -- we need this for calculating index value later
    num_refs := InfoGet('num_refs');
    num_dims := InfoGet('num_dims');
    c_val := InfoGet('c_val');
    
    if c_val = 0.0 then
        raise exception 'Invalid c value!';
    end if;
    
    if dbg = TRUE then
        raise notice 'c_val = %', c_val;
    end if;
    
    -- collect refs in memory for speed and convenience
    counter := 0;
    FOR r in SELECT * FROM refs
    LOOP
        --RAISE NOTICE 'counter = %', counter;
        IF counter = 0 THEN --need to initialize array first time
            ref_ids := ARRAY[r.id];
            ref_dims := ARRAY[r.dims];
        ELSE
            ref_ids := ref_ids || r.id;
            ref_dims := ref_dims || r.dims;
        END IF;
        counter := counter + 1;
    END LOOP;
    
    /*
    RAISE NOTICE 'ids = %', ref_ids;
    RAISE NOTICE 'dims = %', ref_dims;
    RAISE NOTICE 'array dims = %', array_dims(ref_dims);
    
    RAISE NOTICE 'ids length = %', array_length(ref_ids, 1);
    RAISE NOTICE 'dims length = %', array_length(ref_dims, 1);
    RAISE NOTICE 'dims2 length = %', array_length(ref_dims, 2);
    */
    
    -- assure things working correctly
    if array_length(ref_dims, 1) <> num_refs then
        raise exception 'Incorrect number of reference points!';
    end if;
    
    
    for p in select * from data loop
    
        if dbg = TRUE then
            RAISE NOTICE 'point p has id % with dims: %', p.id, p.dims;
        end if;
        
        curdist := 0.0;
        curid := -1;
        counter := 1; -- fyi, one-based indexing
        FOREACH ref SLICE 1 IN ARRAY ref_dims
        LOOP
            RAISE NOTICE 'row = %', ref;
            dist := distance(ref, p.dims);
            RAISE NOTICE '    distance = %', dist;
            IF dist < curdist OR curid = -1 THEN
                curid := ref_ids[counter];
                curdist := dist;
            END IF;
            counter := counter + 1;
        END LOOP;
        
        if dbg = TRUE then
            RAISE NOTICE '    closest ref % with dist %', curid, curdist;
        end if;
        
        --update point partition id (convenience knowledge)
        UPDATE data SET pid = curid WHERE id = p.id;
        
        --check partition distmaxes
        SELECT refs.distmax FROM refs WHERE refs.id = curid INTO old_distmax;
        
        RAISE NOTICE '    old distmax = %', old_distmax;
        
        IF old_distmax < curdist OR old_distmax IS NULL THEN
            if dbg = TRUE then
                RAISE NOTICE '      replacing distmax!';
            end if;
            UPDATE refs SET distmax = curdist, distmaxid = p.id WHERE id = curid;
            
        END IF;
        
        --calculate index value
        index_y := (curid * c_val) + curdist;
        RAISE NOTICE '    index value => %', index_y;
        
        insert into index(id, val) VALUES (p.id, index_y);
    
    end loop;
    

END;
$$ LANGUAGE plpgsql;


--uses RETURN NEXT to iteratively (row by row) build up the result set
CREATE OR REPLACE FUNCTION Test_IndexGet() RETURNS 
TABLE(id int, dims real[], val real) AS $$
DECLARE

BEGIN
    return query select data.id, data.dims, index.val from index inner join data on index.id = data.id where index.val between 0.0 and 2.0;
    
END;
$$ LANGUAGE plpgsql;

/*
--uses RETURN NEXT to iteratively (row by row) build up the result set
CREATE OR REPLACE FUNCTION Test_IndexGet() RETURNS SETOF record AS $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT * FROM index LOOP
        -- can do some processing here
        RETURN NEXT r; -- return current row of SELECT
    END LOOP;
    RETURN;
    
END;
$$ LANGUAGE plpgsql;
*/


-- cycle through data and refs building index
CREATE OR REPLACE FUNCTION QueryIndex() RETURNS void AS $$
DECLARE
BEGIN

--select data.id, data.dims, index.val from index inner join data on index.id = data.id where index.val between 0.0 and 2.0;


    

END;
$$ LANGUAGE plpgsql;
