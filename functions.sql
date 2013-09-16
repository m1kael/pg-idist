
-----------------------------------------------------------------------
-- HELPER FUNCTIONS
-----------------------------------------------------------------------

-- returns the value column from the matching unique key query
CREATE OR REPLACE FUNCTION InfoGet(key_in varchar) RETURNS text AS $$
DECLARE
    val_out varchar = NULL;
BEGIN
    select val from info where info.key = key_in into val_out;
    if not found then
        raise exception 'no entry found for key %', key_in;
    end if;

    --raise notice 'val_out = %', val_out;
    return val_out;
END;
$$ LANGUAGE plpgsql;



-- sets the value column given a key val pair
CREATE OR REPLACE FUNCTION InfoSet(key_in varchar, val_in varchar) RETURNS void AS $$
DECLARE
    cur_val varchar := '';
BEGIN
    select val from info where info.key = key_in into cur_val;
    if found then
        raise notice 'key % already exists with value %', key_in, cur_val;
        update info set val = val_in where key = key_in;
    else
        insert into info(key, val) values (key_in, val_in);
    end if;
      
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


-- function wrapper for the index function mapping calculation
-- note: we decrement i (ref index) by one to achieve 0-based index values
CREATE OR REPLACE FUNCTION CalculateY(i real, c real, d real) RETURNS real AS $$
BEGIN 
    return (((i-1) * c) + d);
END;
$$ LANGUAGE plpgsql;



-- calculates euclidean distance between two real-valued arrays
CREATE OR REPLACE FUNCTION Distance(a real[], b real[], OUT dist real) AS $$
DECLARE
    ndims integer;
    sum real := 0.0;
BEGIN
    EXECUTE 'SELECT array_length($1,1)' INTO ndims USING a;
    --ndims := SELECT array_length(a) INTO ndims;
    FOR i in 1..ndims LOOP
        sum = sum + ((a[i] - b[i]) ^ 2);
    END LOOP;
    dist = |/ sum;
END;
$$ LANGUAGE plpgsql;


-- debug function to show everything in each table
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


-----------------------------------------------------------------------
-- TEST FUNCTIONS
-----------------------------------------------------------------------

-- run the full test suite
-- this will delete everything in the db and redo it from scratch!
CREATE OR REPLACE FUNCTION Test() RETURNS void AS $$
DECLARE

BEGIN

    execute 'truncate table info';
    execute 'truncate table data';
    execute 'truncate table refs';
    execute 'truncate table index';
    
    raise notice 'INSERTING DATA...';
    execute Test_InsertData();
    raise notice 'CREATING REFS...';
    execute Test_InsertRefs();
    raise notice 'INIT OPTIONS...';
    execute InitOptions();
    raise notice 'BUILD INDEX...';
    execute BuildIndex();
    raise notice 'KNN RETRIEVAL';
    execute Test_KNN();
END;
$$ LANGUAGE plpgsql;



-- populates half-points strategy in reference table for explicit 2D case
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
    
    -- using manual counter for ID values (1-based to match array indexing)
    i := 1;
    FOREACH pt SLICE 1 IN ARRAY points LOOP
        RAISE NOTICE 'ref[%] = %', i, pt;
        INSERT into refs (id, dims) VALUES (i, pt);
        i := i + 1;
    END LOOP;

END;
$$ LANGUAGE plpgsql;



/* Test 2D dataset

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

-- populates test data, again explicit 2D case here
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
    
    
    -- using manual counter for 1-based ID values
    i := 1;
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
    

END;
$$ LANGUAGE plpgsql;


-- this sets up and runs an example KNN query on the 2D test
CREATE OR REPLACE FUNCTION Test_KNN() RETURNS void AS $$
DECLARE
    q real[];
    k int;
BEGIN
    q := '{0.0, 0.0}';
    k := 10;
    perform QueryKNN(q, k);
    
END;
$$ LANGUAGE plpgsql;



------------------------------------------------------------
-- idistance functions
------------------------------------------------------------

-- populates half-points in reference table for any dimension
CREATE OR REPLACE FUNCTION BuildRefs_HalfPoints() RETURNS void AS $$
DECLARE
    points real[][];
    pt real[];
    i integer;
    j integer;
    tmpVal varchar;
    tmpR real;
    num_dims integer;
    num_refs integer;
BEGIN
    points := '{}';
    
    num_dims := InfoGet('num_dims');
    num_refs := 2 * num_dims;

    RAISE NOTICE 'Number of dims: = %', num_dims;    
    RAISE NOTICE 'Number of refs: = %', num_refs;
    
    --set this info for later
    tmpVal := num_refs;
    perform InfoSet('num_refs', tmpVal);
 
    -- first we initialize all ref dims to 0.5
    for i in 1..num_refs loop
        
        pt := '{}';
        tmpR := 0.5;
        for j in 1..num_dims loop
            
            pt := pt || tmpR;
        end loop;
        
        points := points || ARRAY[pt];
            
    end loop;
    
    
    
    -- then we walk thru and set the 0,1 vals for each dim
    for i in 1..num_dims loop
        
        --raise notice 'points before = %', points;
        points[i][i] := 0;
        points[num_dims+i][i] := 1;
        --raise notice 'points after = %', points;
                
    end loop;
    
    
    --raise notice 'points = %', points;
    
    -- using manual counter for ID values (1-based to match array indexing)
    i := 1;
    FOREACH pt SLICE 1 IN ARRAY points LOOP
        RAISE NOTICE 'ref[%] = %', i, pt;
        INSERT into refs (id, dims) VALUES (i, pt);
        i := i + 1;
    END LOOP;
      
END;
$$ LANGUAGE plpgsql;




-- simple wrapper for any initializations required prior to index building
CREATE OR REPLACE FUNCTION InitOptions() RETURNS void AS $$
DECLARE
    c_val_str varchar;
    r_init_str varchar := '0.05';
    r_delt_str varchar := '0.05';
    
BEGIN
    
    select CalculateC() into c_val_str;
    execute InfoSet('c_val', c_val_str);
    
    -- query radius variables
    execute InfoSet('r_init', r_init_str);
    execute InfoSet('r_delt', r_delt_str);
    
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
            dist := Distance(ref, p.dims);
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
        index_y := CalculateY(curid, c_val, curdist);
        
        --index_y := (curid * c_val) + curdist;
        RAISE NOTICE '    index value => %', index_y;
        
        insert into index(id, val) VALUES (p.id, index_y);
    
    end loop;
    

END;
$$ LANGUAGE plpgsql;


-- perform a knn query on the table-based index
-- NOTE: this version manually redoes each query radius increase and
--       does not sort the order of points returned!
CREATE OR REPLACE FUNCTION QueryKNN_2(q real[], k int, OUT knn_ids int[]) AS $$
DECLARE
    
    r_init real := 0.0;
    r_delt real := 0.0;
    r real;
    dist real;
    
    pt data%ROWTYPE;
    done boolean;
    knn_count int;

BEGIN

    raise notice 'query q = %', q;
    raise notice '  with kNN where k = %', k;
    
    
    r_init := InfoGet('r_init');
    r_delt := InfoGet('r_delt');
    if r_init = 0.0 or r_delt = 0.0 then
        raise exception 'Invalid r values!';
    end if;
    
    done := false;
    r := r_init;
    knn_ids := '{}';
    knn_count := 0;
    
    --raise notice 'knn_ids = %', knn_ids;
    --raise notice 'knn_ids type = %', array_dims(knn_ids);
    
    while not done loop

        for pt in select * from QuerySphere(q, r) loop
            raise notice 'pt = %', pt;
            
            dist := Distance(q,pt.dims);
            if dist <= r then
                knn_ids := knn_ids || pt.id;
                knn_count := knn_count + 1;
            end if;
        end loop;
        
        raise notice 'knn size = %', array_length(knn_ids,1);
        
        -- for now, if we dont have them all we have to start over
        if knn_count < k then
            r := r + r_delt;
            knn_ids := '{}';
            knn_count := 0;
        else
            done := true;
        end if;
            
    end loop;
    
    raise notice 'knn_ids = %', knn_ids;
    
    return;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION QuerySphere(q real[], r real) RETURNS SETOF data AS $$
DECLARE
    dist real;
    i int;
    ref refs%rowtype;
    pt data%rowtype;
    q_index real;
    p_min real;
    p_max real;
    c_val real := 0.0;

    
BEGIN

    raise notice 'query sphere at %  (+/- %)', q, r;
    
    c_val := InfoGet('c_val');
    if c_val = 0.0 then
        raise exception 'Invalid c value!';
    end if;
    
    
    i := 0;
    for ref in select * from refs loop
        dist := Distance(q, ref.dims);
        raise notice 'ref % : % with dist = %', i, ref.dims, dist;
        
        if (dist - r) <= ref.distmax then
            raise notice '  overlap!';
          
            q_index := CalculateY(ref.id, c_val, dist);
            
            if dist <= ref.distmax then
                -- inside, so search 'in and out' from q
                raise notice '    inside!';
                p_min := dist - r;
                if p_min < 0.0 then
                    p_min := 0.0;
                end if;
                p_max := dist + r;
                if p_max > ref.distmax then
                    p_max := ref.distmax;
                end if;
                p_min := CalculateY(ref.id, c_val, p_min);
                p_max := CalculateY(ref.id, c_val, p_max);
                
                raise notice '  p_min, p_max = %, %', p_min, p_max;
                
                return query select * from data where data.id in (select index.id from index where index.val between p_min AND p_max);
                
--                return query select data.id, data.dims from data inner join index on data.id = index.id where index.val between 
                
            else
                -- intersects, so we search 'inward' from the partition edge
                raise notice '    intersect!';
                
                p_min := q_index - r;
                if p_min < 0.0 then
                    p_min := 0.0;
                end if;
                
                p_max := CalculateY(ref.id, c_val, ref.distmax);
                
                raise notice '  p_min, p_max = %, %', p_min, p_max;
                
                return query select * from data where data.id in (select index.id from index where index.val between p_min AND p_max);
                
            end if;
            
        end if;
        i := i + 1;
    end loop;


END;
$$ LANGUAGE plpgsql;


------------------------------------------------


--perform a knn query on the table-based index
--return $k nearest neighbors from query point $q 
CREATE OR REPLACE FUNCTION QueryKNN(q real[], k int, OUT knn_ids int[], OUT knn_dists real[]) AS $$
DECLARE

    num_refs int;
    c_val real;
    r_init real := 0.0;
    r_delt real := 0.0;
    
    -- flag for whether we've checked a partition yet 0 = no, 1 = yes
    p_checked int[]; 
    -- left/right cursors for each partition 
    -- (in order, so partition i has left ref [2i] and right ref [2i+1]
    p_refs refcursor[];
    p_ref refcursor;
    p_ref2 refcursor;
        
    pt data%ROWTYPE;
    ref refs%ROWTYPE;
    done boolean;
    knn_count int;
    knn_ids int[];
    knn_dists real[];
    r real;
    dist real;
    
    tmp real;
    qIndex real ARRAY;
    cand_ids int[];
    cand_dists real[];
    cand_count int;
    tmpRec RECORD;

BEGIN

    raise notice 'query q = %', q;
    raise notice '  with kNN where k = %', k;

    num_refs := InfoGet('num_refs');    
    raise notice 'num refs = %', num_refs;
    c_val := InfoGet('c_val');
    
    r_init := InfoGet('r_init');
    r_delt := InfoGet('r_delt');
    if r_init = 0.0 or r_delt = 0.0 then
        raise exception 'Invalid r values!';
    end if;
    
    done := false;
    r := r_init;
    knn_ids := '{}';
    knn_dists := '{}';
    knn_count := 0;
    
    -- initialize checked array to all 0's
    p_checked := ARRAY[0];
    for i in 2..num_refs loop
        p_checked := p_checked || 0;
    end loop;
    
    
    -- initialize all refcursors to null
    p_refs := ARRAY[NULL];
    p_refs := p_refs || '{NULL}';
    for i in 2..num_refs loop
        p_refs := p_refs || '{NULL}' || '{NULL}';
    end loop;
    
    
    -- initialize qIndex to all 0's
    -- this variable isn't necessary, but bounds the distance calculations
    -- needed, despite the iterations of radius increase
    tmp := 0.0; -- can't figure out how to assign '0.0' as a real value
    qIndex := ARRAY[tmp];
    for i in 2..num_refs loop
        qIndex := qIndex || tmp;
    end loop;
    
    --check everything looks good to go
    raise notice 'p_checked = %', p_checked;
    raise notice 'p_refs = %', p_refs;
    raise notice 'qIndex = %', qIndex;
    raise notice 'knn_ids = %', knn_ids;
    raise notice 'c_val = %', c_val;
    raise notice 'r_init, r_delt = %, %', r_init, r_delt;
    

    
    while not done loop
    
        raise notice '----------------------------------------';
        raise notice 'Searching with radius %', r;
        raise notice 'p_checked = %', p_checked;
        raise notice 'p_refs = %', p_refs;
        raise notice 'qIndex = %', qIndex;

        
        --now we loop over all refs and check for search ranges
        for i in 1..num_refs loop
        
            raise notice ' checking P%', i;
            
            if p_checked[i] = 0 then
                -- hasn't been checked yet, test overlaps
                select * from refs where refs.id = i into ref;
                raise notice '  not yet searched, ref = %', ref;
                dist := Distance(q, ref.dims);
                if (dist - r) <= ref.distmax then
                
                    -- q overlaps p somehow
                    -- mark we are now checking it and get our qIndex for later
                    p_checked[i] := 1;
                    qIndex[i] := CalculateY(i, c_val, dist);
                    
                    if dist < ref.distmax then
                        
                        raise notice '   q resides within P';
                        -- partition lower bound for limit of search in

                        tmp := CalculateY(i, c_val, 0.0);
                        p_ref := p_refs[(2*i)-1];
                        raise notice '    inward search bounds: %, %', tmp, qIndex[i];
                        open p_ref scroll for select data.id, data.dims, index.val from index inner join data on index.id = data.id where index.val >= tmp and index.val <= qIndex[i] order by index.val desc;
                        raise notice 'p_ref = %', p_ref;
                        
                        select * from SearchInOut(0, p_ref, qIndex[i] - r, q) into p_ref, cand_ids, cand_dists;
                        p_refs[(2*i)-1] := p_ref; -- have to reset this because of the "into" command above

                        select * from AddCandidates(k, knn_ids, knn_dists, cand_ids, cand_dists) into knn_ids, knn_dists, knn_count;
                        raise notice '    k set (%): %, %', knn_count, knn_ids, knn_dists;
                        
                        -------------------------------
                        
                        -- partition upper bound for limit of search out
                        tmp := CalculateY(i, c_val, ref.distmax);
                        p_ref := p_refs[(2*i)];
                        raise notice '    outward search bounds: %, %', qIndex[i], tmp;
                        open p_ref scroll for select data.id, data.dims, index.val from index inner join data on index.id = data.id where index.val >= qIndex[i] and index.val <= tmp order by index.val asc;
                        raise notice 'p_ref = %', p_ref;
                        
                        select * from searchInOut(1, p_ref, qIndex[i] + r, q) into p_ref, cand_ids, cand_dists;
                        p_refs[(2*i)] := p_ref;
                        
                        select * from AddCandidates(k, knn_ids, knn_dists, cand_ids, cand_dists) into knn_ids, knn_dists, knn_count;
                        raise notice '    k set (%): %, %', knn_count, knn_ids, knn_dists;
                
                    else
                    
                        raise notice '    q intersects P';
                        p_ref := p_refs[(2*i)-1];
                        -- partition lower and upper bounds
                        raise notice '    inward search bounds: %, %', CalculateY(i, c_val, 0.0), CalculateY(i, c_val, ref.distmax);
                        open p_ref scroll for select data.id, data.dims, index.val from index inner join data on index.id = data.id where index.val >= CalculateY(i, c_val, 0.0) and index.val <= CalculateY(i, c_val, ref.distmax) order by index.val desc;
                        raise notice 'p_ref = %', p_ref;
                        
                        --select * from searchInOut(0, p_refs[(2*i)-1], qIndex[i] - r) into p_refs[(2*i)-1], cands;
                        select * from searchInOut(0, p_ref, qIndex[i] - r, q) into p_ref, cand_ids, cand_dists;
                        p_refs[(2*i)-1] := p_ref;
                        
                        select * from AddCandidates(k, knn_ids, knn_dists, cand_ids, cand_dists) into knn_ids, knn_dists, knn_count;
                        raise notice '    k set (%): %, %', knn_count, knn_ids, knn_dists;
                
                    end if;
                
                else
                
                    raise notice '   q does not overlap P';
                
                end if; -- doesn't overlap, ignore it
                
            else
                --raise notice '  already searched before, continue..';
                -- already checked once, just continue now
                
                if p_refs[(2*i)-1] is not null then

                    raise notice '   continuing left';
                
                    p_ref := p_refs[(2*i)-1];
                    select * from searchInOut(0, p_ref, qIndex[i] - r, q) into p_ref, cand_ids, cand_dists;
                    p_refs[(2*i)-1] := p_ref;
                    
                    select * from AddCandidates(k, knn_ids, knn_dists, cand_ids, cand_dists) into knn_ids, knn_dists, knn_count;
                    raise notice '    k set (%): %, %', knn_count, knn_ids, knn_dists;
                
                end if;
                
                if p_refs[(2*i)] is not null then

                    raise notice '   continuing right';                    
                
                    p_ref := p_refs[(2*i)];
                    select * from searchInOut(1, p_ref, qIndex[i] + r, q) into p_ref, cand_ids, cand_dists;
                    p_refs[(2*i)] := p_ref;
                    
                    select * from AddCandidates(k, knn_ids, knn_dists, cand_ids, cand_dists) into knn_ids, knn_dists, knn_count;
                    raise notice '    k set (%): %, %', knn_count, knn_ids, knn_dists;
                
                end if;
                
            end if; -- partition overlap checks 
        
        end loop; -- over all partitions
    
        knn_count := array_length(knn_ids, 1);
        raise notice '  iteration finished with k = %', knn_count;
  
        -- already verified list, so only need to check total to quit
        if knn_count = k then
            if r > knn_dists[k] then
                done := true; -- quit the while loop
            end if;
        end if;
        
        r := r + r_delt;
        if r > c_val then -- hack to quit even if something went wrong
            raise notice 'r too big, failure somewhere!';
            done := true;
        end if;
    
    end loop; -- while not done with knn search
    
    raise notice 'final knn_ids = %', knn_ids;
    raise notice 'final knn_dists = %', knn_dists;
    
    
    return;

END;
$$ LANGUAGE plpgsql;


-- searches either IN (0; decreasing values) or OUT (1; increasing values) given the dir flag
-- uses query bounded refcursors as "pointers" to btree leaves
-- returns the updated pointer and candidates (ids, dists)
CREATE OR REPLACE FUNCTION SearchInOut(dir int, curIn refcursor, rStop real, qDims real[], OUT curOut refcursor, OUT cand_ids int[], OUT cand_dists real[]) AS $$
DECLARE

    done boolean;
    tmp record;
    dist real;
    
BEGIN

    --initialize return variables
    cand_ids := '{}';
    cand_dists := '{}';
    curOut := curIn;
    --note this is a shallow copy, so curOut always points where curIn does 
    
    done := false;
    if dir = 0 then
        raise notice '   search IN til < %', rStop;
    else
        raise notice '   search OUT til > %', rStop;
    end if;
    
    while not done loop
        
        -- gets the next item from the query the cursor is opened for
        fetch curIn into tmp; 
        raise notice '    fetch: %', tmp;
        
        if tmp is null then
            -- reached end of query set by cursor (partition boundary)
            -- even with radius increases, this won't find anything else
            -- so we use NULL to signify its over (i.e., don't even check it next iteration)
            raise notice '    bound terminated search!';
            curOut := NULL;
            done := true;
        
        else 
            if dir = 0 and tmp.val < rStop then
                -- reached end of radius going inward (0)
                -- might still be more points left to search next iteration
                -- backup one so we refetch what tmp is (since we have to get it to know when to stop)
                raise notice '    radius terminated search!';
                move -1 from curIn;
                done := true;
            elsif dir = 1 and tmp.val > rStop then
                -- reached end of radius going outward (1)
                raise notice '    radius terminated search!';
                move -1 from curIn;
                done := true;                
            else
                -- either direction, the index value is within our radius so collect it
                raise notice '    adding % to cands!', tmp.id;
                dist := Distance(tmp.dims, qDims);
                cand_ids := cand_ids || tmp.id;
                cand_dists := cand_dists || dist;
                
                
            end if;
        
        end if;
        
    end loop;
    
    return;
    
END;
$$ LANGUAGE plpgsql;




-- updates our true knn set with a potential set of candidates
CREATE OR REPLACE FUNCTION AddCandidates(knn_max int, knn_ids int[], knn_dists real[], cand_ids int[], cand_dists real[], OUT knn_ids_out int[], OUT knn_dists_out real[], OUT knn_count int) AS $$
DECLARE

    cand_count int;
    j int;
    added boolean;
    
BEGIN

    cand_count := array_length(cand_ids,1);
    knn_count := array_length(knn_ids,1);
    
    if knn_count is null then
        knn_count := 0;
    end if;
    
   
    if cand_count is null then
        -- nothing to even add, get out of here!
        knn_ids_out := knn_ids;
        knn_dists_out := knn_dists;
        return;
    end if;
    
    raise notice '     knn max = %, cur count = %', knn_max, knn_count;
    raise notice '     adding cands (%) = %, %', cand_count, cand_ids, cand_dists;
    
    
    --start with our return set empty
    knn_ids_out := '{}';
    knn_dists_out := '{}';
    
    --have look at each candidate either way
    for i in 1..cand_count loop
        
        -- lazy sorting, always improveable later
        -- just iterate through whole knn list for each one (N^2)
        added := false;
        
        if knn_count = 0 then
            
            -- list is empty, prime it with one
            raise notice '    prime list!';
            knn_ids_out := knn_ids_out || cand_ids[i];
            knn_dists_out := knn_dists_out || cand_dists[i];
            knn_count := 1;                 
            added := true;
            
        else
            
            for j in 1..knn_count loop
            
                if added = true then
                    -- just add the rest quick, but truncate if nec.
                    
                    knn_ids_out := knn_ids_out || knn_ids[j];
                    knn_dists_out := knn_dists_out || knn_dists[j];               
                    
                    
                elsif cand_dists[i] < knn_dists[j] then
            
                    -- belongs right before this item in the current list
                    raise notice '   insert here: %, %', cand_dists[i], knn_dists[j];
                    knn_ids_out := knn_ids_out || cand_ids[i] || knn_ids[j];
                    knn_dists_out := knn_dists_out || cand_dists[i] || knn_dists[j];
                    knn_count := knn_count + 1;                 
                    added := true;
                    
                else
                
                    -- add and move to next one
                    knn_ids_out := knn_ids_out || knn_ids[j];
                    knn_dists_out := knn_dists_out || knn_dists[j];
                
                end if;
                
            end loop;
            
            if added = false then
                if knn_count < knn_max then
                    --wasn't added, but we don't have a full list, append at end
                    knn_ids_out := knn_ids_out || cand_ids[i];
                    knn_dists_out := knn_dists_out || cand_dists[i];
                    knn_count := knn_count + 1;
                end if;
            end if;
        end if; --adding each candidate
        
        raise notice 'next cand!';
        knn_ids := knn_ids_out;
        knn_dists := knn_dists_out;
        knn_ids_out := '{}';
        knn_dists_out := '{}';
            
                        
    end loop; -- end for each cand
    
    knn_ids_out := knn_ids;
    knn_dists_out := knn_dists;
    
    return;

END;
$$ LANGUAGE plpgsql;

