
#Query to create summary neolab table
def summary_neolab_query():
  return   f''' drop table if exists derived.summary_neolab cascade;
            create table derived.summary_neolab as 
            (
            with earliest_neolab as (
            select
            facility,
            uid, 
            min("DateBCR.value") as "DateBCR" -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
            from derived.neolab
            where uid not like '0000%' and uid not like '***%'
            group by 1,2
            )
            select
            earliest_neolab.facility,
            earliest_neolab.uid,
            earliest_neolab."DateBCR",
            derived.neolab."BCResult.value" AS "BCResult",
                    CASE WHEN 
                    derived.neolab."BCType" like '%PRELIMINARY%' THEN 'PRELIMINARY'
                    WHEN derived.neolab."BCType" like '%FINAL%' THEN 'FINAL'
                    END AS "Status",
                    derived.neolab."DateBCT.value" AS "DATEBCT",
				    (select count(derived.neolab.uid) from derived.neolab where 
				    earliest_neolab.uid=derived.neolab."uid") AS "Number Of Cultures"        
            from earliest_neolab join derived.neolab
            on earliest_neolab."DateBCR" = derived.neolab."DateBCR.value" 
            );'''