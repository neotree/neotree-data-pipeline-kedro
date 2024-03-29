
#Query to create summary neolab table
def summary_neolab_query():
  return   f''' drop table if exists derived.summary_neolab cascade;;
            create table derived.summary_neolab as 
            (
            with latest_neolab as (
            select
            facility,
            uid,
            episode, 
            max("DateBCR.value") as "DateBCR" -- This takes the last upload 
                    -- most recently uploaded
            from derived.neolab
            where uid not like '0000%%' and uid not like '***%%'
            group by 1,2,3
            )
            select
            latest_neolab.facility,
            latest_neolab.uid,
            latest_neolab.episode,
            latest_neolab."DateBCR",
            derived.neolab."Org1.label",
            derived.neolab."Org1.value",
            derived.neolab."OtherOrg1.value",
            derived.neolab."BCResult.value" AS "BCResult",
                    CASE WHEN 
                    derived.neolab."BCType" like '%%PRELIMINARY%%' THEN 'PRELIMINARY'
                    WHEN derived.neolab."BCType" like '%%FINAL%%' THEN 'FINAL'
                    END AS "Status",
                    derived.neolab."DateBCT.value" AS "DATEBCT",
				    (select count(derived.neolab.uid) from derived.neolab where 
				    latest_neolab.uid=derived.neolab."uid" and latest_neolab.episode = derived.neolab."episode") AS "NumberOfCulturesForEpisode",
           CASE WHEN (derived.neolab."BCResult.value" ='Pos' and  derived.neolab."Org1.value" ='CONS') OR 
                    (derived.neolab."BCResult.value" ='PC') THEN 'Contaminant'
                WHEN ((CURRENT_DATE - derived.neolab."DateBCR.value"::date) <= 5 
                     and (derived.neolab."BCResult.value"='NegP' or derived.neolab."BCResult.value"='PosP'))
                THEN 'Awaiting Final Result'
                ELSE
                    derived.neolab."BCResult.value" END AS "CombinedResult"   
            from latest_neolab join derived.neolab
            on latest_neolab.uid=derived.neolab."uid" and latest_neolab."DateBCR" = derived.neolab."DateBCR.value" 
            );;'''