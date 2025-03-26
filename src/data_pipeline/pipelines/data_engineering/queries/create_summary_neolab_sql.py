from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
#Query to create summary neolab table
def summary_neolab_query():
  prefix = f''' drop table if exists derived.summary_neolab cascade;;
                 create table derived.summary_neolab as    (
                '''
  suffix = ')'
  where = ''
  if(table_exists("derived","summary_neolab")):
    prefix= f''' INSERT INTO derived.summary_neolab (
    "facility", 
    "uid", 
    "episode", 
    "DateBCR", 
    "Org1.label", 
    "Org1.value", 
    "OtherOrg1.value", 
    "BCResult", 
    "Status", 
    "DATEBCT", 
    "NumberOfCulturesForEpisode", 
    "CombinedResult"
        )  '''
    where = f''' AND  NOT EXISTS ( SELECT 1  FROM derived.summary_neolab  WHERE "uid" IN (select uid from derived.neolab)) '''
    suffix =''

  return   prefix+f'''
            
         
            with latest_neolab as (
            select
            facility,
            uid,
            episode, 
            max("DateBCR.value") as "DateBCR" -- This takes the last upload 
                    -- most recently uploaded
            from derived.neolab
            where uid not like '0000%%' and uid not like '***%%' {where}
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
            {suffix};;'''