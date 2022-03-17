
#Query to create summary neolab table
def summary_neolab_query():
  return   f''' DROP TABLE IF EXISTS derived.summary_neolab;
                CREATE TABLE derived.summary_neolab AS 
                (
                  WITH latest_neolab as (
                  SELECT
                    "facility",
                    "uid",
                    max("DateBCR.value") as "DateBCR.value" -- Take The Last Record
                    from derived.neolab where uid not like '0000%' and uid not like '***%'
                    group by 1,2
                    )
                    SELECT 
                    latest_neolab."facility",
                    latest_neolab."uid",
                    latest_neolab."DateBCR.value" AS "DateBCR",
                    derived.neolab."BCResult.value" AS "BCResult",
                    CASE WHEN 
                    derived.neolab."BCType" like '%PRELIMINARY%' THEN 'PRELIMINARY'
                    WHEN derived.neolab."BCType" like '%FINAL%' THEN 'FINAL'
                    END AS "Status",
                    derived.neolab."DateBCT.value" AS "DATEBCT",
                    count(latest_neolab."uid") as "Total Blood Cultures"
                    from latest_neolab join derived.neolab on
                    latest_neolab."DateBCR.value" = derived.neolab."DateBCR.value"
              );'''