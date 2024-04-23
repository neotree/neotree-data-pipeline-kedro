def manually_fix_discharges_query():
    #To Add Code Once Fixes Are Available
    return ''' 
        UPDATE derived.discharges
        SET "NeoTreeOutcome.label" = CASE
                            WHEN "NeoTreeOutcome.value" = 'DC' THEN 'Discharged'
                            WHEN "NeoTreeOutcome.value" = 'Discharged' THEN 'Discharged'
                            WHEN "NeoTreeOutcome.value" = 'NND' THEN 'Died'
                            WHEN "NeoTreeOutcome.value" = 'ABS' THEN 'Absconded'
                            WHEN "NeoTreeOutcome.value" = 'TRH' THEN 'Transferred to other Hospital'
                            WHEN "NeoTreeOutcome.value" = 'TRO' THEN 'Transferred to other ward'
                            WHEN "NeoTreeOutcome.value" = 'DAMA' THEN 'Discharged against medical advice'
                            WHEN "NeoTreeOutcome.value" = 'UK' THEN 'Unknown'
                            WHEN "NeoTreeOutcome.value" = 'BID' THEN 'Brought in dead' 
                            ELSE trim("NeoTreeOutcome.value")
                        END
        WHERE "NeoTreeOutcome.label" = 'None';;
        
        UPDATE DERIVED.DISCHARGES SET "DIAGDIS1.label" = "DIAGDIS1.value" WHERE "DIAGDIS1.label" = 'None';;
        UPDATE DERIVED.DISCHARGES SET "MedsGiven.label" = "MedsGiven.value" WHERE "MedsGiven.label" = 'None';;

        '''