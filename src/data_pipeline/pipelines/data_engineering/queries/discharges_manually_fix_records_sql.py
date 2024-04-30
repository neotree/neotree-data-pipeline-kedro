def manually_fix_discharges_query():
    #To Add Code Once Fixes Are Available
    return f''' 
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
        
        {update_meds_given('Ampicillin','AMP')}
        {update_meds_given('X-penicillin and gentamicin','ABX')}
        {update_meds_given('Amoxicillin','AMOX')}
        {update_meds_given('AZT (Zidovudine)','AZT')}
        {update_meds_given('X penicillin','BP')}
        {update_meds_given('Caffeine','CAF')}
        {update_meds_given('Ceftriaxone','CEF')}
        {update_meds_given('Gentamicin','GENT')}
        {update_meds_given('Nevirapine','NVP')}
        {update_meds_given('Other','OTH')}
        {update_meds_given('Paracetamol','PCM')}
        {update_meds_given('Phenobarbitone','PHEN')}
        
        {update_meds_given('Amikacin','AMIK')}
        {update_meds_given('Aminophylline','AMIN')}
        {update_meds_given('BCG Vaccine','BCG')}
        {update_meds_given('Cloxacillin','CLOX')}
        {update_meds_given('Folate','FOL')}
        {update_meds_given('Iron','IRON')}
        {update_meds_given('Metronidazole','MET')}
        {update_meds_given('Procaine Penicillin','PROC')}
        {update_meds_given('Vancomycin','VAN')}
        {update_meds_given('Vitamin D','VITD')} 
        {update_meds_given('Imipenem','IMI')}

        '''
        
    
        
def update_meds_given(name,value):
    return f'''
        update derived.discharges set "MedsGiven.label" = '{name}' where "MedsGiven.value" = '{value}' and "MedsGiven.label" = 'None';;
    '''        
        
        
        # UPDATE DERIVED.DISCHARGES SET "MedsGiven.label" = "MedsGiven.value" WHERE "MedsGiven.label" = 'None';;