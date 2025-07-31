import logging
from conf.common.sql_functions import inject_sql,column_exists,inject_sql_with_return,get_table_column_type,inject_sql_procedure
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.common.format_error import formatError

def generate_update_query(facility, variable, to_update, values,table,where):

    additional_where = f''' and facility='{facility}' '''
    if(facility=='PHC' or facility==''):
        additional_where=''
    # Start building the update query
    query_parts = []
    for reference, new_value in values:
        # Only consider the rows for the specified facility 
        query_parts.append(f''' WHEN "{variable}" = '{reference}' THEN '{new_value}' ''')

    # Build the full SQL update query
    if query_parts:
        update_query = f"""
        UPDATE derived.{table}
        SET "{to_update}" = CASE WHEN "{variable}" is NULL THEN NULL
            { ' '.join(query_parts) }
            ELSE "{to_update}" -- Keep the existing value if no match
        END
        WHERE "{to_update}" ILIKE '{where}'  {additional_where};;
        """
        return update_query.strip()
    else:
        return ""
    
def update_refferred_from():
    facilities = ['KDH','BHC'] 
    variable = "ReferredFrom.value"
    to_update = "ReferredFrom.label"
    table="admissions"
    where = "Name of%"

    for facility in facilities:
        if facility=='BHC' or facility=='KDH' :
            values = '''AncF,Anchor Farm
                        Bua,Bua
                        Cha,Chambwe
                        Chm,Chamwabvi
                        Chi,Chilanga
                        Chn,Chinyama
                        Chu,Chulu
                        Gog,Gogode
                        Kak,Kakwale
                        Kal,Kaluluma
                        Kam,Kamboni
                        Kap,Kapelula
                        Kai,Kapichira
                        Kay,Kapyanga
                        Kaw,Kawamba
                        Kho,Khola
                        Lin,Linyangwa
                        Liv,Livwezi
                        Lod,Lodjwa
                        Mdu,Mdunga
                        Mkh,Mkhota
                        Mny,Mnyanja
                        Mpe,Mpepa
                        Mtu,Mtunthama
                        Mzi,Mziza
                        New,Newa
                        Nkh,Nkhamenya
                        Ofe,Ofesi
                        San,Santhe
                        Sim,Simlemba
                        StA,St Andrew\\’s
                        StD,St Dennis
                        StF,St Faith
                        Wim,Wimbe
                        O,Other'''
            transformed= transform_values(values)
            if len(transformed)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
                query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
                query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
                if len(query1)>0:
                    inject_sql(query1,"UPDATE REFFERRED FROM")
                if len(query2)>0:
                    inject_sql(query2,"UPDATE REFFERRED FROM IN JOINED ADMISSIONS")

    
def update_age_category():
    scripts = ['admissions','joined_admissions_discharges']
    for script in scripts:
        query = f'''UPDATE derived.{script} set "AgeCategory" = 
        CASE WHEN "Age.value" is NULL THEN 'Unknown'
        WHEN "Age.value"<2 THEN 'Fresh Newborn (< 2 hours old)'
        WHEN "Age.value"<=23 THEN 'Newborn (2 - 23 hrs old)'
        WHEN "Age.value"<=47 THEN 'Newborn (1 day - 1 day 23 hrs old)'
        WHEN "Age.value"<=71 THEN 'Infant (2 days - 2 days 23 hrs old)' 
        ELSE 'Infant (> 3 days old)' END 
        where "AgeCategory" is NULL;;
        '''
        inject_sql(query,f"UPDATE AGE {script}")

def update_admission_weight():
    scripts = ['admissions','joined_admissions_discharges']
    for script in scripts:
        query = f'''UPDATE derived.{script} set "AWGroup.value" = 
        CASE WHEN "AdmissionWeight.value" is NULL THEN 'Unknown'
        WHEN "AdmissionWeight.value" >=4000 THEN '>4000g'
        WHEN "AdmissionWeight.value" <4000 THEN '2500-4000g'
        WHEN "AdmissionWeight.value"<2500 THEN '1500-2500g'
        WHEN "AdmissionWeight.value"<1500 THEN '1000-1500g' 
        ELSE '<1000g' END
        where "AWGroup.value" is NULL;;
        '''
        inject_sql(query,f"UPDATE AdmissionWeight {script}")

def update_mode_delivery():
     facilities = ['KDH','BHC','KCH','PHC'] 
     variable = "ModeDelivery.value"
     to_update = "ModeDelivery.label"
     table="admissions"
     where = "Mode of%"
     if not table_exists('derived',table_name=table):
         return
  
     values=f'''1,Spontaneous Vaginal Delivery (SVD)
                2,Vacuum extraction
                3,Forceps extraction
                4,Elective Ceasarian Section (ELCS)
                5,Emergency Ceasarian Section (EMCS)
                6,Breech extraction (vaginal)
                7,Induced Vaginal Delivery
                SVD,Spontaneous Vaginal Delivery (SVD)
                CSPrLab,Caeser before onset labour
                CSPoLab,Caeser after onset labour
                '''
            
     transformed= transform_values(values)
     if len(transformed):
                
        query3 = generate_update_query('',variable,to_update,transformed,"phc_admissions",where)              
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where) 

     if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
        inject_sql(query1,"UPDATE MODE DELIVERY FROM")
     if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
        inject_sql(query2,"UPDATE DELIVERY IN JOINED ADMISSIONS")
     if len(query3)>0 and  table_exists('derived',table_name='phc_admissions') and column_exists("derived","phc_admissions",to_update):
        inject_sql(query3,"UPDATE PHC ADMISSIONS")


def update_signature():
     facilities = ['KDH','BHC','KCH','PHC'] 
     variable = "HCWSig.value"
     to_update = "HCWSig.label"
     table="admissions"
     where = "Electronic%"
     if not table_exists('derived',table_name=table):
         return
     for facility in facilities:
        values=[]
        if facility=='KCH':
            values=f'''RUMA,Ruth Mases
                       BETE,Bentley Tembo
                       BERN,Bernadette Nyambo
                       BRNA,Bridget Namanya
                       DAMU,Daniel Mughyombe
                       ELTE,Elvin Tembo
                       MAGF,Margret Fikilini
                       FEZA,Fe Zambezi
                       FUST,Funnie Steven
                       GLZA,Gloria Zailani
                       TANY,Tamara Nyasulu
                       LISA,Linda Saidi
                       LIPH,Linna Phiri
                       MPGR,Mphatso Grant
                       NAME,Naomie Meke
                       NAKH,Naomie Khurungira
                       PRDI,Precious Dinga
                       PRMA,Prince Magawa
                       RHCH,Rhoda Chifisi
                       PAKA,Pamela Kawaga
                       TAWH,Tamandani Whayo
                       WAKU,Watson Kumwenda
                       BRMH,Brandina Mhlanga
                       OMMA,Omega Makonde
                       OTH,Other'''
            
        elif facility=='BHC':
            values= f'''GeYo,Gerson Yohane
                        GrKh,Griceria Khumalo
                        LiMv,Linda Mvula
                        MeNa,Melina Naman
                        PaSa,Patrick Sani
                        PeCh,Peter Chigome
                        OTH,Other'''
            
        elif facility=='KDH':
            values= f'''BrNa,Bridget Namisikha
                        CaBa,Carien Banda
                        DaNy,Davie Flemmings Nyirenda
                        DoCh,Dorica Chisasula
                        HaCh,Hannock Chingondo
                        JoMw,John Mwafulirwa
                        LiMu,Dr Linily Musa
                        MaFr,Mark Frazer
                        MaMn,Martha Mndzeka
                        NeCh,Nellie Chirwa
                        OlMk,Olive Mkoma
                        PhNa,Philomena Nambuzi
                        RuMa,Ruth Mases
                        TiMw,Tiwonge Mwale
                        ViGa,Victoria Gausi
                        WeMp,Wezzie Mphande
                        WiCh,Winfred Chunda
                        OTH,Other\\/ Student
                        '''
        elif facility=='PHC':
            values=f'''ALMBA,ALICE MSOMPHA BANDA
                        CHBA,CHIMWEMWE BANDA
                        CHKA,CHIMWEMWE KAMONJOLA
                        CHMW,CHIMWEMWE MWALE
                        MMU, MPHATSO MUGONYA
                        TRIBA,TRIZZA BANDA
                        JICHI,JINDA CHIPEMBERE
                        IRENK,IREEN NKHOMA
                        JOCHI,JOHN CHIKOKO
                        FAKAJ,FANNY KAJEDULA
                        TILIK,TIONGE LIKWEMBA
                        TIKAS,TIA KASERA
                        JATHI,JANE THINDWA
                        CHKAS, CHISOMO KASEKA
                        CHPH,CHINSISI PHIRI
                        CLMA,CLARA MAWANGA
                        DAMA,DALITSO MALANGO
                        DECH,DEBORA CHADZA
                        ESCH,ESAU CHIKOLERA
                        EUKA,EUNICE KATONDO
                        FAMA,FANY MATAKA
                        FYGO,FYNESS GONDWE
                        JEKW,JESSY KWATAINE
                        JUPH,JUSTINA PHIRI
                        LEPH,LEMSON PHIRI
                        LIDO,LIANA DOMINIC
                        MAJCH,MAYESO J.E. CHIKWALIKWALI
                        MAKA,MAUREEN KATSACHE
                        MAMA,MADALO MATEWERE
                        MAMK,MARTHA MKANDAWIRE
                        MAMT,MASAUTSO MTIYESANJI
                        MPMA,MPHATSO MASIYE
                        NAJE,NAOMI JEKEMU
                        OLME,OLIPA MEKE
                        PCH,P. CHIGWENEMBE
                        PRMB,PRISCA MBEWE
                        RUVY,RUTH VYSON
                        SEMD,SENITA MDULAMIZU
                        TIKA,TIYAMIKE KACHIGAYO
                        TRSI,TREZER SIYADO
                        UNMW,UNITY MWENDO
                        VEKU,Veronica Kunkeyani
                        YOMI,YONA MISINDE
                        ZIPE,ZIONE PERERA
                        STU,Student
                        OTH,Other
                        '''    
        transformed= transform_values(values)
        if len(transformed)>0:
            query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
            query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
            query3=''
            if facility=='PHC':
                table3='phc_admissions'
                query3 = generate_update_query(facility,variable,to_update,transformed,table3,where)

            if len(query1)>0 and column_exists("derived",table,to_update) :
                inject_sql(query1,"UPDATE MODE DELIVERY IN ADMISSIONS")
            if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) :
                inject_sql(query2,"UPDATE DELIVERY IN JOINED ADMISSIONS")
            if len(query3)>0 and column_exists("derived","phc_admissions",to_update):
                inject_sql(query3,"UPDATE PHC ADMISSIONS")       


def transform_values(input_string):
    lines = input_string.strip().split('\n')
    result = []

    for line in lines:
        # Split each line by comma to get key and value
        parts = line.split(',')
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip()
            result.append((key, value))

    return result

def update_cause_death():
    facilities = ['SMCH','CPH','BPH','PGH'] 
    variable = "CauseDeath.value"
    to_update = "CauseDeath.label"
    table="discharges"
    where = "Cause%"

    for facility in facilities:
        values=[]
        if facility=='SMCH':
            values=f'''An,Anaemia
                        BBA,Born before arrival
                        BI,Birth trauma
                        Cong,Congenital Abnormality
                        CHD,Consider Congenital Heart Disease
                        Conv,Convulsions
                        Dhyd,Dehydration
                        DF,Difficulty feeding
                        DUM,Dumped baby
                        HIVLR,HIV low risk
                        HIVHR,HIV high risk
                        HIVU,HIV unknown
                        HypogAs,Hypoglycaemia (NOT symptomatic)
                        HypogSy,Hypoglycaemia (Symptomatic)
                        RiHypog,Risk of hypoglycaemia
                        BA,Hypoxic Ischaemic Encephalopathy
                        GSch,Gastroschisis
                        MJ,Physiological Jaundice
                        LBW,Low Birth Weight (1500-2499g)
                        VLBW,Very Low Birth Weight (1000-1499g)
                        ExLBW,ExtremelyLow Birth Weight (<1000g)
                        MA,Possible Meconium Aspiration
                        MecEx,Meconium exposure (asymptomatic baby)
                        MiHypo,Mild Hypothermia
                        ModHypo,Moderate Hypothermia
                        SHypo,Severe Hypothermia
                        Hyperth,Hyperthermia
                        SEPS,Neonatal Sepsis
                        Risk,Risk factors for sepsis (asymptomatic baby)
                        NB,Normal baby
                        PN,Pneumonia / Bronchiolitis
                        Prem,Premature (32-36 weeks)
                        VPrem,Very Premature (28-31 weeks)
                        ExPrem,Extremely Premature (<28 weeks)
                        PremRD,Prematurity with RD 
                        Safe,Safekeeping
                        TTN,Transient Tachypnoea of Newborn (TTN)
                        OTH,Other
                        HBW,Macrosomia (>4000g)
                        TermRD, Term with RD
                        DJ,Pathological Jaundice
                        sHIE,Suspected Hypoxic Ischaemic Encephalopathy
                        PJaundice,Prolonged Jaundice
                        CleftLip,Cleft lip
                        CleftRD,Cleft lip and/or palate with RD
                        CleftLipPalate,Cleft lip and/or palate
                        Omph,Omphalocele
                        Myelo,Myelomeningocele
                        CDH,Congenital Dislocation of the hip (CDH)
                        MiTalipes,Mild Talipes (club foot)
                        MoTalipes,Moderate Talipes (club foot)
                    '''
        elif facility=='CPH' or facility=='BPH':
             values=f'''ASP,Aspiration
                        CA,Congenital Abnormality incompatible with life
                        HIE,Hypoxic Ischaemic Encephalopathy
                        Gastro,Gastroschisis
                        NEC,Necrotising Enterocolitis
                        EONS,Neonatal Sepsis - Early Onset
                        LONS,Neonatal Sepsis - Late Onset
                        MAS,Meconium aspiration syndrome
                        PR,Prematurity
                        PRRDS,Prematurity with RDS
                        PN,Pneumonia
                        OTH,Other

                    '''
        elif facility=='PGH':
             values=f'''
                An,Anaemia
                BBA,Born before arrival
                BI,Birth trauma
                Cong,Congenital Abnormality
                CHD,Consider Congenital Heart Disease
                Conv,Convulsions
                Dhyd,Dehydration
                DF,Difficulty feeding
                DUM,Abandoned baby
                HIV, HIV exposed
                HypogAs,Hypoglycaemia (NOT symptomatic)
                HypogSy,Hypoglycaemia (Symptomatic)
                RiHypog,Risk of hypoglycaemia
                BA,Hypoxic Ischaemic Encephalopathy, Birth asphyxia
                GSch,Gastroschisis
                MJ,Physiological Jaundice
                LBW,Low Birth Weight (1500-2499g)
                VLBW,Very Low Birth Weight (1000-1499g)
                ExLBW,ExtremelyLow Birth Weight (<1000g)
                MA,Possible Meconium Aspiration
                MecEx,Meconium exposure (asymptomatic baby)
                MiHypo,Mild Hypothermia
                ModHypo,Moderate Hypothermia
                SHypo,Severe Hypothermia
                Hyperth,Hyperthermia
                SEPScul,Neonatal Sepsis (culture confirmed)
                SEPSclin, Neonatal Sepsis (clinical diagnosis only)
                Risk,Risk factors for sepsis (asymptomatic baby)
                NB,Normal baby
                PN,Pneumonia
                Prem,Premature (32-36 weeks)
                VPrem,Very Premature (28-31 weeks)
                ExPrem,Extremely Premature (<28 weeks)
                PremRD,Prematurity with Respiratory distress 
                Safe,Safekeeping
                TTN,Transient Tachypnoea of Newborn (TTN)
                OTH,Other
                HBW,Macrosomia (>4000g)
                TermRD, Term with Respiratory distress
                DJPh, Pathological jaundice (phototherapy range)
                DJEx, Pathological jaundice (exchange transfusion required)
                sHIE,Suspected Hypoxic Ischaemic Encephalopathy
                PJaundice,Prolonged Jaundice
                CleftLip,Cleft lip
                CleftRD,Cleft lip and/or palate with RD
                CleftLipPalate,Cleft lip and/or palate
                Omph,Omphalocele
                Myelo,Myelomeningocele
                CDH,Congenital Dislocation of the hip (CDH)
                MiTalipes,Mild Talipes (club foot)
                MoTalipes,Moderate Talipes (club foot)
            '''
        transformed= transform_values(values)
        if len(transformed)>0:
            query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
            query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
            if len(query1)>0 and column_exists("derived",table,to_update) :
                inject_sql(query1,"UPDATING CAUSE OF DEATH")
            if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) :
                inject_sql(query2,"UPDATING CAUSE OF DEATH IN JOINED ADMISSIONS")

def update_disdiag():
    facilities = ['SMCH','CPH','BPH','PGH'] 
    variable = "DIAGDIS1.value"
    to_update = "DIAGDIS1.label"
    table="discharges"
    where = "Primary%"

    for facility in facilities:
        values=[]
        if facility=='SMCH':
            values=f'''An,Anaemia
                        BBA,Born before arrival
                        BI,Birth trauma
                        Cong,Congenital Abnormality
                        CHD,Consider Congenital Heart Disease
                        Conv,Convulsions
                        Dhyd,Dehydration
                        DF,Difficulty feeding
                        DUM,Abandoned baby
                        HIVLR,HIV low risk
                        HIVHR,HIV high risk
                        HIVU,HIV unknown
                        HypogAs,Hypoglycaemia (NOT symptomatic)
                        HypogSy,Hypoglycaemia (Symptomatic)
                        RiHypog,Risk of hypoglycaemia
                        BA,Hypoxic Ischaemic Encephalopathy
                        GSch,Gastroschisis
                        MJ,Physiological Jaundice
                        LBW,Low Birth Weight (1500-2499g)
                        VLBW,Very Low Birth Weight (1000-1499g)
                        ExLBW,ExtremelyLow Birth Weight (<1000g)
                        MA,Possible Meconium Aspiration
                        MecEx,Meconium exposure (asymptomatic baby)
                        MiHypo,Mild Hypothermia
                        ModHypo,Moderate Hypothermia
                        SHypo,Severe Hypothermia
                        Hyperth,Hyperthermia
                        SEPS,Neonatal Sepsis
                        Risk,Risk factors for sepsis (asymptomatic baby)
                        NB,Normal baby
                        PN,Pneumonia / Bronchiolitis
                        Prem,Premature (32-36 weeks)
                        VPrem,Very Premature (28-31 weeks)
                        ExPrem,Extremely Premature (<28 weeks)
                        PremRD,Prematurity with RD 
                        Safe,Safekeeping
                        TTN,Transient Tachypnoea of Newborn (TTN)
                        OTH,Other
                        HBW,Macrosomia (>4000g)
                        TermRD, Term with RD
                        DJ,Pathological Jaundice
                        sHIE,Suspected Hypoxic Ischaemic Encephalopathy
                        PJaundice,Prolonged Jaundice
                        CleftLip,Cleft lip
                        CleftRD,Cleft lip and/or palate with RD
                        CleftLipPalate,Cleft lip and/or palate
                        Omph,Omphalocele
                        Myelo,Myelomeningocele
                        CDH,Congenital Dislocation of the hip (CDH)
                        MiTalipes,Mild Talipes (club foot)
                        MoTalipes,Moderate Talipes (club foot)
                    '''
        elif facility=='CPH' or facility=='BPH':
             values=f'''AN,Anaemia
                        HIE,Hypoxic Ischaemic Encephalopathy
                        BI,Birth Injury
                        BBA,Born before arrival
                        BO,Bowel Obstruction
                        CHD,Congenital Heart Disease
                        DEHY,Dehydration
                        FD,Feeding Difficulties
                        G,Gastroschisis
                        HIVXH,HIV Exposed High Risk
                        HIVXL,HIV Exposed Low Risk
                        JAUN,Jaundice
                        LBW,Low Birth Weight
                        MA,Meconium Aspiration
                        Mac,Macrosomia
                        MD,Musculoskeletal Abnormalities
                        NEC,Necrotising Enterocolitis
                        EONS,Neonatal Sepsis - Early Onset
                        LONS,Neonatal Sepsis - Late Onset
                        NB,Normal baby
                        OM,Omphalocele
                        OCA,Other congenital abnormality
                        PR,Prematurity
                        PRRDS,Prematurity with RDS
                        PN,Pneumonia
                        Ri,Risk of sepsis 
                        Safe,Safekeeping
                        Twin,Accompanying Twin
                        TTN,Transient Tachypnoea Newborn
                        OTH,Other
                    '''
        elif facility=='PGH':
             values=f'''
               An,Anaemia
                BBA,Born before arrival
                BI,Birth trauma
                Cong,Congenital Abnormality
                CHD,Consider Congenital Heart Disease
                Conv,Convulsions
                Dhyd,Dehydration
                DF,Difficulty feeding
                DUM,Abandoned baby
                HIV, HIV exposed
                HypogAs,Hypoglycaemia (NOT symptomatic)
                HypogSy,Hypoglycaemia (Symptomatic)
                RiHypog,Risk of hypoglycaemia
                BA,Hypoxic Ischaemic Encephalopathy, Birth asphyxia
                GSch,Gastroschisis
                MJ,Physiological Jaundice
                LBW,Low Birth Weight (1500-2499g)
                VLBW,Very Low Birth Weight (1000-1499g)
                ExLBW,Extremely Low Birth Weight (<1000g)
                MA,Possible Meconium Aspiration
                MecEx,Meconium exposure (asymptomatic baby)
                MiHypo,Mild Hypothermia
                ModHypo,Moderate Hypothermia
                SHypo,Severe Hypothermia
                Hyperth,Hyperthermia
                SEPScul,Neonatal Sepsis (culture confirmed)
                SEPSclin, Neonatal Sepsis (clinical diagnosis only)
                Risk,Risk factors for sepsis (asymptomatic baby)
                NB,Normal baby
                PN,Pneumonia
                Prem,Premature (32-36 weeks)
                VPrem,Very Premature (28-31 weeks)
                ExPrem,Extremely Premature (<28 weeks)
                PremRD,Prematurity with Respiratory distress 
                Safe,Safekeeping
                TTN,Transient Tachypnoea of Newborn (TTN)
                OTH,Other
                HBW,Macrosomia (>4000g)
                TermRD, Term with Respiratory distress
                DJPh, Pathological jaundice (phototherapy range)
                DJEx, Pathological jaundice (Exchange transfusion required)
                sHIE,Suspected Hypoxic Ischaemic Encephalopathy
                PJaundice,Prolonged Jaundice
                CleftLip,Cleft lip
                CleftRD,Cleft lip and/or palate with RD
                CleftLipPalate,Cleft lip and/or palate
                Omph,Omphalocele
                Myelo,Myelomeningocele
                CDH,Congenital Dislocation of the hip (CDH)
                MiTalipes,Mild Talipes (club foot)
                MoTalipes,Moderate Talipes (club foot)
                 '''
        transformed= transform_values(values)
        if len(transformed)>0:
            query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
            query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
            if len(query1)>0 and column_exists("derived",table,to_update) :
                inject_sql(query1,"UPDATING CAUSE OF DEATH")
            if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) :
                inject_sql(query2,"UPDATING CAUSE OF DEATH IN JOINED ADMISSIONS")


def update_hive_result():
     facilities = ['SMCH','BPH','CPH','PGH'] 
     variable = "HIVtestResult.value"
     to_update = "HIVtestResult.label"
     table="admissions"
     where = "What%"
     for facility in facilities:
         if facility=='SMCH' or facility=='CPH' or facility=='BPH' or facility=='PGH':
            values=f'''R,Positive
                    NR,Negative
                    U,Unknown'''
            
            transformed= transform_values(values)
            if len(transformed):
                query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
                query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
             
                if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
                    inject_sql(query1,"UPDATE HIV TEST RESULT IN ADMISSIONS")
                if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
                    inject_sql(query2,"UPDATE HIV TEST RESULT IN IN JOINED ADMISSIONS")
              

def fix_broken_dates_query(table:str):
    
    if (table_exists('derived',table_name=table)):
        try:
            affected_dates = get_affected_date_columns(table)
            if affected_dates:
                rows = [affected_dates] if isinstance(affected_dates, dict) else affected_dates
                for row in rows:
                    label = get_lable_from_value(row[0])
                    value = row[0]
                
                    data_type = str(row[1])
                    query = ''
                    label_exists = column_exists('derived',table,label)
                   
                    if label_exists:
                        label_type = get_table_column_type(table,'derived',label)[0][0]
                        
                        if 'text' in label_type:
                            if('text' in data_type):
                                query= f'''UPDATE derived.{table} SET "{value}" =to_char(to_timestamp("{label}",'DD Mon, YYYY HH24:MI'),
                                'YYYY-MM-DD HH24:MI:00') WHERE  "{label}"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3,10}}.*' and ("{value}" is null
                                OR "{value}"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3,10}}.*');;'''
                            else:
                                if ('date' in data_type or 'timestamp' in data_type):
                                    query= f''' UPDATE derived.{table} SET "{value}" = to_timestamp("{label}"
                                    , 'DD Mon, YYYY HH24:MI:00') 
                                    WHERE "{label}"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3,10}}.*' and "{value}" is null;; '''
                                    
                            if (len(query)>0):            
                                inject_sql(query,f"UPDATING DATES FOR {table}")
                        

        except Exception as ex:
            logging.error("#### FAILED TO FIX YOUR DIRTY DATES #########")
            logging.error(formatError(ex))

def fix_broken_dates_combined():
    fix_broken_dates_query('admissions')
    fix_broken_dates_query('discharges')
    fix_broken_dates_query('joined_admissions_discharges')
    fix_broken_dates_query('vital_signs')
    fix_broken_dates_query('neolabs')
    fix_broken_dates_query('maternal_outcomes')
    fix_broken_dates_query('summary_maternal_outcomes')
    fix_broken_dates_query('maternal_completeness')
    fix_broken_dates_query('summary_maternal_completeness')
    fix_broken_dates_query('daily_review')
    fix_broken_dates_query('infections')
    fix_broken_dates_query('phc_admissions')
    fix_broken_dates_query('phc_discharges')



def get_affected_date_columns(table: str):
    query = f'''SELECT column_name,data_type FROM  information_schema.columns WHERE 
    table_schema = 'derived' 
    AND table_name = '{table}'
    AND (LOWER(column_name) LIKE '%date%' OR LOWER(column_name) LIKE '%day%')  AND LOWER(column_name) LIKE '%.value';;'''
    return inject_sql_with_return(query)

def get_lable_from_value(label:str):
    return label.replace('.value','.label')

def update_gender():
     facilities = ['SMCH','BPH','CPH','PGH'] 
     variable = "Gender.value"
     to_update = "Gender.label"
     table="admissions"
     where = "What%"
     for facility in facilities:
         if facility=='SMCH' or facility=='CPH' or facility=='BPH' or facility=='PGH':
            values=f'''M,Male
                    F,Female
                    NS,Not sure'''
            
            transformed= transform_values(values)
            if len(transformed)>0:
                query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
                query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
             
                if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
                    inject_sql(query1,"UPDATE GENDER IN ADMISSIONS")
                if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
                    inject_sql(query2,"UPDATE GENDER IN IN JOINED ADMISSIONS")

def deduplicate_combined():
    tables = ['admissions','discharges'
              ,'joined_admissions_discharges','vital_signs'
              ,'neolab','maternal_outcomes',
              'maternal_completeness',
              'daily_review','infections','phc_discharges'
              ,'old_new_admissions_view',
              'old_new_discharges_view'
              ,'old_new_matched_view'
              ,'baseline','maternal_completeness'
              ,'phc_admissions','phc_discharges'
              ,'clean_admissions','clean_discharges'
              ,'clean_joined_adm_discharges'
              ,'clean_maternal_outcomes'
              ,'clean_maternal_completeness',
              'clean_infections',
              'clean_daily_review','clean_phc_discharges','clean_phc_admissions'
              ,'clean_vital_signs','clean_neolab']
    
    for table in tables:
        if (table_exists('derived',table)):
            deduplicate_derived_tables(table)
    logging.info("#######DONE DEDUPLICATING DERIVED TABLES################")

def update_stools():
    variable = "StoolsInfant.value"
    to_update = "StoolsInfant.label"
    table="admissions"
    where = "Stools%"
    values=f'''Norm,Opening bowels normally
            BNO,Bowels not opening
            Diarr,Has diarrhoea and passing large water stools
            BlDi,Bloody Diarrhoea'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE STOOLS")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE STOOLS IN IN JOINED ADMISSIONS")

def update_admreason():
    variable = "AdmReason.value"
    to_update = "AdmReason.label"
    table="admissions"
    where = "Presenting%"
    values=f'''AD,Abdominal distension
    BBA,Born Before Arrival
    Convulsions,Convulsions
    DIB,Difficulty in breathing
    DU,Dumped baby
    Fev,Fever
    FD,Not sucking \\/ feeding difficulties
    G,Gastroschisis
    NE,Neonatal encephalopathy
    HIVX,HIV exposed
    J,Jaundice
    LowBirthWeight,Low Birth Weight
    Apg,Low Apgars
    Mec,Possible Meconium Aspiration
    NTD,Neural Tube Defect \\/ Spina Bifida
    Prem,Prematurity
    PremRDS,Prematurity with RDS
    SPn,Severe Pneumonia
    OM,Omphalocele
    Cong,Other congenital abnormality
    Mac,Macrosomia
    Safe,Safekeeping
    Risk,Risk factors for sepsis
    O,Other'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE AdmReason")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE AdmReason IN IN JOINED ADMISSIONS")

def update_puurine():
    variable = "PUInfant.value"
    to_update = "PUInfant.label"
    table="admissions"
    where = "Passing urine%"
    values=f'''Norm,Passing urine normally
            NoPU,Not passing urine'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE PASSING URINE")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE PASSING URINE IN IN JOINED ADMISSIONS")

def update_puurine_nb():
    variable = "PUNewborn.value"
    to_update = "PUNewborn.label"
    table="admissions"
    where = "Passing urine?%"
    values=f'''Yes,Has passed urine in 1st 24 hrs
            No,Has not passed urine
            Unk,Not sure
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE PASSING URINE")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE PASSING URINE IN IN JOINED ADMISSIONS")

def update_haart():
    variable = "HAART.value"
    to_update = "HAART.label"
    table="admissions"
    where = "Is the mother%"
    values=f'''Y,Yes
            N,No'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE HAART")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE HAART IN IN JOINED ADMISSIONS") 


def update_lengthhaart():
    variable = "LengthHAART.value"
    to_update = "LengthHAART.label"
    table="admissions"
    where = "Mother on HAART%"
    values=f'''1stTrim,1st Trimester or earlier
              2ndTrim,2nd Trimester 
              3rdTrim,3rd Trimester more than 1 month before delivery
              Late,Less than 1 month before delivery
              U,Unknown'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE LengthHAART")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE LengthHAART IN IN JOINED ADMISSIONS")  

def update_anmatsyphtreat():
    variable = "ANMatSyphTreat.value"
    to_update = "ANMatSyphTreat.label"
    table="admissions"
    where = "%treatment for syphilis%"
    values=f'''Y,Yes
                N,No'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE ANMatSyphTreat")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE ANMatSyphTreat IN IN JOINED ADMISSIONS")  

def update_patnsyph():
    variable = "PartnerTrSyph.value"
    to_update = "PartnerTrSyph.label"
    table="admissions"
    where = "%Was the partner treated?%"
    values=f'''Y,Yes
                N,No
                U,Unknown'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE PartnerTrSyph")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE PartnerTrSyph IN IN JOINED ADMISSIONS") 

def update_anster():
    variable = "ANSterCrse.value"
    to_update = "ANSterCrse.label"
    table="admissions"
    where = "%When were the steroids given?%"
    values=f'''FC14, Full course given in last 14 days
                PC14, Partial course given in last 14 days
                FCG14, Full course given more than 14 days ago
                U, Unknown'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE ANSterCrse")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE ANSterCrse IN IN JOINED ADMISSIONS") 


def update_ansteroids():
    variable = "ANSteroids.value"
    to_update = "ANSteroids.label"
    table="admissions"
    where = "%Were antenatal steroids given?%"
    values=f'''Y,Yes
                N,No
                U,Unknown'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE ANSteroids")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE ANSteroids IN IN JOINED ADMISSIONS") 

def update_transfusion():
    variable = "Transfusion.value"
    to_update = "Transfusion.label"
    table="discharges"
    where = "%Did the baby receive any blood products?%"
    values=f'''Y,Yes
                N,No'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE Transfusion")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE Transfusion IN IN JOINED ADMISSIONS") 

def update_transtype():
    variable = "TransType.value"
    to_update = "TransType.label"
    table="discharges"
    where = "%What kind of transfusion?%"
    values=f'''R,Packed Red Cells
                FFP,FFP
                P,Platelets
                E,Exchange transfusion
                M,More than one kind
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE TransType")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE TransType IN IN JOINED ADMISSIONS")

def update_specrev():
    variable = "SPECREV.value"
    to_update = "SPECREV.label"
    table="discharges"
    where = "%Was the baby%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE SPECREV")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE SPECREV IN IN JOINED ADMISSIONS") 

def update_matadmit():
    variable = "MatAdmit.value"
    to_update = "MatAdmit.label"
    table="discharges"
    where = "%Was%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE MatAdmit")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE MatAdmit IN IN JOINED ADMISSIONS") 

def update_matdisc():
    variable = "MatDischarge.value"
    to_update = "MatDischarge.label"
    table="discharges"
    where = "%If%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE MatDischarge")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE MatDischarge IN IN JOINED ADMISSIONS")  


def update_matdisc():
    variable = "MatDischarge.value"
    to_update = "MatDischarge.label"
    table="discharges"
    where = "%If%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE MatDischarge")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE MatDischarge IN IN JOINED ADMISSIONS")  

def update_troward():
    variable = "TROWard.value"
    to_update = "TROWard.label"
    table="discharges"
    where = "%Which%"
    values=f'''PostA,Postnatal A
                PostB,Postnatal B
                Paeds,Paediatric
                OTH,Other
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE TROWard")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE TROWard IN IN JOINED ADMISSIONS")  

def update_specrevtype():
    variable = "SPECREVTYP.value"
    to_update = "SPECREVTYP.label"
    table="discharges"
    where = "%Which%"
    values=f'''Neph,Nephrology
                Ortho,Orthopaedic Surgery
                Surg,Surgical
                Opth,Opthalmology
                MxFx,Maxillofacial Surgery
                Neuro,Neurology
                Endoc,Endocrinology
                Oth,Other
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE SPECREVTYP")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE SPECREVTYP IN IN JOINED ADMISSIONS") 


def update_ageestimate():
    variable = "AgeEstimate.value"
    to_update = "AgeEstimate.label"
    table="admissions"
    where = "%Age Category Estimate%"
    values=f'''FNB,Fresh Newborn \\(\\< 2 hours old\\)
                NB24,Newborn \\(2 - 23 hrs old\\)
                NB48,Newborn \\(1 day - 1 day 23 hrs old\\)
                INF72,Infant \\(2 days - 2 days 23 hrs old\\)
                INF,Infant \\(\\> 3 days old\\)'''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE AgeEstimate")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE AgeEstimate IN IN JOINED ADMISSIONS")  

def update_birthfac():
    variable = "BirthFacility.value"
    to_update = "BirthFacility.label"
    table="admissions"
    where = "%Name of Facility%"
    values=f'''PA,Parirenyatwa
                BU,Budiriro
                EP,Epworth
                GV,Glen View
                HC,Hatcliffe
                HF,Highfield
                KA,Kambuzuma
                KU,Kuwadzana
                MA,Mabvuku
                MB,Mbare
                RS,Rutsanana
                RU,Rujuko
                WP,Warren Park
                O,Other
                '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE BirthFacility")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE BirthFacility IN IN JOINED ADMISSIONS")  



def update_reason():
    variable = "Reason.value"
    to_update = "Reason.label"
    table="admissions"
    where = "Reason for CS"
    values=f'''FD,Foetal Distress
              Pres,Malpresentation \\(e.g. breech\\) 
              Scar,Previous scar\\(s\\)
              E,Eclampsia
              PE,Pre-Eclampsia
              Fi,Fibroids
              Fund,Big Fundus
              Mult,Multiple gestation
              Pr,Prolonged labour
              Post,Post Term
              CPD,Cephalopelvic Disproportion
              APH,Antepartum Haemorrhage
              O,Other
              '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE CS Reasons")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE CS Reasons IN IN JOINED ADMISSIONS")     

def update_readmission():
    variable = "Readmission.value"
    to_update = "Readmission.label"
    table="admissions"
    where = "%Readmission%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE ReAdmission")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE ReAdmission IN IN JOINED ADMISSIONS") 
            
def update_ANVDRL():
    variable = "ANVDRL.value"
    to_update = "ANVDRL.label"
    table="admissions"
    where = "%Syphilis test?%"
    values=f'''Y,Yes
                N,No
                U,Not sure
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE Syphylis")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE Syphylis IN IN JOINED ADMISSIONS") 

def update_IRON():
    variable = "Iron.value"
    to_update = "Iron.label"
    table="admissions"
    where = "%received?%"
    values=f'''Y,Yes
                N,No
                U,Unkown
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE IRON")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE IRON IN IN JOINED ADMISSIONS")

def update_TTV():
    variable = "TTV.value"
    to_update = "TTV.label"
    table="admissions"
    where = "%received?%"
    values=f'''Y,Yes
                N,No
                U,Unkown
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE TTV")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE TTV IN IN JOINED ADMISSIONS") 

def update_CryBirth():
    variable = "CryBirth.value"
    to_update = "CryBirth.label"
    table="admissions"
    where = "%did?%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE CryBirth")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE CryBirth IN IN JOINED ADMISSIONS")

def update_VitK():
    variable = "VitK.value"
    to_update = "VitK.label"
    table="admissions"
    where = "%Did%"
    values=f'''Y,Yes
                N,No,
                NS,Not Sure
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE VitK")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE VitK IN IN JOINED ADMISSIONS")

def update_TEO():
    variable = "TEO.value"
    to_update = "TEO.label"
    table="admissions"
    where = "%given?%"
    values=f'''Y,Yes
                N,No,
                NS,Not Sure
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE TEO")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE TEO IN IN JOINED ADMISSIONS")

def update_DateVDRLSameHIV():
    variable = "DateVDRLSameHIV.value"
    to_update = "DateVDRLSameHIV.label"
    table="admissions"
    where = "%Date%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE DateVDRLSameHIV")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE DateVDRLSameHIV IN IN JOINED ADMISSIONS")


def update_AnvdrlResult():
    variable = "ANVDRLResult.value"
    to_update = "ANVDRLResult.label"
    table="admissions"
    where = "%Result%"
    values=f'''N,Negative
                P,Positive
                U,Unkown
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
          
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE AnvdrlResult")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE AnvdrlResult IN IN JOINED ADMISSIONS")
            
def update_BSUnit():
    variable = "BSUnit.value"
    to_update = "BSUnit.label"
    table="admissions"
    where = "%Blood%"
    values=f'''Mol,mmol\\/L
                Mg,mg\\/dL
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE BSUnit")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE BSUnit IN IN JOINED ADMISSIONS")

def update_BsMonyn():
    variable = "BSmonYN.value"
    to_update = "BSmonYN.label"
    table="admissions"
    where = "%measure%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE BSmonYN")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE BSmonYN IN IN JOINED ADMISSIONS")

def update_VRLKnown():
    variable = "VLKnown.value"
    to_update = "VLKnown.label"
    table="admissions"
    where = "%Do%"
    values=f'''Y,Yes
                N,No
                NA,Not applicable
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE VLKnown")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE VLKnown IN IN JOINED ADMISSIONS")

def update_ROM():
    variable = "ROM.value"
    to_update = "ROM.label"
    table="admissions"
    where = "%rupture?%"
    values=f'''Y,Yes
                N,No
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE ROM")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE ROM IN IN JOINED ADMISSIONS") 

def update_ROMLENGTH():
    variable = "ROMlength.value"
    to_update = "ROMlength.label"
    table="admissions"
    where = "%How long%"
    values=f'''NOPROM,Less than 18 hours
                PROM,Greater than 18 hours
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE ROMlength")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE ROMlength IN IN JOINED ADMISSIONS") 

def update_vomiting():
    variable = "Vomiting.value"
    to_update = "Vomiting.label"
    table="admissions"
    where = "%Vomiting?%"
    values=f'''Poss,Small milky possets after feeds \\(normal\\)
                Yes,Vomiting all feeds
                YesBl,Vomiting with blood
                YesGr,Vomiting bright green
                No,NONE
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE Vomiting")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE Vomiting IN IN JOINED ADMISSIONS") 

def update_passedmec():
    variable = "PassedMec.value"
    to_update = "PassedMec.label"
    table="admissions"
    where = "%Meconium?%"
    values=f'''MecLiq,Meconium present at delivery
                Mec24,Passed meconium in 1st 24 hrs
                NoMec,No meconium in 1st 24 hrs
            '''
            
    transformed= transform_values(values)
    if len(transformed)>0:
        query1 = generate_update_query('',variable,to_update,transformed,table,where)
        query2 = generate_update_query('',variable,to_update,transformed,"joined_admissions_discharges",where)
             
        if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
            inject_sql(query1,"UPDATE Meconium")
        if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
            inject_sql(query2,"UPDATE Meconium IN IN JOINED ADMISSIONS")

def deduplicate_derived_tables(table: str):
    query = f'''DO $$
        DECLARE
            rows_deleted INTEGER := 1;
            total_deleted INTEGER := 0;
            batch_size INTEGER := 10000;
        BEGIN
            WHILE rows_deleted > 0 LOOP
                WITH ranked_duplicates AS (
                    SELECT ctid,
                        ROW_NUMBER() OVER (
                            PARTITION BY LEFT(unique_key,10), uid
                            ORDER BY ctid
                        ) AS rn
                    FROM derived."{table}"
                    WHERE unique_key IS NOT NULL
                ),
                to_delete AS (
                    SELECT ctid
                    FROM ranked_duplicates
                    WHERE rn > 1
                    LIMIT batch_size
                )
                DELETE FROM derived."{table}" 
                WHERE ctid IN (SELECT ctid FROM to_delete);

                GET DIAGNOSTICS rows_deleted = ROW_COUNT;
                total_deleted := total_deleted + rows_deleted;
                
                RAISE NOTICE 'Deleted % rows in this batch, % total', rows_deleted, total_deleted;
                
                -- Small delay to reduce lock contention
                PERFORM pg_sleep(0.1);
            END LOOP;
        END $$;;'''
    inject_sql_procedure(query,f"DEDUPLICATE DERIVED {table}")

