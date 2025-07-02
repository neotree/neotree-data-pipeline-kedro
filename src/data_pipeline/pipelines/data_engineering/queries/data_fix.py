import logging
from conf.common.sql_functions import inject_sql,column_exists,inject_sql_with_return,get_table_column_type
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.common.format_error import formatError

def generate_update_query(facility, variable, to_update, values,table,where):
    additional_where = f''' and facility='{facility}' '''
    if(facility=='PHC'):
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
        SET "{to_update}" = CASE
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
     for facility in facilities:
         if facility=='KCH' or facility=='KDH' or facility=='BHC' or facility=='PHC':
            values=f'''1,Spontaneous Vaginal Delivery (SVD)
                       2,Vacuum extraction
                       3,Forceps extraction
                       4,Elective Ceasarian Section (ELCS)
                       5,Emergency Ceasarian Section (EMCS)
                       6,Breech extraction (vaginal)
                       7,Induced Vaginal Delivery'''
            
            transformed= transform_values(values)
            if len(transformed):
                query1=''
                query2=''
                query3=''
                if facility=='PHC':
                    query3 = generate_update_query(facility,variable,to_update,transformed,"phc_admissions",where)
                else:
                    query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
                    query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where) 

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
                                'YYYY-MM-DD HH24:MI') WHERE  "{label}" ~ '^[0-9]{1,2} [A-Za-z]{3,10}.*' and ("{value}" is null
                                OR "{value}" ~ '^[0-9]{1,2} [A-Za-z]{3,10}.*');;'''
                            else:
                                if ('date' in data_type or 'timestamp' in data_type):
                                    query= f''' UPDATE derived.{table} SET "{value}" = to_timestamp("{label}"
                                    , 'DD Mon, YYYY HH24:MI') 
                                    WHERE "{label}" ~ '^[0-9]{1,2} [A-Za-z]{3,10}.*' and "{value}" is null;; '''

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