from conf.common.sql_functions import inject_sql
from conf.common.sql_functions import column_exists

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
            if len(transformed) and facility!='PHC':
                query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
                query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
                query3=''
                if facility=='PHC':
                    query3 = generate_update_query(facility,variable,to_update,transformed,"phc_admissions",where)

                if len(query1)>0 and column_exists("derived",table,to_update) and column_exists("derived",table,variable):
                    inject_sql(query1,"UPDATE MODE DELIVERY FROM")
                if len(query2)>0 and column_exists("derived","joined_admissions_discharges",to_update) and column_exists("derived","joined_admissions_discharges",variable):
                    inject_sql(query2,"UPDATE DELIVERY IN JOINED ADMISSIONS")
                if len(query3)>0 and column_exists("derived","phc_admissions",to_update):
                    inject_sql(query3,"UPDATE PHC ADMISSIONS")


def update_signature():
     facilities = ['KDH','BHC','KCH','PHC'] 
     variable = "HCWSig.value"
     to_update = "HCWSig.label"
     table="admissions"
     where = "Electronic%"
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
        if len(transformed)>0 and facility!='PHC':
            query1 = generate_update_query(facility,variable,to_update,transformed,table,where)
            query2 = generate_update_query(facility,variable,to_update,transformed,"joined_admissions_discharges",where)
            query3=''
            if facility=='PHC':
                query3 = generate_update_query(facility,variable,to_update,transformed,table,where)

            if len(query1)>0 and column_exists("derived",table,to_update) :
                inject_sql(query1,"UPDATE MODE DELIVERY FROM")
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
