## DISCHARGES DATA
import pandas as pd 
from conf.common.sql_functions import column_exists,inject_sql

def format_column_as_numeric(df,fields):
    for field in fields:
        fld = f'{field}.value'
        if fld in df.columns:
            df[fld] = pd.to_numeric(df[fld], errors='coerce') 
    return df

def format_column_as_datetime(df,fields):
    for field in fields:
        fld = f'{field}.value'
        if fld in df.columns:
            df[fld] = pd.to_datetime(df[fld], errors='coerce') 

    return df

def convert_false_numbers_to_text(df: pd.DataFrame,schema,table) -> pd.DataFrame:
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            if df[column].apply(lambda x: not isinstance(x, (int, float))).any():
                df[column] = df[column].astype(str)
                if column_exists(schema,table,column):
                    sql_query = '''ALTER TABLE {0}.{1} ALTER COLUMN {2} TYPE TEXT;;'''.format(schema,table,column)
                    inject_sql(sql_query,'''...UPDATING COLUMN {0} of TABLE {1}'''.format(column,table))
    return df
            
@DeprecationWarning
def fix_neotree_oucome(value):
        if value== 'DC':
            return 'Discharged'
        if value== 'NND':
            return 'Died'
        if value== 'ABS':
            return 'Absconded'
        if value== 'TRH':
            return 'Transferred to other Hospital'
        if value== 'TRO':
            return 'Transferred to other ward'
        if value== 'DAMA':
            return 'Discharged against medical advice'
        if value== 'UK':
            return 'Unknown'
        if value =='BID':
            return 'Brought in dead'
        if value== 'null':
            return None
@DeprecationWarning
def fix_discharge_sex(value):
        if value== 'F':
            return 'Female'
        if value== 'M':
            return 'Male'
        if value== 'U':
            return 'Unsure'

        if value== 'null':
            return None
@DeprecationWarning
def fix_maternal_outcome(value):
    
        if value== 'A':
            return 'Alive'
        if value== 'D':
            return 'Deceased' 
        return None
@DeprecationWarning
def fix_place_of_birth(value):
    
        if value== 'SMCH':
            return 'Sally Mugabe Central Hospital'
        if value== 'OtH':
            return 'Other clinic in Harare'
        if value== 'OtR':
            return 'Other clinic outside Harare'
        if value== 'H':
            return 'Home' 
        return None
@DeprecationWarning
def fix_mode_of_delivery(value):
        if value== 'SVD':
            return 'Spontaneous vaginal delivery'
        if value== 'IVD':
            return 'Induced vaginal delivery'
        if value== 'ECS':
            return 'Emergency Caesarean section'
        if value== 'ElCS':
            return 'Elective Caesarean section'
        if value== 'Vent':
            return 'Ventouse'
        if value =='1':
            return 'Spontaneous Vaginal Delivery (SVD)'
        if value =='2':
            return 'Vacuum extraction'
        if value=='3':
            return 'Forceps extraction'
        if value =='4':
            return 'Elective Ceasarian Section (ELCS)'
        if value =='5':
            return 'Emergency Ceasarian Section (EMCS)'
        if value =='6':
            return 'Breech extraction (vaginal'
        if value=='7':
            return 'Unknown'
        if value =='null' or value is None:
           return None
@DeprecationWarning
def fix_transfare_wards(value):
    
        if value== 'PostA':
            return 'Postnatal A'
        if value== 'PostB':
            return 'Postnatal B'
        if value== 'Paeds':
            return 'Paediatric'
        if value== 'OTH':
            return 'Other'
        return None
@DeprecationWarning
def fix_transfare_wards(value):
    
        if value== 'PostA':
            return 'Postnatal A'
        if value== 'PostB':
            return 'Postnatal B'
        if value== 'Paeds':
            return 'Paediatric'
        if value== 'OTH':
            return 'Other'
        return None
@DeprecationWarning
def fix_bcresults(value):
    
        if value== 'NO':
            return 'No Growth'
        if value== 'CO':
            return 'Likely Contaminant Organism'
        if value== 'BS':
            return 'Coagulase Negative Staphylococcus'
        if value== 'CONS':
            return 'Coagulase Negative Staphylococcus'
        if value== 'EC':
            return 'E.Coli'
        if value== 'GBS':
            return 'Group B Streptococcus'
        if value== 'GDS':
            return 'Group D Streptococcus'
        if value== 'KP':
            return 'Klebsiella Pneumoniae'
        if value== 'PA':
            return 'Pseudomonas Aeruginosa'
        if value== 'SA':
            return 'Staphylococcus Aureus'
        if value== 'LFC':
            return 'Lactose Fermenting Coliform'
        if value== 'NLFC':
            return 'Non Lactose Fermenting Coliform'
        if value== 'OGN':
            return 'Other Gram Negative'
        if value== 'OGP':
            return 'Other Gram Positive'

        if value== 'OGP':
            return 'Other Gram Positive'
        
        if value== 'OTH':
            return 'Other'
        return None
@DeprecationWarning
def fix_hivpcr(value):
    
        if value== 'P':
            return 'Positive'

        if value== 'N':
            return 'Negative'
        
        if value== 'I':
            return 'Indeterminate'
        return None
@DeprecationWarning
def fix_mat_hivpr(value):
    
        if value== 'P':
            return 'Code 1'

        if value== 'N':
            return 'Code 0'
        
        if value== 'I':
            return 'Indeterminate'
        return None        
@DeprecationWarning
def fix_matrprr(value):
    
        if value== 'P':
            return 'Positive'

        if value== 'N':
            return 'Negative'
        
        if value== 'I':
            return 'Indeterminate'
        
        if value== 'U':
            return 'Unknown'
        return None
@DeprecationWarning
def fix_matpart(value):
    
        if value== 'Y':
            return 'Yes'

        if value== 'N':
            return 'No'
        
        if value== 'U':
            return 'Unknown'

        return None
@DeprecationWarning
def fix_lpgluc(value):
    
        if value== 'Y':
            return 'Yes'

        if value== 'N':
            return 'No'
        
        if value== 'ND':
            return 'Not Done'
        return None

@DeprecationWarning
def fix_thomp_tone(value):
    
        if value== '0':
            return '0 = normal'
        if value== '1':
            return '1 = hypertonia' 
        if value== '2':
            return '2 = hypotonia'
        if value== '3':
            return '3 = flaccid'
        return None
@DeprecationWarning
def fix_thomp_alert(value):
    
        if value== '0':
            return '0 = Alert'
        if value== '1':
            return '1 = Hyperalert/stare' 
        if value== '2':
            return '2 = Lethargic'
        if value== '3':
            return '3 = Comatose'
        return None
@DeprecationWarning
def fix_thomp_seize(value):
    
        if value== '0':
            return '0 = none'
        if value== '1':
            return '1 = 2 or less per day' 
        if value== '2':
            return '2 = More than 2 per day'
        return None
@DeprecationWarning
def fix_thomp_refl(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Fisting, cycling' 
        if value== '2':
            return '2 = Strong distal flexion'
        if value== '3':
            return '3 = Decerebrate'
        return None
@DeprecationWarning
def fix_thomp_resp(value):
    
        if value== '0':
            return '0 = normal'
        if value== '1':
            return '1 = Hyperventilation' 
        if value== '2':
            return '2 = Brief apnoea'
        if value== '3':
            return '3 = Apnoea needing ventilatory support'
        return None
@DeprecationWarning
def fix_thomp_moro(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Partial' 
        if value== '2':
            return '2 = Absent'
        return None
@DeprecationWarning
def fix_thomp_grasp(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Poor' 
        if value== '2':
            return '2 = Absent'
        return None
@DeprecationWarning
def fix_thomp_font(value):
    
        if value== '0':
            return '0 = normal'
        if value== '1':
            return '1 = full, not tense' 
        if value== '2':
            return '2 = tense'
        return None
@DeprecationWarning
def fix_thomp_feeds(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Poor' 
        if value== '2':
            return '2 = Absent or with bites'
        return None
@DeprecationWarning
def fix_thomp_feed(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Poor' 
        if value== '2':
            return '2 = Absent or with bites'
        return None
@DeprecationWarning
def fix_thomp_resp(value):
    
        if value== '0':
            return '0 = normal'
        if value== '1':
            return '1 = Hyperventilation' 
        if value== '2':
            return '2 = Brief apnoea'
        if value== '3':
            return '3 = Apnoea needing ventilatory support'
        return None
@DeprecationWarning
def fix_thomp_front(value):
    
        if value== '0':
            return '0 = normal'
        if value== '1':
            return '1 = full, not tense' 
        if value== '2':
            return '2 = tense'
        return None
@DeprecationWarning
def fix_transfusion(value):
    
        if value== 'Y':
            return 'Yes'
        if value== 'N':
            return 'No' 
        return None
@DeprecationWarning
def fix_high_risk(value):
    
        if value== 'Y':
            return 'Yes'
        if value== 'N':
            return 'No' 
        if value== 'NS':
            return 'Not Sure' 
        return None
@DeprecationWarning
def fix_trans_type(value):
    
        if value== 'R':
            return 'Packed Red Cells'
        if value== 'FFP':
            return 'FFP' 
        if value== 'P':
            return 'Platelets' 
        if value== 'E':
            return 'Exchange transfusion' 
        if value== 'M':
            return 'More than one kind' 
        return None
@DeprecationWarning
def fix_spec_type(value):
    
        if value== 'Neph':
            return 'Nephrology'
        if value== 'Ortho':
            return 'Orthopaedic Surgery' 
        if value== 'Surg':
            return 'Surgical' 
        if value== 'Opth':
            return 'Opthalmology' 
        if value== 'MxFx':
            return 'Maxillofacial Surgery' 
        if value== 'Neuro':
            return 'Neurology' 
        if value== 'Endoc':
            return 'Endocrinology' 
        if value== 'Oth':
            return 'Other' 
        return None
@DeprecationWarning
def fix_rev_clinic_type(value):
    
        if value== 'KMC':
            return 'Kangaroo clinic (Wednesday and Thursday)'
        if value== 'NNC':
            return 'Neonatal clinic (Friday)' 
        if value== 'LOC':
            return 'Local clinic'
        if value== 'SUR':
            return 'Surgical clinic'  
        if value== 'ORTH':
            return 'Orthopaedic clinic' 
        if value== 'OTH':
            return 'Other clinic'   
        return None
@DeprecationWarning
def fix_cadre_disc(value):
    
        if value== 'S':
            return 'SRMO'
        if value== 'SH':
            return 'SHO' 
        if value== 'R':
            return 'Registrar'
        if value== 'SR':
            return 'Senior Registrar'  
        if value== 'N':
            return 'Nurse' 
        if value== 'MW':
            return 'Midwife' 
        if value== 'O':
            return 'Other'     
        return None
@DeprecationWarning
def fix_good_prog(value):
    
        if value== 'OB':
            return 'Admitted for observation and progressed well'
        if value== 'SE':
            return 'Risk factors for sepsis and progressed well' 
        if value== 'ME':
            return 'Respiratory distress resolved with no treatment' 
        if value== 'REO':
            return 'Respiratory distress resolved with O2' 
        if value== 'FE':
            return 'Feeding difficulties resolved with feeding support' 
        if value== 'JA':
            return 'Jaundice resolved with phototherapy' 
        if value== 'JAS':
            return 'Suspected jaundice with no phototherapy needed' 
        if value== 'MA':
            return 'Admitted with macrosomia Blood sugars normal so discharged' 
        if value== 'CS':
            return 'Confirmed sepsis treated successfully with antibiotics'
        if value== 'SS':
            return 'Suspected sepsis treated successfully with antibiotics'  
        if value== 'DI':
            return 'Died shortly after admission'  
        if value== 'DE':
            return 'Deteriorated and died despite interventions'  
        if value== 'DOA':
            return 'Dead on arrival' 
        if value== 'OTH':
            return 'Additional Information not covered above' 
        return None
@DeprecationWarning
def fix_admission_reason(value):
    
        if value== 'An':
            return 'Anaemia'
        if value== 'BBA':
            return 'Born before arrival'
        if value== 'BI':
            return 'Birth trauma'
        if value== 'Cong':
            return 'Congenital Abnormality'
        if value== 'CHD':
            return 'Consider Congenital Heart Disease'
        if value== 'Conv':
            return 'Convulsions'
        if value== 'Dhyd':
            return 'Dehydration'
        if value== 'DF':
            return 'Difficulty feeding'
        if value== 'DUM':
            return 'Abandoned baby'
        if value== 'HIVLR':
            return 'HIV low risk'
        if value== 'HIVHR':
            return 'HIV high risk'
        if value== 'HIVU':
            return 'HIV unknown'
        if value== 'HypogAs':
            return 'Hypoglycaemia (NOT symptomatic)'
        if value== 'HypogSy':
            return 'Hypoglycaemia (Symptomatic)'
        if value== 'RiHypog':
            return 'Risk of hypoglycaemia'
        if value== 'BA':
            return 'Hypoxic Ischaemic Encephalopathy'
        if value== 'GSch':
            return 'Gastroschisis'
        if value== 'MJ':
            return 'Physiological Jaundice'
        if value== 'LBW':
            return 'Low Birth Weight (1500-2499g)'
        if value== 'VLBW':
            return 'Very Low Birth Weight (1000-1499g)'

        if value== 'ExLBW':
            return 'ExtremelyLow Birth Weight (<1000g)'

        if value== 'MA':
            return 'Possible Meconium Aspiration'

        if value== 'MecEx':
            return 'Meconium exposure (asymptomatic baby)'

        if value== 'MiHypo':
            return 'Mild Hypothermia'
        
        if value== 'ModHypo':
            return 'Moderate Hypothermia'
        
        if value== 'SHypo':
            return 'Severe Hypothermia'
        
        if value== 'Hyperth':
            return 'Hyperthermia'
        
        if value== 'SEPS':
            return 'Neonatal Sepsis'

        if value== 'Risk':
            return 'Risk factors for sepsis (asymptomatic baby)'
        
        if value== 'NB':
            return 'Normal baby'
        
        if value== 'PN':
            return 'Pneumonia / Bronchiolitis'
        
        if value== 'Prem':
            return 'Premature (32-36 weeks)'
        
        if value== 'VPrem':
            return 'Very Premature (28-31 weeks)'
        
        if value== 'ExPrem':
            return 'Extremely Premature (<28 weeks)'
        
        if value== 'PremRD':
            return 'Prematurity with RD'

        if value== 'Safe':
            return 'Safekeeping'
        
        if value== 'TTN':
            return 'Transient Tachypnoea of Newborn (TTN)'
        
        if value== 'OTH':
            return 'Other'
        
        if value== 'HBW':
            return 'Macrosomia (>4000g)'
        
        if value== 'TermRD':
            return 'Term with RD'
        
        if value== 'DJ':
            return 'Pathological Jaundice'

        if value== 'sHIE':
            return 'Suspected Hypoxic Ischaemic Encephalopathy'
        
        if value== 'PJaundice':
            return 'Prolonged Jaundice'
        
        if value== 'CleftLip':
            return 'Cleft lip'
        
        if value== 'CleftRD':
            return 'Cleft lip and/or palate with RD'
        
        if value== 'CleftLipPalate':
            return 'Cleft lip and/or palate'
        
        if value== 'Omph':
            return 'Omphalocele'
        
        if value== 'Myelo':
            return 'Myelomeningocele'
        
        if value== 'CDH':
            return 'Congenital Dislocation of the hip (CDH)'

        if value== 'MiTalipes':
            return 'Mild Talipes (club foot)'
        
        if value== 'MoTalipes':
            return 'Moderate Talipes (club foot)'
        return None

##FIX MATERNAL DATA
@DeprecationWarning
def fix_maternal_mode_of_delivery(value):
    if value=='1':
        return 'Spontaneous Vaginal Delivery'
    if value =='2':
        return 'Vacuum extraction'
    if value == '3':
        return 'Forceps extraction'
    if value == '4':
        return 'Elective Caesarian Section (ELCS)'
    if value == '5':
        return 'Emergency Caesarian Section (EMCS)'
    if value == '6':
        return 'Breech extraction (vaginal)'
    if value=='7':
        return 'Unknown'
    return None
@DeprecationWarning
def fix_maternal_neonatal_outcome(value):
    if value == 'LB':
        return 'Live Birth'
    if value == 'ENND':
        return 'Early Neonatal Death'
    if value == 'STBM':
        return 'Stillbirth Mascerated'
    if value == 'STBF':
        return 'Stillbirth Fresh'
    return None
@DeprecationWarning
def fix_maternal_mother_outcome(value):
    if value == 'D':
        return 'Died'
    if value == 'S':
        return 'Survived'
    return None

## FIX ADMISSIONS
@DeprecationWarning
def fix_adm_yes_no(value):
    if value=='Y':
        return 'Yes'
    if value == 'N':
        return 'No'
    return None;
@DeprecationWarning
def fix_adm_yes_no_ns(value):
    if value=='Y':
        return 'Yes'
    if value == 'N':
        return 'No'
    if value == 'NS':
        return 'Not Sure'
    return None;
@DeprecationWarning
def fix_adm_rom_length(value):
    if value=='NOPROM':
        return '< 18 hours'
    if value == 'PROM':
        return '> 18 hours'
    return None;
@DeprecationWarning
def fix_adm_presentation(value):
    if value=='Vertex':
        return 'Vertex'
    if value == 'Brow':
        return 'Brow'
    if value == 'Breech':
        return 'Breech'
    if value == 'Face':
        return 'Face'
    if value == 'Unk':
        return 'Unknown'
    return None;
@DeprecationWarning
def fix_adm_mode_delivery(value):
    if value=='1':
        return 'Spontaneous Vaginal Delivery'
    if value == '2':
        return 'Vacuum extraction'
    if value == '3':
        return 'Forceps extraction'
    if value == '4':
        return 'Elective Ceasarian Section (ELCS)'
    if value == '5':
        return 'Emergency Ceasarian Section (EMCS)'
    if value == '6':
        return 'Breech extraction (vaginal)'
    if value == '7':
        return 'Induced Vaginal Delivery'
    return None;
@DeprecationWarning
def fix_adm_balad_score(value):
    if value=='0':
        return '0'
    if value=='1':
        return '1'
    if value == '2':
        return '2'
    if value == '3':
        return '3'
    if value == '4':
        return '4'
    if value == '5':
        return '5'
    if value == '-1':
        return '-1'
    return None;
@DeprecationWarning
def fix_adm_cadre(value):
    if value=='GMO':
        return 'Government Medical Officer'
    if value=='HMO':
        return 'Hospital Medical Officer'
    if value == 'SHO':
        return 'Senior House Officer'
    if value == 'SRMO':
        return 'Senior Resident Medical Officer'
    if value == 'HCA':
        return 'Health Care Assistant'
    if value == 'MedSt':
        return 'Medical Student'
    if value == 'NS':
        return 'Nursing Student'
    if value == 'NMT':
        return 'Nurse Midwife Technician '
    if value == 'NO':
        return 'Nursing officer'
    if value == 'N':
        return 'Nurse'
    if value == 'COSt':
        return 'Clinical Officer Student'
    if value == 'COIn':
        return 'Clinical Officer Intern'
    if value == 'CO':
        return 'Clinical Officer'
    if value == 'PaeReg':
        return 'Paediatric Registrar'
    if value == 'NeoReg':
        return 'Neonatal Registrar'
    if value == 'MedReg':
        return 'Medical Registrar'
    if value == 'PaeCon':
        return 'Paediatric Consultant'
    if value == 'NeoCon':
        return 'Paediatric Consultant'
    return None;
@DeprecationWarning
def fix_adm_firm(value):
    if value=='GC':
        return 'Dr Gwen Chimhini'
    if value=='SC':
        return 'Dr Simba Chimhuya'
    if value == 'LM':
        return 'Dr Lethile Madzudzo'
    if value == 'MM':
        return 'Dr Marcia Mangiza'
    if value == 'AS':
        return 'Dr Alex Stevenson'
    return None
@DeprecationWarning
def fix_adm_review_cadre(value):
    if value=='SHO':
        return 'SHO'
    if value=='R':
        return 'Registrar'
    if value == 'SR':
        return 'Senior Registrar'
    if value == 'C':
        return 'Consultant'
    return None

@DeprecationWarning
def fix_adm_thomp_tone(value):
    if value=='0':
        return '0 = normal'
    if value == '1':
        return '1 = hypertonia'
    if value == '2':
        return '2 = hypotonia'
    if value == '3':
        return '3 = flaccid'
    return None
@DeprecationWarning
def fix_adm_thomp_refl(value):
    if value=='0':
        return '0 = Normal'
    if value == '1':
        return '1 = Fisting, cycling'
    if value == '2':
        return '2 = Strong distal flexion'
    if value == '3':
        return '3 = Decerebrate'
    return None
@DeprecationWarning
def fix_adm_thomp_alert(value):
    if value=='0':
        return '0 = Alert'
    if value == '1':
        return '1 = Hyperalert/stare'
    if value == '2':
        return '2 = Lethargic'
    if value == '3':
        return '3 = Comatose'
    return None
@DeprecationWarning
def fix_adm_thomp_seiz(value):
    if value=='0':
        return '0 = none'
    if value == '1':
        return '1 = 2 or less per day'
    if value == '2':
        return '2 = More than 2 per day'
    return None
@DeprecationWarning
def fix_adm_thomp_moro(value):
    if value=='0':
        return '0 = Normal'
    if value == '1':
        return '1 = Partial'
    if value == '2':
        return '2 = Absent'
    return None
@DeprecationWarning
def fix_adm_thomp_grasp(value):
    if value=='0':
        return '0 = Normal'
    if value == '1':
        return '1 = Poor'
    if value == '2':
        return '2 = Absent'
    return None
@DeprecationWarning   
def fix_adm_reason_for_cs(value):
    if value=='FD':
        return 'Foetal Distress'
    if value == 'Pres':
        return 'Malpresentation (e.g. breech)'
    if value == 'Scar':
        return 'Previous scar(s)'
    if value == 'E':
        return 'Eclampsia'
    if value == 'Fi':
        return 'Fibroids'
    if value == 'Fund':
        return 'Big Fundus'
    if value == 'Mult':
        return 'Multiple gestation'
    if value == 'Pr':
        return 'Prolonged labour'
    if value == 'Post':
        return 'Post Term'
    if value == 'CPD':
        return 'Cephalopelvic Disproportion'
    if value == 'O':
        return 'Other'
    return None;
@DeprecationWarning
def fix_adm_yes_no_unknown(value):
    if value=='Y':
        return 'Yes'
    if value == 'N':
        return 'No'
    if value == 'U':
        return 'Unknown'
    return None;
@DeprecationWarning
def fix_adm_place_of_birth(value):
    if value=='H':
        return 'Home'
    if value == 'T':
        return 'In transport to clinic'
    if value == 'R':
        return 'By Road'
    if value == 'To':
        return 'Toilet'
    if value == 'O':
        return 'Other'
    return None;
@DeprecationWarning
def fix_adm_cpr_out(value):
    if value=='Six':
        return 'HR Remained below 60 so CPR stopped'
    if value == 'Hun':
        return 'HR Remained below 100 so CPR stopped'
    if value == 'Suc':
        return 'HR came above 100 and baby improved CPR stopped'
    if value == 'Resp':
        return 'HR came above 100 gasping respirations only BVM cont'
    return None;
@DeprecationWarning
def fix_adm_who_delivered(value):
    if value=='S':
        return 'Self'
    if value == 'F':
        return 'Father'
    if value == 'Fa':
        return 'Family member'
    if value == 'Fr':
        return 'Friend'
    if value == 'N':
        return 'Nurse in ambulance'
    return None;
@DeprecationWarning
def fix_adm_yes_no_na(value):
    if value=='Y':
        return 'Yes'
    if value == 'N':
        return 'No'
    if value == 'NA':
        return 'Not applicable'
    return None;
@DeprecationWarning
def fix_hiv_result(value):
    if value=='P':
        return 'Positive'
    if value=='R':
        return 'Positive'
    if value == 'N':
        return 'Negative'
    if value == 'NR':
        return 'Negative'
    if value == 'U':
        return 'Unknown'
    return None;
@DeprecationWarning
def fix_anvdrl_result(value):
    if value=='R':
        return 'Positive'
    if value == 'NR':
        return 'Negative'
    if value == 'U':
        return 'Unknown'
    return None;
@DeprecationWarning
def fix_hiv_report(value):
    if value=='Self':
        return 'Self-reported from mother'
    if value == 'Doc':
        return 'From her documentation'
    return None;
@DeprecationWarning
def fix_age_estimate(value):
    if value=='FNB':
        return 'Fresh Newborn (< 2 hours old)'
    if value=='NB24':
        return 'Newborn (2 - 23 hrs old)'
    if value =='NB48':
       return 'Newborn (1 day - 1 day 23 hrs old)'
    if value== 'INF72':
        return 'Infant (2 days - 2 days 23 hrs old)'
    if value == 'INF':
        return 'Infant (> 3 days old)'
    return None
@DeprecationWarning
def fix_adm_type_birth(value):
    if value=='S':
        return 'Single'
    if value == 'Tw1':
        return '1st Twin'
    if value == 'Tw2':
        return '2nd Twin'
    if value == 'Tr1':
        return '1st Triplet'
    if value == 'Tr2':
        return '2nd Triplet'
    if value == 'Tr3':
        return '3rd Triplet'
    return None
@DeprecationWarning
def fix_adm_admission_reason(value):
    if value =='AD':
        return 'Abdominal distension'
    if value == 'BBA':
       return 'Born Before Arrival'
    if value == 'Convulsions':
        return 'Convulsions'
    if value == 'DIB':
        return 'Difficulty in breathing'
    if value == 'DU':
        return 'Dumped baby'
    if value == 'Fev':
        return 'Fever'
    if value == 'FD':
        return 'Not sucking / feeding difficulties'
    if value == 'G':
        return 'Gastroschisis'
    if value == 'NE':
        return 'Neonatal encephalopathy'
    if value =='HIVX':
        return 'HIV exposed'
    if value == 'J':
        return 'Jaundice'
    if value == 'LowBirthWeight'or value=='LBW':
        return 'Low Birth Weight'
    if value == 'Apg':
        return 'Low Apgars'
    if value == 'Mec':
        return 'Possible Meconium Aspiration'
    if value == 'NTD':
        return 'Neural Tube Defect / Spina Bifida'
    if value =='Prem':
        return 'Prematurity'
    if value =='PremRDS':
        return 'Prematurity with RDS'
    if value == 'SPn':
        return 'Severe Pneumonia'
    if value == 'OM':
        return 'Omphalocele'
    if value == 'Cong':
        return 'Other congenital abnormality'
    if value == 'Mac':
        return 'Macrosomia'
    if value == 'Safe':
        return 'Safekeeping'
    if value == 'Risk':
        return 'Risk factors for sepsis'
    if value=='BA':
        return 'Hypoxic ischaemic encephalopathy'
    if value == 'O':
        return 'Other'
    return None
@DeprecationWarning
def fix_adm_gest_method(value):
    if value=='LMP':
        return 'Last Menstrual Period (LMP)'
    if value == 'FH':
        return 'Fundal height'
    if value == 'USS':
        return 'USS'
    return None;
@DeprecationWarning
def fix_adm_jaundice(value):
    if value=='1':
        return 'Head and neck (TSB 100umol/L)'
    if value == '2':
        return 'Upper trunk (TSB 150umol/L)'
    if value == '3':
        return 'Lower trunk and thighs (TSB 200umol/L)'
    if value == '4':
        return 'Arms and lower legs (TSB 250umol/L)'
    if value == '5':
        return 'Palms and soles (TSB >250umol/L)'
    return None;
@DeprecationWarning
def fix_adm_feeding_review(value):
    if value=='NFY':
        return 'Has not had a breast feed yet'
    if value == 'BF':
        return 'Breast feeding normally'
    if value == 'BFH':
        return 'Needs help breast feeding'
    if value == 'CSP':
        return 'Needs cup or spoon'
    if value == 'NG':
        return 'Refusing all feeds: Needs NG'
    if value == 'IVF':
        return 'Requires IV fluids'
    if value == 'BOT':
        return 'Bottle feeding'
    return None;
@DeprecationWarning
def fix_adm_meconium(value):
    if value=='MecLiq':
        return 'Meconium present in the liquor'
    if value == 'Mec24':
        return 'Passed meconium after delivery'
    if value == 'NoMec24':
        return 'Under 24 hours old and no meconium yet'
    if value == 'NoMec':
        return 'No meconium in 1st 24 hrs'
    return None;
@DeprecationWarning
def fix_adm_stools_infant(value):
    if value=='Norm':
        return 'Opening bowels normally'
    if value == 'BNO':
        return 'Bowels not opening'
    if value == 'Diarr':
        return 'Has Diarrhoea and passing large water stools'
    if value == 'BlDi':
        return 'Bloody Diarrhoea'
    return None;
@DeprecationWarning
def fix_adm_pu_newborn(value):
    if value=='Yes':
        return 'Has passed urine in 1st 24 hrs'
    if value == 'No':
        return 'Has not passed urine'
    if value == 'Bld':
        return 'Blood in urine'
    if value == 'Unk':
        return 'Not sure'
    return None;
@DeprecationWarning
def fix_adm_pu_infant(value):
    if value=='Norm':
        return 'Passing urine normally'
    if value == 'Bld':
        return 'Blood in urine'
    if value == 'NoPU':
        return 'Not passing urine'
    return None;

@DeprecationWarning
def fix_adm_length_of_haart(value):
    if value=='1stTrim':
        return '1st Trimester or earlier'
    if value == '2ndTrim':
        return '2nd Trimester '
    if value == '3rdTrim':
        return '3rd Trimester more than 1 month before delivery'
    if value == 'Late':
        return 'Less than 1 month before delivery'
    if value == 'U':
        return 'Unknown'
    return None;

@DeprecationWarning
def fix_adm_mat_place(value):
    if value=='PostA':
        return 'Postnatal A'
    if value == 'PostB':
        return 'Postnatal B'
    if value == 'Labour':
        return 'Labour Ward'
    if value == 'ICU':
        return 'ICU'
    if value == 'Other':
        return ' Other'
    return None;
@DeprecationWarning
def fix_adm_mat_ethnicity(value):
    if value=='AA':
        return 'African/AfricanAmerican'
    if value == 'AS':
        return 'Asian/Pacific Islander'
    if value == 'W':
        return 'White'
    if value == 'NA':
        return 'NativeAmerican'
    if value == 'O':
        return ' Other'
    return None;
@DeprecationWarning
def fix_adm_mat_province(value):
    if value=='HA':
        return 'Harare'
    if value == 'BU':
        return 'Bulawayo'
    if value == 'MC':
        return 'Mashonaland Central'
    if value == 'ME':
        return 'Mashonaland East'
    if value == 'MW':
        return 'Mashonaland West'
    if value == 'MA':
        return 'Manicaland'
    if value == 'MAS':
        return 'Masvingo'
    if value == 'MN':
        return 'Matabeleland North'
    if value == 'MS':
        return 'Matabeleland South'
    if value == 'MID':
        return 'Midlands'
    return None;
@DeprecationWarning
def fix_adm_mat_district(value):
    if value=='Av':
        return 'Avondale'
    if value == 'Be':
        return 'Belvedere'
    if value == 'Bo':
        return 'Borrowdale'
    if value == 'Bu':
        return 'Budiriro'
    if value == 'CH':
        return 'Chitungwiza'
    if value == 'Dz':
        return 'Dzivaresekwa'
    if value == 'EP':
        return 'Epworth'
    if value == 'GLN':
        return 'Glen Norah'
    if value == 'GLV':
        return 'Glen View'
    if value == 'Gr':
        return 'Greendale'  
    if value == 'Ha':
        return 'Hatfield'
    if value == 'Hat':
        return 'Hatcliffe'
    if value == 'HC':
        return 'Harare Central'
    if value == 'Hi':
        return 'Highlands'
    if value == 'Hif':
        return 'Highfield'
    if value == 'Ho':
        return 'Hopley'
    if value == 'Ka':
        return 'Kambuzuma'
    if value == 'Ku':
        return 'Kuwadzana'
    if value == 'Ma':
        return 'Mabelreign'
    if value == 'Mar':
        return 'Marlborough'
    if value == 'Mab':
        return 'Mabvuku'
    if value == 'Mb':
        return 'Mbare'
    if value == 'Mo':
        return 'Mount Pleasant'
    if value == 'Mu':
        return 'Mufakose'
    if value == 'So':
        return 'Southerton'
    if value == 'Su':
        return 'Sunningdale'
    if value == 'Ta':
        return 'Tafara'
    if value == 'Wa':
        return 'Warren Park'
    if value == 'Wat':
        return 'Waterfalls'
    if value == 'OT':
        return 'Other'
    
    return None;
   
   

## FIX DISCHARGES
@DeprecationWarning
def fix_disharge_label(key,value):
        if key== 'NeoTreeOutcome':
            return fix_neotree_oucome(value)
        if key== 'SexDis':
            return fix_discharge_sex(value)
        if key== 'MatOutcome':
            return fix_maternal_outcome(value)
        if key== 'ModeDelivery':
            return fix_mode_of_delivery(value)
        if key== 'BirthPlace' :
            return fix_place_of_birth(value)
        if key== 'AdmReason':
            return fix_admission_reason(value)
        if key== 'DIAGDIS1':
            #Same values as Adm Reasons
            return fix_admission_reason(value)
        if key== 'TROWard':
            return fix_transfare_wards(value)   
        if key== 'CauseDeath':
            #Same values as Adm Reasons
            return fix_admission_reason(value)
        if key== 'BC1R':
            return fix_bcresults(value)
        if key== 'HIVPCRInfR':
            return fix_hivpcr(value)
        if key== 'MatRPRR':
            return fix_matrprr(value)
        if key== 'MatHIVPR':
            return fix_mat_hivpr(value)
        if key== 'MatPART':
           return fix_matpart(value)   
        if key== 'MatRPRT':
           return fix_matpart(value)
        if key== 'BC2R':
            #Same values as BC Results
            return fix_bcresults(value)
        if key== 'LPGlucAvail':
            return fix_lpgluc(value)
        if key== 'LPProtAvail':
            return fix_lpgluc(value)
        if key== 'CSFOrg':
            #Same values as BC Results
            return fix_bcresults(value)
        if key== 'ThompTone':
            return fix_thomp_tone(value)
        if key== 'ThompAlert':
            return fix_thomp_alert(value)
        if key== 'ThompSeiz':
            return fix_thomp_seize(value)
        if key== 'ThompRefl':
            return fix_thomp_refl(value)
        if key== 'ThompMoro':
            return fix_thomp_moro(value)
        if key== 'ThompGrasp':
            return fix_thomp_grasp(value)
        if key== 'ThompFeed':
            return fix_thomp_feed(value)
        if key== 'ThompResp': 
            return fix_thomp_resp(value)
        if key== 'ThompFont':
           return fix_thomp_front(value)
        if key== 'Transfusion':
            return fix_transfusion(value)
        if key== 'TransType':
            return fix_trans_type(value)
        if key== 'SPECREV':
              #Same Values as Transfusion
              return fix_transfusion(value)
        if key== 'SPECREVTYP':
             return fix_spec_type(value)
        if key== 'GoodProg':
             return fix_good_prog(value)
        if key== 'HighRisk':
             return fix_high_risk(value)
        if key== 'REVCLIN':
            #Same Values as Transfusion
            return fix_transfusion(value)
        if key== 'REVCLINTYP':
            #Same Values as Transfusion
            return fix_rev_clinic_type(value)
        if key== 'CadreDis':
            #Same Values as Transfusion
            return fix_cadre_disc(value)
        pass

## MATERNAL OUTCOMES
@DeprecationWarning
def fix_maternal_label(key,value):
    if(key =='SexDis'):
        return fix_discharge_sex(value)
    if (key == 'ModeDelivery'):
        return fix_maternal_mode_of_delivery(value)
    if (key== 'NeoTreeOutcome'):
        return fix_maternal_neonatal_outcome(value)
    if (key=='MatOutcome'):
        return fix_maternal_mother_outcome(value)
    pass


## ADMISSIONS
@DeprecationWarning
def fix_admissions_label(key,value):
    if(key =='GLUSTX'):
        return fix_adm_yes_no(value)
    if (key == 'AdmReason'):
        return fix_adm_admission_reason(value)
    if (key=='Readmission'):
        return fix_adm_yes_no(value)
    if (key=='DOBYN'):
        return fix_adm_yes_no(value)
    if (key=='AgeEstimate'):
        return fix_age_estimate(value)
    if (key=='TypeBirth'):
        return fix_adm_type_birth(value)
    if (key =='MethodEstGest'):
        return fix_adm_gest_method(value)
    if (key=='YColour'):
        return fix_adm_yes_no(value)
    if (key =='Jaundice'):
        return fix_adm_jaundice(value)
    if (key=='FeedingReview'):
        return fix_adm_feeding_review(value)
    if (key=='PassedMec'):
        return fix_adm_meconium(value)
    if (key=='StoolsInfant'):
        return fix_adm_stools_infant(value)
    if(key=='PUNewborn'):
        return fix_adm_pu_newborn(value)
    if(key=='PUInfant'):
        return fix_adm_pu_infant(value)
    if(key=='MatAdmPlace'):
        return fix_adm_mat_place(value)
    if(key=='Ethnicity'):
        return fix_adm_mat_ethnicity(value)
    if(key=='MatAddrProvince'):
        return fix_adm_mat_province(value)
    if(key=='MatAddrHaDistrict'):
        return fix_adm_mat_district(value)
    if(key=='TestThisPreg'):
        return fix_adm_yes_no_unknown(value)
    if(key=='HIVtestResult'):
        return fix_hiv_result(value)
    if(key=='HIVtestReport'):
        return fix_hiv_report(value)
    if(key=='HAART'):
        return fix_adm_yes_no(value)
    if(key=='LengthHAART'):
        return fix_adm_length_of_haart(value)
    if(key=='VLKnown'):
        return fix_adm_yes_no_na(value)
    if(key=='DateVDRLSameHIV'):
        return fix_adm_yes_no(value)
    if(key=='ANVDRLResult'):
        return fix_anvdrl_result(value)
    if(key=='ANVDRLReport'):
        return fix_hiv_report(value)
    if(key=='ANMatSyphTreat'):
        return fix_adm_yes_no(value)
    if(key=='PartnerTrSyph'):
        return fix_adm_yes_no_unknown(value)
    if(key=='Iron'):
        return fix_adm_yes_no_unknown(value)
    if(key=='Folate'):
        return fix_adm_yes_no_unknown(value)
    if(key=='TTV'):
        return fix_adm_yes_no_unknown(value)
    if(key=='BBALoc'):
        return fix_adm_place_of_birth(value)
    if(key=='BBALoc'):
        return fix_adm_place_of_birth(value)
    if(key=='BBADel'):
        return fix_adm_who_delivered(value)
    if(key=='ROMlength'):
        return fix_adm_rom_length(value)
    if(key=='Presentation'):
        return fix_adm_presentation(value)
    if(key=='ModeDelivery'):
        return fix_adm_mode_delivery(value)
    if(key=='Reason'):
        return fix_adm_reason_for_cs(value)
    if(key=='CryBirth'):
        return fix_adm_yes_no_unknown(value)
    if (key=='CPROut'):
        return fix_adm_cpr_out(value)
    if (key=='VitK'):
        return fix_adm_yes_no_ns(value)
    if (key=='TEO'):
        return fix_adm_yes_no_ns(value)
    if (key=='NMPosture' or key=='NMSquare' or key=='NMArmR' or key=='NMPop' 
    or key=='NMScarf' or key =='NMHeelEar' or key=='NMSkin' or key=='NMLan' 
    or key=='NMPlant' or key=='NMBreast' or key=='NMEye' or key=='NMGen'):
        return fix_adm_balad_score(value)
    if(key=='ThompTone'):
        return fix_adm_thomp_tone(value)
    if(key=='ThompAlert'):
        return fix_adm_thomp_alert(value)
    if(key=='ThompSeiz'):
        return fix_adm_thomp_seiz(value)
    if(key=='ThompRefl'):
        return fix_adm_thomp_refl(value)
    if(key=='ThompMoro'):
        return fix_adm_thomp_moro(value)
    if(key=='ThompGrasp'):
        return fix_thomp_grasp(value)
    if(key=='ThompFeed'):
        return fix_thomp_feed(value)
    if(key=='ThompResp'):
        return fix_thomp_resp(value)
    if(key=='ThompFont'):
        return fix_thomp_font(value)
    if(key=='Cadre'):
        return fix_adm_cadre(value)
    if(key=='Firm'):
        return fix_adm_firm(value)
    if(key=='Review'):
        return fix_adm_yes_no(value)
    if(key=='ReviewCadre'):
        return fix_adm_review_cadre(value)
    pass

## MATERNAL OUTCOMES
@DeprecationWarning
def fix_baseline_label(key,value):
    if(key =='NeoTreeOutcome'):
        return fix_neotree_oucome(value)
    pass