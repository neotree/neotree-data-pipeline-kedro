
from tkinter.messagebox import RETRY


def fix_neotree_oucome(value):
    match value:
        case 'DC':
            return 'Discharged'
        case 'NND':
            return 'Died'
        case 'ABS':
            return 'Absconded'
        case 'TRH':
            return 'Transferred to other Hospital'
        case 'TRO':
            return 'Transferred to other ward'
        case 'DAMA':
            return 'Discharged against medical advice'

        case _:
            return None

def fix_discharge_sex(value):
    match value:
        case 'F':
            return 'Female'
        case 'M':
            return 'Male'
        case 'U':
            return 'Unsure'

        case _:
            return None

def fix_maternal_outcome(value):
    match value:
        case 'A':
            return 'Alive'
        case 'D':
            return 'Deceased'
        case _:
            return None

def fix_place_of_birth(value):
    match value:
        case 'SMCH':
            return 'Sally Mugabe Central Hospital'
        case 'OtH':
            return 'Other clinic in Harare'
        case 'OtR':
            return 'Other clinic outside Harare'
        case 'H':
            return 'Home'
        case _:
            return None

def fix_mode_of_delivery(value):
    match value:
        case 'SVD':
            return 'Spontaneous vaginal delivery'
        case 'IVD':
            return 'Induced vaginal delivery'
        case 'ECS':
            return 'Emergency Caesarean section'
        case 'ElCS':
            return 'Elective Caesarean section'
        case 'Vent':
            return 'Ventouse'
        case _:
            return None

def fix_transfare_wards(value):
    match value:
        case 'PostA':
            return 'Postnatal A'
        case 'PostB':
            return 'Postnatal B'
        case 'Paeds':
            return 'Paediatric'
        case 'OTH':
            return 'Other'
        case _:
            return None

def fix_transfare_wards(value):
    match value:
        case 'PostA':
            return 'Postnatal A'
        case 'PostB':
            return 'Postnatal B'
        case 'Paeds':
            return 'Paediatric'
        case 'OTH':
            return 'Other'
        case _:
            return None

def fix_bcresults(value):
    match value:
        case 'NO':
            return 'No Growth'
        case 'CO':
            return 'Likely Contaminant Organism'
        case 'BS':
            return 'Coagulase Negative Staphylococcus'
        case 'CONS':
            return 'Coagulase Negative Staphylococcus'
        case 'EC':
            return 'E.Coli'
        case 'GBS':
            return 'Group B Streptococcus'
        case 'GDS':
            return 'Group D Streptococcus'
        case 'KP':
            return 'Klebsiella Pneumoniae'
        case 'PA':
            return 'Pseudomonas Aeruginosa'
        case 'SA':
            return 'Staphylococcus Aureus'
        case 'LFC':
            return 'Lactose Fermenting Coliform'
        case 'NLFC':
            return 'Non Lactose Fermenting Coliform'
        case 'OGN':
            return 'Other Gram Negative'

        case 'OGP':
            return 'Other Gram Positive'

        case 'OGP':
            return 'Other Gram Positive'
        
        case 'OTH':
            return 'Other'
        case _:
            return None

def fix_hivpcr(value):
    match value:
        case 'P':
            return 'Positive'

        case 'N':
            return 'Negative'
        
        case 'I':
            return 'Indeterminate'
        case _:
            return None

def fix_mat_hivpr(value):
    match value:
        case 'P':
            return 'Code 1'

        case 'N':
            return 'Code 0'
        
        case 'I':
            return 'Indeterminate'
        case _:
            return None        

def fix_matrprr(value):
    match value:
        case 'P':
            return 'Positive'

        case 'N':
            return 'Negative'
        
        case 'I':
            return 'Indeterminate'
        
        case 'U':
            return 'Unknown'

        case _:
            return None

def fix_matpart(value):
    match value:
        case 'Y':
            return 'Yes'

        case 'N':
            return 'No'
        
        case 'U':
            return 'Unknown'

        case _:
            return None

def fix_lpgluc(value):
    match value:
        case 'Y':
            return 'Yes'

        case 'N':
            return 'No'
        
        case 'ND':
            return 'Not Done'

        case _:
            return None


def fix_thomp_tone(value):
    match value:
        case '0':
            return '0 = normal'
        case '1':
            return '1 = hypertonia' 
        case '2':
            return '2 = hypotonia'
        case '3':
            return '3 = flaccid'

        case _:
            return None
def fix_thomp_alert(value):
    match value:
        case '0':
            return '0 = Alert'
        case '1':
            return '1 = Hyperalert/stare' 
        case '2':
            return '2 = Lethargic'
        case '3':
            return '3 = Comatose'
        case _:
            return None

def fix_thomp_seize(value):
    match value:
        case '0':
            return '0 = none'
        case '1':
            return '1 = 2 or less per day' 
        case '2':
            return '2 = More than 2 per day'
        case _:
            return None

def fix_thomp_refl(value):
    match value:
        case '0':
            return '0 = Normal'
        case '1':
            return '1 = Fisting, cycling' 
        case '2':
            return '2 = Strong distal flexion'
        case '3':
            return '3 = Decerebrate'
        case _:
            return None

def fix_thomp_moro(value):
    match value:
        case '0':
            return '0 = Normal'
        case '1':
            return '1 = Partial' 
        case '2':
            return '2 = Absent'
        case _:
            return None

def fix_thomp_grasp(value):
    match value:
        case '0':
            return '0 = Normal'
        case '1':
            return '1 = Poor' 
        case '2':
            return '2 = Absent'
        case _:
            return None

def fix_thomp_feed(value):
    match value:
        case '0':
            return '0 = Normal'
        case '1':
            return '1 = Poor' 
        case '2':
            return '2 = Absent or with bites'
        case _:
            return None

def fix_thomp_resp(value):
    match value:
        case '0':
            return '0 = normal'
        case '1':
            return '1 = Hyperventilation' 
        case '2':
            return '2 = Brief apnoea'
        case '3':
            return '3 = Apnoea needing ventilatory support'
        case _:
            return None

def fix_thomp_front(value):
    match value:
        case '0':
            return '0 = normal'
        case '1':
            return '1 = full, not tense' 
        case '2':
            return '2 = tense'
        case _:
            return None

def fix_transfusion(value):
    match value:
        case 'Y':
            return 'Yes'
        case 'N':
            return 'No' 
        case _:
            return None

def fix_high_risk(value):
    match value:
        case 'Y':
            return 'Yes'
        case 'N':
            return 'No' 
        case 'NS':
            return 'Not Sure' 
        case _:
            return None

def fix_trans_type(value):
    match value:
        case 'R':
            return 'Packed Red Cells'
        case 'FFP':
            return 'FFP' 
        case 'P':
            return 'Platelets' 
        case 'E':
            return 'Exchange transfusion' 
        case 'M':
            return 'More than one kind' 
        case _:
            return None

def fix_spec_type(value):
    match value:
        case 'Neph':
            return 'Nephrology'
        case 'Ortho':
            return 'Orthopaedic Surgery' 
        case 'Surg':
            return 'Surgical' 
        case 'Opth':
            return 'Opthalmology' 
        case 'MxFx':
            return 'Maxillofacial Surgery' 
        case 'Neuro':
            return 'Neurology' 
        case 'Endoc':
            return 'Endocrinology' 
        case 'Oth':
            return 'Other' 
        case _:
            return None

def fix_rev_clinic_type(value):
    match value:
        case 'KMC':
            return 'Kangaroo clinic (Wednesday and Thursday)'
        case 'NNC':
            return 'Neonatal clinic (Friday)' 
        case 'LOC':
            return 'Local clinic'
        case 'SUR':
            return 'Surgical clinic'  
        case 'ORTH':
            return 'Orthopaedic clinic' 
        case 'OTH':
            return 'Other clinic'   
        case _:
            return None

def fix_cadre_disc(value):
    match value:
        case 'S':
            return 'SRMO'
        case 'SH':
            return 'SHO' 
        case 'R':
            return 'Registrar'
        case 'SR':
            return 'Senior Registrar'  
        case 'N':
            return 'Nurse' 
        case 'MW':
            return 'Midwife' 
        case 'O':
            return 'Other'     
        case _:
            return None

def fix_good_prog(value):
    match value:
        case 'OB':
            return 'Admitted for observation and progressed well'
        case 'SE':
            return 'Risk factors for sepsis and progressed well' 
        case 'ME':
            return 'Respiratory distress resolved with no treatment' 
        case 'REO':
            return 'Respiratory distress resolved with O2' 
        case 'FE':
            return 'Feeding difficulties resolved with feeding support' 
        case 'JA':
            return 'Jaundice resolved with phototherapy' 
        case 'JAS':
            return 'Suspected jaundice with no phototherapy needed' 
        case 'MA':
            return 'Admitted with macrosomia Blood sugars normal so discharged' 
        case 'CS':
            return 'Confirmed sepsis treated successfully with antibiotics'
        case 'SS':
            return 'Suspected sepsis treated successfully with antibiotics'  
        case 'DI':
            return 'Died shortly after admission'  
        case 'DE':
            return 'Deteriorated and died despite interventions'  
        case 'DOA':
            return 'Dead on arrival' 
        case 'OTH':
            return 'Additional Information not covered above' 
        case _:
            return None

def fix_admission_reason(value):
    match value:
        case 'An':
            return 'Anaemia'
        case 'BBA':
            return 'Born before arrival'
        case 'BI':
            return 'Birth trauma'
        case 'Cong':
            return 'Congenital Abnormality'
        case 'CHD':
            return 'Consider Congenital Heart Disease'
        case 'Conv':
            return 'Convulsions'
        case 'Dhyd':
            return 'Dehydration'
        case 'DF':
            return 'Difficulty feeding'
        case 'DUM':
            return 'Abandoned baby'
        case 'HIVLR':
            return 'HIV low risk'
        case 'HIVHR':
            return 'HIV high risk'
        case 'HIVU':
            return 'HIV unknown'
        case 'HypogAs':
            return 'Hypoglycaemia (NOT symptomatic)'
        case 'HypogSy':
            return 'Hypoglycaemia (Symptomatic)'
        case 'RiHypog':
            return 'Risk of hypoglycaemia'
        case 'BA':
            return 'Hypoxic Ischaemic Encephalopathy'
        case 'GSch':
            return 'Gastroschisis'
        case 'MJ':
            return 'Physiological Jaundice'

        case 'LBW':
            return 'Low Birth Weight (1500-2499g)'

        case 'VLBW':
            return 'Very Low Birth Weight (1000-1499g)'

        case 'ExLBW':
            return 'ExtremelyLow Birth Weight (<1000g)'

        case 'MA':
            return 'Possible Meconium Aspiration'

        case 'MecEx':
            return 'Meconium exposure (asymptomatic baby)'

        case 'MiHypo':
            return 'Mild Hypothermia'
        
        case 'ModHypo':
            return 'Moderate Hypothermia'
        
        case 'SHypo':
            return 'Severe Hypothermia'
        
        case 'Hyperth':
            return 'Hyperthermia'
        
        case 'SEPS':
            return 'Neonatal Sepsis'

        case 'Risk':
            return 'Risk factors for sepsis (asymptomatic baby)'
        
        case 'NB':
            return 'Normal baby'
        
        case 'PN':
            return 'Pneumonia / Bronchiolitis'
        
        case 'Prem':
            return 'Premature (32-36 weeks)'
        
        case 'VPrem':
            return 'Very Premature (28-31 weeks)'
        
        case 'ExPrem':
            return 'Extremely Premature (<28 weeks)'
        
        case 'PremRD':
            return 'Prematurity with RD'

        case 'Safe':
            return 'Safekeeping'
        
        case 'TTN':
            return 'Transient Tachypnoea of Newborn (TTN)'
        
        case 'OTH':
            return 'Other'
        
        case 'HBW':
            return 'Macrosomia (>4000g)'
        
        case 'TermRD':
            return 'Term with RD'
        
        case 'DJ':
            return 'Pathological Jaundice'

        case 'sHIE':
            return 'Suspected Hypoxic Ischaemic Encephalopathy'
        
        case 'PJaundice':
            return 'Prolonged Jaundice'
        
        case 'CleftLip':
            return 'Cleft lip'
        
        case 'CleftRD':
            return 'Cleft lip and/or palate with RD'
        
        case 'CleftLipPalate':
            return 'Cleft lip and/or palate'
        
        case 'Omph':
            return 'Omphalocele'
        
        case 'Myelo':
            return 'Myelomeningocele'
        
        case 'CDH':
            return 'Congenital Dislocation of the hip (CDH)'

        case 'MiTalipes':
            return 'Mild Talipes (club foot)'
        
        case 'MoTalipes':
            return 'Moderate Talipes (club foot)'
        case _:
            return None

def fix_label(key,value):
    match key:
        case 'NeoTreeOutcome':
            return fix_neotree_oucome(value)
        case 'SexDis':
            return fix_discharge_sex(value)
        case 'MatOutcome':
            return fix_maternal_outcome(value)
        case 'ModeDelivery':
            return fix_mode_of_delivery(value)
        case 'BirthPlace' :
            return fix_place_of_birth(value)
        case 'AdmReason':
            return fix_admission_reason(value)
        case 'DIAGDIS1':
            #Same values as Adm Reasons
            return fix_admission_reason(value)
        case 'TROWard':
            return fix_transfare_wards(value)   
        case 'CauseDeath':
            #Same values as Adm Reasons
            return fix_admission_reason(value)
        case 'BC1R':
            return fix_bcresults(value)
        case 'HIVPCRInfR':
            return fix_hivpcr(value)
        case 'MatRPRR':
            return fix_matrprr(value)
        case 'MatHIVPR':
            return fix_mat_hivpr(value)
        case 'MatPART':
           return fix_matpart(value)   
        case 'MatRPRT':
           return fix_matpart(value)
        case 'BC2R':
            #Same values as BC Results
            return fix_bcresults(value)
        case 'LPGlucAvail':
            return fix_lpgluc(value)
        case 'LPProtAvail':
            return fix_lpgluc(value)
        case 'CSFOrg':
            #Same values as BC Results
            return fix_bcresults(value)
        case 'ThompTone':
            return fix_thomp_tone(value)
        case 'ThompAlert':
            return fix_thomp_alert(value)
        case 'ThompSeiz':
            return fix_thomp_seize(value)
        case 'ThompRefl':
            return fix_thomp_refl(value)
        case 'ThompMoro':
            return fix_thomp_moro(value)
        case 'ThompGrasp':
            return fix_thomp_grasp(value)
        case 'ThompFeed':
            return fix_thomp_feed(value)
        case 'ThompResp': 
            return fix_thomp_resp(value)
        case 'ThompFont':
           return fix_thomp_front(value)
        case 'Transfusion':
            return fix_transfusion(value)
        case 'TransType':
            return fix_trans_type(value)
        case 'SPECREV':
              #Same Values as Transfusion
              return fix_transfusion(value)
        case 'SPECREVTYP':
             return fix_spec_type(value)
        case 'GoodProg':
             return fix_good_prog(value)
        case 'HighRisk':
             return fix_high_risk(value)
        case 'REVCLIN':
            #Same Values as Transfusion
            return fix_transfusion(value)
        case 'REVCLINTYP':
            #Same Values as Transfusion
            return fix_rev_clinic_type(value)
        case 'CadreDis':
            #Same Values as Transfusion
            return fix_cadre_disc(value)
              
        case _:
            pass
