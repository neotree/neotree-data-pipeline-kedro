
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

        if value== 'null':
            return None

def fix_discharge_sex(value):
        if value== 'F':
            return 'Female'
        if value== 'M':
            return 'Male'
        if value== 'U':
            return 'Unsure'

        if value== 'null':
            return None

def fix_maternal_outcome(value):
    
        if value== 'A':
            return 'Alive'
        if value== 'D':
            return 'Deceased' 
        return None

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
        return None

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

def fix_hivpcr(value):
    
        if value== 'P':
            return 'Positive'

        if value== 'N':
            return 'Negative'
        
        if value== 'I':
            return 'Indeterminate'
        return None

def fix_mat_hivpr(value):
    
        if value== 'P':
            return 'Code 1'

        if value== 'N':
            return 'Code 0'
        
        if value== 'I':
            return 'Indeterminate'
        return None        

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

def fix_matpart(value):
    
        if value== 'Y':
            return 'Yes'

        if value== 'N':
            return 'No'
        
        if value== 'U':
            return 'Unknown'

        return None

def fix_lpgluc(value):
    
        if value== 'Y':
            return 'Yes'

        if value== 'N':
            return 'No'
        
        if value== 'ND':
            return 'Not Done'
        return None


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

def fix_thomp_seize(value):
    
        if value== '0':
            return '0 = none'
        if value== '1':
            return '1 = 2 or less per day' 
        if value== '2':
            return '2 = More than 2 per day'
        return None

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

def fix_thomp_moro(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Partial' 
        if value== '2':
            return '2 = Absent'
        return None

def fix_thomp_grasp(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Poor' 
        if value== '2':
            return '2 = Absent'
        return None

def fix_thomp_feed(value):
    
        if value== '0':
            return '0 = Normal'
        if value== '1':
            return '1 = Poor' 
        if value== '2':
            return '2 = Absent or with bites'
        return None

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

def fix_thomp_front(value):
    
        if value== '0':
            return '0 = normal'
        if value== '1':
            return '1 = full, not tense' 
        if value== '2':
            return '2 = tense'
        return None

def fix_transfusion(value):
    
        if value== 'Y':
            return 'Yes'
        if value== 'N':
            return 'No' 
        return None

def fix_high_risk(value):
    
        if value== 'Y':
            return 'Yes'
        if value== 'N':
            return 'No' 
        if value== 'NS':
            return 'Not Sure' 
        return None

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
