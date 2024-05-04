import logging

# Query To Manually correct Admissions with the specified UIDS
def manually_fix_admissions_query():
    logging.info("SOX >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ::::::::::::::: manually_fix_admissions_query")
    #formatted_hr = format_static_admission_label("HR", "Heart Rate (beats min)")
    #logging.info(formatted_hr)
    sql = '''UPDATE derived.admissions SET "AW.value" = 1640,"AdmissionWeight.value"=1640 WHERE "uid" ='F55F-0513';;
            UPDATE derived.admissions SET "AW.value" = 2000,"AdmissionWeight.value"=2000 WHERE "uid" ='6367-0975';;
            UPDATE derived.admissions SET "AW.value" = 2350,"AdmissionWeight.value"=2350 WHERE "uid" ='F55F-0118';;
            UPDATE derived.admissions SET "AW.value" = 3000,"AdmissionWeight.value"=3000 WHERE "uid" ='0BC7-0292';;
            UPDATE derived.admissions SET "AW.value" = 3000,"AdmissionWeight.value"=3000 WHERE "uid" ='B385-0321';;
            UPDATE derived.admissions SET "AW.value" = 3000,"AdmissionWeight.value"=3000 WHERE "uid" ='F55F-0665';;
            UPDATE derived.admissions SET "AW.value" = 3000,"AdmissionWeight.value"=3000 WHERE "uid" ='F55F-0815';;
            UPDATE derived.admissions SET "AW.value" = 3020,"AdmissionWeight.value"=3020 WHERE "uid" ='0BC7-0324';; 
            UPDATE derived.admissions SET "AW.value" = 3300,"AdmissionWeight.value"=3300 WHERE "uid" ='9525-0817';; 
            UPDATE derived.admissions SET "AW.value" = 4000,"AdmissionWeight.value"=4000 WHERE "uid" ='B385-0196';; 
            UPDATE derived.admissions SET "AW.value" = 4000,"AdmissionWeight.value"=4000 WHERE "uid" ='6367-0862';; 
            UPDATE derived.admissions SET "AW.value" = 4200,"AdmissionWeight.value"=4200 WHERE "uid" ='A7C6-0350';; 
            UPDATE derived.admissions SET "AW.value" = 4200,"AdmissionWeight.value"=4200 WHERE "uid" ='A7C6-0378';; 

            UPDATE derived.admissions SET "BirthWeight.value"=1000 WHERE uid='A7C6-0022';; 
            UPDATE derived.admissions SET "BirthWeight.value"=1000 WHERE uid='6367-1109';;
            UPDATE derived.admissions SET "BirthWeight.value"=1385 WHERE uid='F55F-0343';;
            UPDATE derived.admissions SET "BirthWeight.value"=1400 WHERE uid='6367-0898';; 
            UPDATE derived.admissions SET "BirthWeight.value"=1700 WHERE uid='A46C-0206';;
            UPDATE derived.admissions SET "BirthWeight.value"=2000 WHERE uid='B385-0330';; 
            UPDATE derived.admissions SET "BirthWeight.value"=2000 WHERE uid='A46C-0214';;
            UPDATE derived.admissions SET "BirthWeight.value"=2350 WHERE uid='F55F-0118';;
            UPDATE derived.admissions SET "BirthWeight.value"=2500 WHERE uid='F55F-0805';;
            UPDATE derived.admissions SET "BirthWeight.value"=3000 WHERE uid='0BC7-0292';;
            UPDATE derived.admissions SET "BirthWeight.value"=3000 WHERE uid='F55F-0815';;
            UPDATE derived.admissions SET "BirthWeight.value"=3000 WHERE uid='F55F-0820';;
            UPDATE derived.admissions SET "BirthWeight.value"=3050 WHERE uid='B385-0218';;
            UPDATE derived.admissions SET "BirthWeight.value"=3100 WHERE uid='F55F-0785';;
            UPDATE derived.admissions SET "BirthWeight.value"=3180 WHERE uid='C22B-0117';;
            UPDATE derived.admissions SET "BirthWeight.value"=3600 WHERE uid='F55F-0467';;
            UPDATE derived.admissions SET "BirthWeight.value"=3800 WHERE uid='A7C6-0350';;
            UPDATE derived.admissions SET "BirthWeight.value"=3800 WHERE uid='A7C6-0378';; 
            UPDATE derived.admissions SET "InOrOut.label" ='Within SMCH' WHERE "InOrOut.label" = 'Within HCH';;
            UPDATE derived.admissions SET "InOrOut.label" ='Outside SMCH' WHERE "InOrOut.label" = 'Outside HCH';; 
            
            UPDATE derived.admissions SET "HIVtestResult.label" = "HIVtestResult.value" WHERE "HIVtestResult.label" = 'None';;
            UPDATE derived.admissions SET "Temperature.label" = 'Temperature (degs C)' WHERE "Temperature.label" = 'None';;
            
            UPDATE derived.admissions SET "InOrOut.label" = CASE WHEN "InOrOut.value"::BOOLEAN = FALSE THEN 'Outside ' || FACILITY WHEN "InOrOut.value"::BOOLEAN = TRUE THEN 'Within ' || FACILITY ELSE "InOrOut.label" END WHERE "InOrOut.label" = 'None';;

            UPDATE derived.admissions SET "AdmReason.label" = "AdmReason.value" WHERE "AdmReason.label" = 'None';;
            
            
            UPDATE derived.admissions SET "HR.label" = 'Heart Rate (beats/min)' WHERE "HR.label" = 'None';;
            UPDATE derived.admissions SET "RR.label" = 'Respiratory Rate (breaths/min)' WHERE "RR.label" = 'None';;
            UPDATE derived.admissions SET "OFC.label" = 'Head Circumference (cm)' WHERE "OFC.label" = 'None';;
            UPDATE derived.admissions SET "PAR.label" = 'Maternal Parity' WHERE "PAR.label" = 'None';;
            UPDATE derived.admissions SET "Apgar1.label" = 'Apgar Score at 1 min' WHERE "Apgar1.label" = 'None';;
            UPDATE derived.admissions SET "Apgar5.label" = 'Apgar Score at 5 mins' WHERE "Apgar5.label" = 'None';;
            
            UPDATE derived.admissions SET "Length.label" = 'Length (cm)' WHERE "Length.label" = 'None';;
            UPDATE derived.admissions SET "SatsAir.label" = 'Oxygen Saturations in Air (%)' WHERE "SatsAir.label" = 'None';;
            UPDATE derived.admissions SET "VLNumber.label" = 'Viral Load' WHERE "VLNumber.label" = 'None';;
            UPDATE derived.admissions SET "Gestation.label" = 'Gestational age at birth (weeks.days)' WHERE "Gestation.label" = 'None';;
            UPDATE derived.admissions SET "HeadShape.label" = 'Head Shape' WHERE "HeadShape.label" = 'None';;
            UPDATE derived.admissions SET "MatAgeYrs.label" = 'Age of Mother (yrs)' WHERE "MatAgeYrs.label" = 'None';;
            UPDATE derived.admissions SET "BirthWeight.label" = 'Birth Weight (g)' WHERE "BirthWeight.label" = 'None';;
            UPDATE derived.admissions SET "Temperature.label" = 'Temperature (degs C)' WHERE "Temperature.label" = 'None';;
            UPDATE derived.admissions SET "BloodSugarmg.label" = 'Blood Sugar (mg/dL)      (45 - 81)' WHERE "BloodSugarmg.label" = 'None';;
            UPDATE derived.admissions SET "AntenatalCare.label" = 'Number of antenatal care visits?' WHERE "AntenatalCare.label" = 'None';;
            UPDATE derived.admissions SET "BloodSugarmmol.label" = 'Blood Sugar (mmol/L)      (2.5 - 4.5)' WHERE "BloodSugarmmol.label" = 'None';;
            UPDATE derived.admissions SET "AdmissionWeight.label" = 'Admission Weight (g) (if different)' WHERE "AdmissionWeight.label" = 'None';;
             
            '''
            
            # 
            

            #  
            # {format_static_admission_label("","")}
            # {format_static_admission_label("","")}
            # {format_static_admission_label("","")}
                        
    #logging.info(sql)    
    return sql
            
def format_static_admission_label(label,value):
    # sql = f'''UPDATE derived.admissions SET "{label}.label" = "{value}" WHERE "{label}.label" = 'None';;'''
    # sql = f'''"{label} {value}"''' 
    # f'''UPDATE derived.admissions SET "{label}.label" = "{value}" WHERE "{label}.label" = 'None';;'''
    # logging.log(sql)
    logging.log(label)
    logging.log(value)
    return ""  