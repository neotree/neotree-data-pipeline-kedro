# Query To Manually correct Admissions with the specified UIDS
def manually_fix_admissions_query():
    return '''UPDATE derived.admissions SET "AW.value" = 1640,"AdmissionWeight.value"=1640 WHERE "uid" ='F55F-0513';;
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
            UPDATE derived.admissions SET "InOrOut.label" ='Outside SMCH' WHERE "InOrOut.label" = 'Outside HCH';; '''
