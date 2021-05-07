# Query To Manually correct Admissions with the specified UIDS
def manually_fix_admissions_query():
    return '''update derived.admissions set "AW.value" = 1640 WHERE "uid" ='F55F-0513';
            update derived.admissions set "AW.value" = 2000 WHERE "uid" ='6367-0975';
            update derived.admissions set "AW.value" = 2350 WHERE "uid" ='F55F-0118';
            update derived.admissions set "AW.value" = 3000 WHERE "uid" ='0BC7-0292';
            update derived.admissions set "AW.value" = 3000 WHERE "uid" ='B385-0321';
            update derived.admissions set "AW.value" = 3000 WHERE "uid" ='F55F-0665';
            update derived.admissions set "AW.value" = 3000 WHERE "uid" ='F55F-0815';
            update derived.admissions set "AW.value" = 3020 WHERE "uid" ='0BC7-0324'; 
            update derived.admissions set "AW.value" = 3300 WHERE "uid" ='9525-0817'; 
            update derived.admissions set "AW.value" = 4000 WHERE "uid" ='B385-0196'; 
            update derived.admissions set "AW.value" = 4000 WHERE "uid" ='6367-0862'; 
            update derived.admissions set "AW.value" = 4200 WHERE "uid" ='A7C6-0350'; 
            update derived.admissions set "AW.value" = 4200 WHERE "uid" ='A7C6-0378'; 

            update derived.admissions set "BW.value" =1000 WHERE uid='A7C6-0022'; 
            update derived.admissions set "BW.value" =1000 WHERE uid='6367-1109';
            update derived.admissions set "BW.value" =1385 WHERE uid='F55F-0343';
            update derived.admissions set "BW.value" =1400 WHERE uid='6367-0898'; 
            update derived.admissions set "BW.value" =1700 WHERE uid='A46C-0206';
            update derived.admissions set "BW.value" =2000 WHERE uid='B385-0330'; 
            update derived.admissions set "BW.value" =2000 WHERE uid='A46C-0214';
            update derived.admissions set "BW.value" =2350 WHERE uid='F55F-0118';
            update derived.admissions set "BW.value" =2500 WHERE uid='F55F-0805';
            update derived.admissions set "BW.value" =3000 WHERE uid='0BC7-0292';
            update derived.admissions set "BW.value" =3000 WHERE uid='F55F-0815';
            update derived.admissions set "BW.value" =3000 WHERE uid='F55F-0820';
            update derived.admissions set "BW.value" =3050 WHERE uid='B385-0218';
            update derived.admissions set "BW.value" =3100 WHERE uid='F55F-0785';
            update derived.admissions set "BW.value" =3180 WHERE uid='C22B-0117';
            update derived.admissions set "BW.value" =3600 WHERE uid='F55F-0467';
            update derived.admissions set "BW.value" =3800 WHERE uid='A7C6-0350';
            update derived.admissions set "BW.value" =3800 WHERE uid='A7C6-0378'; 
            update derived.admissions set "InOrOut.label" ='Within SMCH' WHERE "InOrOut.label" = 'Within HCH';
            update derived.admissions set "InOrOut.label" ='Outside SMCH' WHERE "InOrOut.label" = 'Outside HCH'; '''
