import logging
from conf.base.catalog import params
from pathlib import Path
import json
import ast
from datetime import datetime
from re import search
from conf.common.sql_functions import inject_sql_with_return,inject_sql

def createAdmissionsAndDischargesFromRawData():
    data = formatRawData()
    if data is not None:
        distinct_sessions = []
        copy_data = [
'F665-0602',
'F665-0601',
'EF78-0721',
'EF78-0708',
'EF78-0707',
'EF78-0703',
'EBEF-0065',
'EBEF-0063',
'EBEF-0062',
'EBEF-0061',
'EBEF-0060',
'EBEF-0059',
'EBEF-0049',
'EBEF-0045',
'EBEF-0042',
'EBEF-0039',
'EBEF-0038',
'EBEF-0035',
'EBEF-0032',
'EBEF-0024',
'EBEF-0022',
'EBEF-0009',
'EBEF-0005',
'D667-0059',
'D667-0051',
'D667-0050',
'D667-0049',
'D667-0048',
'D667-0037',
'D667-0035',
'D667-0028',
'D667-0027',
'D667-0019',
'D667-0018',
'D667-0010',
'D667-0003',
'CF1D-0128',
'CBD9-0097',
'CBD9-0095',
'CBD9-0094',
'CBD9-0090',
'CBD9-0089',
'CBD9-0088',
'CBD9-0086',
'CBD9-0083',
'CBD9-0082',
'CBD9-0080',
'CBD9-0064',
'CBD9-0058',
'CBD9-0045',
'CBD9-0014',
'CBD9-0012',
'B5E6-0087',
'B5E6-0086',
'B5E6-0085',
'B5E6-0080',
'B5E6-0075',
'B5E6-0073',
'B5E6-0069',
'B5E6-0063',
'B5E6-0062',
'B5E6-0061',
'B5E6-0058',
'B5E6-0055',
'B5E6-0054',
'B5E6-0053',
'B5E6-0051',
'B5E6-0039',
'B5E6-0038',
'B5E6-0036',
'B5E6-0031',
'B5E6-0030',
'B5E6-0029',
'B5E6-0023',
'B5E6-0014',
'B5E6-0013',
'7D17-0718',
'7D17-0715',
'72E1-0029',
'72E1-0028',
'72E1-0027',
'72E1-0025',
'72E1-0008',
'71D1-0680',
'6909-0086',
'6909-0085',
'6909-0083',
'6909-0061',
'6909-0057',
'6909-0056',
'6909-0054',
'6909-0053',
'6909-0051',
'6909-0044',
'6909-0030',
'6909-0026',
'6909-0025',
'6909-0020',
'6909-0019',
'6909-0018',
'6909-0017',
'6909-0013',
'6909-0009',
'6909-0008',
'6716-0058',
'6716-0056',
'6716-0053',
'6716-0052',
'6716-0050',
'6716-0049',
'6716-0047',
'6716-0044',
'6716-0038',
'6716-0037',
'6716-0036',
'6716-0034',
'6716-0033',
'6716-0020',
'6716-0009',
'6716-0006',
'5528-0106',
'51D4-0070',
'51D4-0068',
'51D4-0053',
'51D4-0052',
'51D4-0025',
'51D4-0021',
'45C7-0067',
'45C7-0066',
'45C7-0065',
'45C7-0046',
'45C7-0045',
'45C7-0026',
'45C7-0025',
'45C7-0023',
'45C7-0022',
'45C7-0021',
'45C7-0018',
'45C7-0017',
'45C7-0015',
'45C7-0014',
'45C7-0011',
'4441-0015',
'4441-0014',
'4441-0011',
'4441-0009',
'4441-0008',
'2653-0198',
'2653-0189',
'2474-0008',
'0E05-0714',
'0E05-0704',
'0028-4140',
'0028-2485']
        #Duplicates Key Should Be In Data To Show That It has Validated Availability of Duplicates(It can be Empty)
        if "sessions"  in data and "duplicates" in data:
            possible_duplicates = data["duplicates"];
            uids = []
            for item in data["duplicates"]:
                if(item["script"]["id"]=='-ZYDiO2BTM4kSGZDVXAO'):
                    uids.append(item["uid"])
           
            if len(possible_duplicates) >0:
                for session in data["sessions"]:
                    if dict(uid=session["uid"],script=session["script"]) in possible_duplicates:
                        pass;
                    else:
                        distinct_sessions.append(session) 
            else:
                distinct_sessions = data["sessions"]
               
            for sess in distinct_sessions:
                insertion_data = json.dumps(sess);
                json_string = insertion_data.replace("'s","s")

                #Convert The Date To Strtime 
                ingested_at = datetime.now()
                scriptId = sess["script"]["id"]
                uid = sess["uid"]
                if(scriptId !="-ZO1TK4zMvLhxTw6eKia" and scriptId!="-ZYDiO2BTM4kSGZDVXAO"):
                    print("sid=",scriptId, "-uid=",uid)
                insertion_query = '''INSERT INTO public.sessions (ingested_at,uid, scriptid,data) VALUES('{0}','{1}','{2}','{3}');'''.format(ingested_at,uid,scriptId,json_string)
                inject_sql(insertion_query,"DATA INSERTION")
    else:
       logging.warn("Importing JSON Files Skipped Because No Data is Available In The specified Directory") 
            

#Restructure All Data To Suit A Format That Is Easy To Read And Export To Dbase
def formatRawData():

    if(params is not None and params["mode"] is not None and params["mode"]=="import"):
        if 'files_dir' in params:
            files_dir = Path(params['files_dir'])
            if files_dir.exists() and files_dir.is_dir():
                formatedSessions = []
                uids = []
                if(any(files_dir.iterdir())):
                    for filename in list(files_dir.glob(r"*.json")):
                            json_file = open(filename,'r');
                            json_script = json_file.read();
                            json_sessions = json.loads(json_script);
                            if "sessions" in json_sessions:
                                sessions = json_sessions['sessions'] 
                                for session in sessions:
                                    
                                    if "uid" in session.keys():
                                        #Check If UID IS Null ---Discharges For Zim Had NeoTreeId inplace Of UID
                                        if session["uid"] is None:
                                            if "entries" in session:
                                                entries = session["entries"];
                                                #FORMAT OLD DATA FORMAT
                                                if type(entries) is list :
                                                    for entry in entries:
                                                        if "key" in entry.keys():
                                                            if entry["key"] =="uid":
                                                                if "values" in entry.keys():
                                                                    values = entry["values"];
                                                                    if len(values)>0:
                                                                        value = values[0]
                                                                        if "value" in value.keys():
                                                                            session["uid"] = value["value"];
                                                                        else:
                                                                            session["uid"] = None;
                                                                    else:
                                                                        session["uid"] = None;
                                                                else:
                                                                    session["uid"] = None;
                                                            else:
                                                                if entry["key"] =="NeoTreeID":
                                                                    if "values" in entry.keys():
                                                                        values = entry["values"];
                                                                        if len(values)>0:
                                                                            value = values[0]
                                                                            if "value" in value.keys():
                                                                                session["uid"] = value["value"];
                                                                            else:
                                                                                session["uid"] = None;
                                                                        else:
                                                                            session["uid"] = None;
                                                                    else:
                                                                        session["uid"] = None;
                                                    
                                                        
                                                #Where New Format Is Dictionary                        
                                                else:
                                                    entries = session["entries"]
                                                    ##For Items With UID In The Entries Not In The Top Level Object
                                                    if "UID" in entries.keys():
                                                        UID = entries["UID"]
                                                        if "values" in UID.keys(): 
                                                            values = UID["values"]
                                                            if "value" in values.keys():
                                                                value = values["value"];
                                                                if type(value) is list:
                                                                    session["uid"] = value[0];
                                                    
                                                    ##For Items Without UID Field But Have NeoTreeID In The Entries 
                                                    if "NeoTreeID" in entries.keys():
                                                        neotree_id = entries["NeoTreeID"]
                                                        if "values" in neotree_id.keys():
                                                            values = neotree_id["values"]
                                                            if "value" in values.keys():
                                                                value = values["value"];
                                                                if type(value) is list:
                                                                    session["uid"] = value[0];
                                                    
                                    else:
                                        if "entries" in session:
                                            entries = session["entries"];
                                            #Just Making Sure That We Have List Type (By Default It Comes As List)
                                            if type(entries) is list:
                                                for entry in entries:
                                                    if "key" in entry.keys():
                                                        if entry["key"] =="UID":
                                                            if "values" in entry.keys():
                                                                values = entry["values"];
                                                                if len(values)>0:
                                                                    value = values[0]
                                                                    if "value" in value.keys():
                                                                        session["uid"] = value["value"];
                                                                    else:
                                                                        session["uid"] = None;
                                                                else:
                                                                    session["uid"] = None;
                                                            else:
                                                                session["uid"] = None;
                                                        
                                                        if entry["key"] =="NeoTreeID":
                                                            if "values" in entry.keys():
                                                                values = entry["values"];
                                                                if len(values)>0:
                                                                    value = values[0]
                                                                    if "value" in value.keys():
                                                                        session["uid"] = value["value"];
                                                                    else:
                                                                        session["uid"] = None;
                                                                else:
                                                                    session["uid"] = None;
                                                            else:
                                                                session["uid"] = None;
                                                        
                                    if "uid"in session.keys() and session["uid"] is not None:                            
                                        if "script" in session.keys():
                                            script = session["script"]
                                            if "id" in script :
                                                scriptId = script["id"]
                                                if scriptId is not None:
                                                    formatedSessions.append(session) 
                                                    #Add uid To list for duplicates check 
                                                    uids.append(session["uid"])
                                   
                
                            json_file.close();
                    #Check If There Exist A Record With The Same UID in The Database
                    potential_duplicates = checkDuplicateDatabaseRecord(uids);
                    return dict(sessions=formatedSessions,duplicates=potential_duplicates);        
                else:
                    logging.warn("Importing JSON Files Will Be Skipped Because the specified  'files_dir' in database.ini does not exist")
                    return None;  
            else:
                logging.warn("Importing JSON Files Will Be Skipped Because the specified  'files_dir' in database.ini does not exist")   
            return None;
        else:
            logging.warn("Importing JSON Files Will Be Skipped Because No 'files_dir' is set in database.ini")
            return None;
    else:
        return None;

def checkDuplicateDatabaseRecord(uids):
    if type(uids) is list and len(uids)>0:
        #Query To Check If The Specified UID Exists
        #PD = Possible Duplicates
        pd_list = []
        query = '''SELECT "uid" as "uid","data"->'script' as "script" from public.sessions where "uid" in ({}) '''.format(str(uids)[1:-1].replace("\"","\'"))
        possible_duplicates = inject_sql_with_return(query);
        for value in possible_duplicates:
            value_dict = dict(uid=value["uid"],script=value["script"]);
            pd_list.append(value_dict)
        return pd_list;
    
    return None

    
  