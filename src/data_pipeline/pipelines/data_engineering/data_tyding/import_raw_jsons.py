import logging
from conf.base.catalog import params
from pathlib import Path
import json
from datetime import datetime
from conf.common.sql_functions import inject_sql_with_return,inject_sql

def createAdmissionsAndDischargesFromRawData():
    data = formatRawData()
    logging.info(f"######FFFF--{data}")
    if data is not None:
        distinct_sessions = []
        #Duplicates Key Should Be In Data To Show That It has Validated Availability of Duplicates(It can be Empty)
        logging.info(f"---MY DATA:::{len(data)}")
        if "sessions"  in data and "duplicates" in data:
            logging.info(f"---IMPORTING:::")
            possible_duplicates = data["duplicates"];
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
                
                ingested_at = datetime.now()
                scriptId = sess["script"]["id"]
                uid = sess["uid"]
                insertion_query = '''INSERT INTO public.sessions (ingested_at,uid, scriptid,data) VALUES('{0}','{1}','{2}','{3}');;'''.format(ingested_at,uid,scriptId,json_string)
                logging.info(f"....FF....{insertion_query}")
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
                    chc_uids = []
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

                                                                if entry["key"] =="NeoTreeIDBC":
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
                                                    ##For Items Without UID Field But Have NeoTreeIDBC In The Entries 
                                                    if "NeoTreeIDBC" in entries.keys():
                                                        neotree_id = entries["NeoTreeIDBC"]
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

                                                        if entry["key"] =="NeoTreeIDBC":
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
                                            #Fix issue With Same Script Being used for multiple Sites Case Of Harare and Chinhoi
                                            if str(filename).endswith('Chinhoyi_Maternity_Outcome.json'):
                                                session['script']['id'] = '-MYk0A3-Z_QjaXYU5MsS'
                                                session['scriptTitle'] = 'Chinhoyi Maternity Outcome'
                                                session['title'] = '-MYk0A3-Z_QjaXYU5MsS'
                                            if "id" in script :
                                                scriptId = script["id"]
                                                if scriptId is not None:
                                                    formatedSessions.append(session) 
                                                    #Add uid To list for duplicates check 
                                                    uids.append(session["uid"])                                            
                                             
                            json_file.close();
                    #Check If There Exist A Record With The Same UID in The Database
                    potential_duplicates = checkDuplicateDatabaseRecord(uids);
                    logging.info(f"@@@@@----@@@---{potential_duplicates}")
                    return dict(sessions=formatedSessions,duplicates=potential_duplicates);        
                else:
                    logging.warning("Importing JSON Files Will Be Skipped Because the specified  'files_dir' in database.ini does not exist")
                    return None;  
            else:
                logging.warning("Importing JSON Files Will Be Skipped Because the specified  'files_dir' in database.ini does not exist")   
            return None;
        else:
            logging.warning("Importing JSON Files Will Be Skipped Because No 'files_dir' is set in database.ini")
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
            value_dict = dict(uid=value[0],script=value[1]);
            pd_list.append(value_dict)
        return pd_list;
    
    return None

    
  