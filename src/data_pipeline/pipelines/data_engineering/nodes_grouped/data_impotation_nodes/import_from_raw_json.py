import os, sys
sys.path.append(os.getcwd())
from conf.common.config import config
from data_pipeline.pipelines.data_engineering.data_tyding.import_raw_jsons import createAdmissionsAndDischargesFromRawData;

params = config()
mode = None
if "mode" in params:
    mode = params["mode"]
#Not passing any Input To Allow Concurrent running of independent Nodes
def import_json_files():
    try:
        
        if mode == "import":
            createAdmissionsAndDischargesFromRawData();
            return dict(
            status='Success',
            message = "Data Importation Successful"
            )
        else:
             return dict(
            status='Skipped',
            message = "Importation"
            )

    except Exception as e:
        raise e
        return None