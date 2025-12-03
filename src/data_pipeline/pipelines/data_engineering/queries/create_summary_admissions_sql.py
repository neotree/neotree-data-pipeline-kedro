from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.common.sql_functions import get_table_column_names
import logging

def get_available_columns(schema='derived', table='admissions'):
    """Get set of available column names from the admissions table."""
    try:
        columns_result = get_table_column_names(table, schema)
        return {row[0] for row in columns_result}
    except Exception as e:
        logging.warning(f"Could not fetch columns from {schema}.{table}: {e}")
        return set()

def build_column_select(source_col, target_col, available_cols, is_case_statement=False):
    """
    Build a column select statement that handles missing columns gracefully.

    Args:
        source_col: The source column name (e.g., "ChestAusc.label")
        target_col: The target alias (e.g., "Chest Auscultation")
        available_cols: Set of available column names in the source table
        is_case_statement: If True, this is part of a CASE statement (return quoted column or NULL)

    Returns:
        SQL string for column selection, or NULL AS alias if column doesn't exist
    """
    # For CASE statements, return quoted column name or NULL
    if is_case_statement:
        base_col = source_col.strip('"')
        if base_col in available_cols:
            return f'"{base_col}"'  # Return quoted column name for CASE statements
        else:
            return 'NULL'

    # For regular columns
    base_col = source_col.strip('"')
    if base_col in available_cols:
        return f'"{base_col}" AS "{target_col}"'
    else:
        logging.debug(f"Column {base_col} not found, using NULL for {target_col}")
        return f'NULL AS "{target_col}"'

#Query to create summary_maternala_completeness table
def summary_admissions_query():
  # Get available columns dynamically
  available_cols = get_available_columns('derived', 'admissions')
  logging.info(f"Found {len(available_cols)} columns in derived.admissions")

  # Helper to build column select
  def col(source, target, is_case=False):
    return build_column_select(source, target, available_cols, is_case)

  prefix = f'''  DROP TABLE IF EXISTS derived.summary_admissions;;
                CREATE TABLE derived.summary_admissions AS  '''
  where = ''
  if(table_exists('derived','summary_admissions')):
    # Ensure all columns exist before INSERT (handles schema evolution)
    prefix=f'''
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "Manual Heart Rate" TEXT;;
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "MatComorbidities" TEXT;;
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "MatComorbidities.value" TEXT;;
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "DOBYN.value" TEXT;;
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "Age Estimated" TEXT;;
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "Age" TEXT;;
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "Age Category" TEXT;;
    ALTER TABLE "derived"."summary_admissions" ADD COLUMN IF NOT EXISTS "BirthWeight" TEXT;;

    INSERT INTO "derived"."summary_admissions" (
    "Facility Name","NeoTree_ID","DateTime Admission","Re-admission?","Gender","Baby Cry Triage","Further Triage",
    "Danger Signs1","Danger Signs2","Respiratory Rate","Saturation in Air","Heart Rate","Oxygen Saturation",
    "Temperature","Temperature Group","TempThermia","Blood Sugar mmol","Blood Sugar mg","Admission Weight","Admission Weight Group",
    "Birth Weight","Birth Weight Group","<28wks/1kg","Low Birth Weight?","Head Circumference (cm)","Admission Reason",
    "Other admission reason","AgeB.label","AgeC.label","AgeA.label","Type of Birth","Gestation","Gestetation Group",
    "Method of Gestation Extimation","Presentation","Mode of Delivery","Meconium Present?","Cry at Birth?","Baby Colour",
    "Apgar score at 1 min","Apgar score at 5 mins","Apgar score at 10 mins","Palate","Head Shape","Dysmorphic",
    "Spine","Activity","Signs of Respiratory Distress","Work of breathing","Stethoscope use?","Chest Auscultation","Murmur",
    "Signs of Dehydration","Abdomen","Umbilicus","Genitalia","Anus2","Musculoskeletal problems","Skin tone","Breathing Problem",
    "Vomiting","Feeding Review","Stools Infant","SRNeuroOther","GSCvsOM","InOrOut","Other Referred From","Referred From","Other Referral Facility",
    "Place of Birth","Birth Facility","Same birth place?","Other Birth Facility","Mothers Disctrict","Mathors Age in years",
    "Marital Status","Ethnicity", "Tribe", "Other Tribe", "Religion", "Other Religion", "HIV test?", "ANVDRL","Date of HIV test",
    "When HIV test was done","HIV test Result","HAART","Length of HAART","NVP given?","ANVDRLDate","Date of VDRL Same as HIV Test Date?",
    "ANVDRL Result","Conditions in Pregnancy","Antenatal Care","Mataternal Syphillis Treated?","IPT Taken","FeFo","TTV","Antenatal Steroids",
    "Problems in Labor","Duration in Labor","ROM","ROM Length","Risk Factors for Sepsis","Resusitation","IM vit K given at birth?",
    "TEO given at birth?","Chlorhexidine on umbilicus at birth?","Plan","Other Plan","RespSR","Diagnoses","Other Diagnoses",
    "Diagnosis (Surgical Cond)","Admission Reason (Surgical Cond)","Admission Source","Meconium?","Passing Urine?","Passing urine? (infant)",
    "Suck Reflex","Fontanelle","Tone","Level of Conciousness","Fits, Seizures or convulsions","Respiration","Thompson Score",
    "Posture","Moro reflex","Grasp reflex","Reason for CS","Other Reason for CS","Length of Resusitation","Length of Resusitation (Known)",
    "Meconium Thick or Thin","Cardiovascular exam","Femorals","HypoSxYN","Chest Ausc","Respiratory Support","RISK for Covid?","External Source",
    "Mothers Symptoms","Mothers Diagnosis","Mother Oxygen saturations","is mother present?","Other Ethnicity",
    "Manual Heart Rate","MatComorbidities","MatComorbidities.value","DOBYN.value","Age Estimated","Age","Age Category","BirthWeight")   '''
    where=f''' WHERE NOT EXISTS ( SELECT 1  FROM derived.summary_admissions  WHERE "NeoTree_ID" IN (select uid from derived.admissions))'''

  return   prefix +f''' SELECT "facility" AS "Facility Name",
                    "uid" AS "NeoTree_ID",
                     CASE
                    WHEN "DateTimeAdmission.value"::TEXT ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                    to_timestamp("DateTimeAdmission.value", 'DD Mon,YYYY')
                    WHEN "DateTimeAdmission.value"::TEXT ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                    to_timestamp("DateTimeAdmission.value", 'YYYY Mon,DD')
                    ELSE NULL
                    END AS "DateTime Admission",
                    "Readmission.label" AS "Re-admission?",
                    "Gender.label" AS "Gender",
                    "BabyCryTriage.label" AS "Baby Cry Triage",
                    "FurtherTriage.label" AS "Further Triage",
                    "DangerSigns.label" AS "Danger Signs1",
                    "DangerSigns2.label" AS "Danger Signs2",
                    "RR.value" AS "Respiratory Rate",
                    "SatsAir.value" AS "Saturation in Air",
                    "HR.value" AS "Heart Rate",
                    "SatsO2.value" AS "Oxygen Saturation",
                    "Temperature.value" AS "Temperature",
                    "TempGroup.value" AS "Temperature Group",
                    "TempThermia.value" AS "TempThermia",
                    "BSmmol.value" AS "Blood Sugar mmol",
                    "BSmg.value" AS "Blood Sugar mg",
                    "AW.value" AS "Admission Weight",
                    "AWGroup.value" AS "Admission Weight Group",
                    CASE WHEN "BirthWeight.value" is not null THEN 
                    "BirthWeight.value" END AS "Birth Weight",
                    "BWGroup.value" AS "Birth Weight Group",
                    "<28wks/1kg.value" AS "<28wks/1kg",
                    {col("LBWBinary", "Low Birth Weight?")},
                    "OFC.value" AS "Head Circumference (cm)",
                    "AdmReason.label" AS "Admission Reason",
                    "AdmReasonOth.label" AS "Other admission reason",
                    "AgeB.label" AS "AgeB.label",
                    "AgeC.label" AS "AgeC.label",
                    "AgeA.label" AS "AgeA.label",
                    "TypeBirth.label" AS "Type of Birth",
                    "Gestation.value" AS "Gestation",
                    "GestGroup.value" AS "Gestetation Group",
                    "MethodEstGest.label" AS "Method of Gestation Extimation",
                    "Presentation.label" AS "Presentation",
                    "ModeDelivery.label" AS "Mode of Delivery",
                    "MecPresent.label" AS "Meconium Present?",
                    "CryBirth.label" AS "Cry at Birth?",
                    "Colour.label" AS "Baby Colour",
                    "Apgar1.value" AS "Apgar score at 1 min",
                    "Apgar5.value" AS "Apgar score at 5 mins",
                    "Apgar10.value" AS "Apgar score at 10 mins",
                    "Palate.label" AS "Palate",
                    "HeadShape.label" AS "Head Shape",
                    "Dysmorphic.label" AS "Dysmorphic",
                    "Spine.label" AS "Spine",
                    "Activity.label" AS "Activity",
                    "SignsRD.label" AS "Signs of Respiratory Distress",
                    "WOB.label" AS "Work of breathing",
                    "Stethoscope.label" AS "Stethoscope use?",
                    "ChestAusc.label" AS "Chest Auscultation",
                    "Murmur.label" AS "Murmur",
                    "SignsDehydrations.label" AS "Signs of Dehydration",
                    "Abdomen.label" AS "Abdomen",
                    "Umbilicus.label" AS "Umbilicus",
                    "Genitalia.label" AS "Genitalia",
                    "Anus2.label" AS "Anus2",
                    "MSKproblems.label" AS "Musculoskeletal problems",
                    "Skin.label" AS "Skin tone",
                    "BrProbs.label" AS "Breathing Problem",
                    "Vomiting.label" AS "Vomiting",
                    "FeedingReview.label" AS "Feeding Review",
                    "StoolsInfant.label" AS "Stools Infant",
                    "SRNeuroOther.label" AS "SRNeuroOther",
                    "GSCvsOM.label" AS "GSCvsOM",
                    "InOrOut.label" AS "InOrOut",
                    "ReferredFrom2.label" AS "Other Referred From",
                    "ReferredFrom.label" AS "Referred From",
                    "OtherReferralFacility.label" AS "Other Referral Facility",
                    "PlaceBirth.label" AS "Place of Birth",
                    "BirthFacility.label" AS "Birth Facility",
                    "BirthPlaceSame.label" AS "Same birth place?",
                    "OtherBirthFacility.label" AS "Other Birth Facility",
                    "MatPhysAddressDistrict.label" AS "Mothers Disctrict",
                    "MatAgeYrs.value" AS "Mathors Age in years",
                    "MaritalStat.label" AS "Marital Status",
                    "Ethnicity.label" AS "Ethnicity",
                    "Tribe.label" AS "Tribe",
                    "TribeOther.label" AS "Other Tribe",
                    "Religion.label" AS "Religion",
                    "ReligionOther.label" AS "Other Religion",
                    "MatHIVtest.label" AS "HIV test?",
                    "ANVDRL.label" AS "ANVDRL",
                    CASE
                    WHEN "DateHIVtest.value"::TEXT ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                    to_timestamp("DateHIVtest.value", 'DD Mon,YYYY')
                    WHEN "DateHIVtest.value"::TEXT ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                    to_timestamp("DateHIVtest.value", 'YYYY Mon,DD')
                    ELSE NULL
                    END AS "Date of HIV test",
                    "TestThisPreg.label" AS "When HIV test was done",
                    "HIVtestResult.label" AS "HIV test Result",
                    "HAART.label" AS "HAART",
                    "LengthHAART.label" AS "Length of HAART",
                    "NVPgiven.label" AS "NVP given?",
                     CASE
                    WHEN "ANVDRLDate.value"::TEXT ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                    to_timestamp("ANVDRLDate.value", 'DD Mon,YYYY')
                    WHEN "ANVDRLDate.value"::TEXT ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                    to_timestamp("ANVDRLDate.value", 'YYYY Mon,DD')
                    ELSE NULL
                    END AS "ANVDRLDate",
                    "DateVDRLSameHIV.value" AS "Date of VDRL Same as HIV Test Date?",
                    "ANVDRLResult.label" AS "ANVDRL Result",
                    "PregConditions.label" AS "Conditions in Pregnancy",
                    "AntenatalCare.label" AS "Antenatal Care",
                    "ANMatSyphTreat.label" AS "Mataternal Syphillis Treated?",
                    "IPT.label" AS "IPT Taken",
                    "FeFo.label" AS "FeFo",
                    "TTV.label" AS "TTV",
                    "ANSteroids.label" AS "Antenatal Steroids",
                    "ProbsLab.label" AS "Problems in Labor",
                    "DurationLab.label" AS "Duration in Labor",
                    "ROM.label" AS "ROM",
                    "ROMLength.value" AS "ROM Length",
                    "RFSepsis.label" AS "Risk Factors for Sepsis",
                    "Resus.label" AS "Resusitation",
                    "VitK.label" AS "IM vit K given at birth?",
                    "TetraEye.label" AS "TEO given at birth?",
                    "Chlor.label" AS "Chlorhexidine on umbilicus at birth?",
                    "Plan.label" AS "Plan",
                    "PlanOth.label" AS "Other Plan",
                    "RespSR.value" AS "RespSR",
                    "Diagnoses.label" AS "Diagnoses",
                    "DiagnosesOth.label" AS "Other Diagnoses",
                    "DiagnosisSurgicalCond.label" AS "Diagnosis (Surgical Cond)",
                    "AdmReaSurgCond.label" AS "Admission Reason (Surgical Cond)",
                    "AdmittedFrom.label" AS "Admission Source",
                    "PassedMec.label" AS "Meconium?",
                    "PUNewborn.label" AS "Passing Urine?",
                    "PUInfant.label" AS "Passing urine? (infant)",
                    CASE WHEN "SuckTh.label" is null THEN
                    "SuckReflex.label" 
                     ELSE "SuckTh.label" END AS "Suck Reflex",
                    CASE WHEN "FontTh.label" is null THEN "Fontanelle.label" 
                    ELSE "FontTh.label" END AS "Fontanelle",  
                    CASE WHEN "ToneTh.label" is null THEN
                    "Tone.label"  ELSE            
                    "ToneTh.label" END AS "Tone",
                    "LOCTh.label" AS "Level of Conciousness",
                    "FitsTh.label" AS "Fits, Seizures or convulsions",
                    "RespTh.label" AS "Respiration",
                    "ThompScore.label" AS "Thompson Score",
                    "PostTh.label" AS "Posture",
                    "MoroTh.label" AS "Moro reflex",
                    "GraspTh.label" AS "Grasp reflex",
                    "Reason.label" AS "Reason for CS",
                    "ReasonOther.label" AS "Other Reason for CS",
                    "LengthResus.value" AS "Length of Resusitation",
                    "LengthResusKnown.label" AS "Length of Resusitation (Known)",
                    "MecThickThin.label" AS "Meconium Thick or Thin",
                    "CRT.label" AS "Cardiovascular exam",
                    "Femorals.label" AS "Femorals",
                    "HypoSxYN.label" AS "HypoSxYN",
                    {col("ChestAusc", "Chest Ausc")},
                    {col("RespSR", "Respiratory Support")},
                    "RISKCovid.label" AS "RISK for Covid?",
                    "EXTERNALSOURCE.label" AS "External Source",
                    "MatSymptoms.label" AS "Mothers Symptoms",
                    "MothersDiagnosis.label" AS "Mothers Diagnosis",
                    CASE
                    WHEN "MotherSatsO2.value" ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
                    THEN CAST("MotherSatsO2.value" AS DOUBLE PRECISION)
                    ELSE NULL
                    END AS "Mother Oxygen saturations",
                    "MotherPresent.label" AS "is mother present?",
                    "EthnicityOther.label" AS "Other Ethnicity",
                    "ManualHR.label" AS "Manual Heart Rate",
                    "MatComorbidities.label" AS "MatComorbidities",
                    "MatComorbidities.value" AS "MatComorbidities.value",
                    "DOBYN.value" AS "DOBYN.value",
                    "AgeEst.label" AS "Age Estimated",
                    "Age.value" AS "Age",
                    CASE WHEN {col("AgeCat.label", "temp", is_case=True)} is null THEN
                    {col("AgeCategory", "temp", is_case=True)}
                    ELSE {col("AgeCat.label", "temp", is_case=True)} END AS "Age Category",
                    "BirthWeight.value" AS "BirthWeight"
                FROM "derived"."admissions" {where} ;; '''