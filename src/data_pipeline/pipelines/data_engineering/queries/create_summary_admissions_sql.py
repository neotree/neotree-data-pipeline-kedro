
#Query to create summary_maternala_completeness table
def summary_admissions_query():
  return   f''' DROP TABLE IF EXISTS derived.summary_admissions;
                CREATE TABLE derived.summary_admissions AS 
                SELECT "facility" AS "Facility Name",
                    "uid" AS "NeoTree_ID",
                    "DateTimeAdmission.value" AS "DateTime Admission",
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
                    "BW.value" AS "Birth Weight",
                    "BWGroup.value" AS "Birth Weight Group",
                    "BW .value" AS "BW .value",
                    "<28wks/1kg.value" AS "<28wks/1kg",
                    "LBWBinary" AS "Low Birth Weight?",
                    "OFC.value" AS "Head Circumference (cm)",
                    "AdmReason.label" AS "Admission Reason",
                    "AdmReasonOth.label" AS "Other admission reason",
                    "AgeCat.label" AS "Age Category",
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
                    "SuckReflex.label" AS "Suck Reflex",
                    "Palate.label" AS "Palate",
                    "Fontanelle.label" AS "Fontanelle",
                    "HeadShape.label" AS "Head Shape",
                    "Dysmorphic.label" AS "Dysmorphic",
                    "Tone.label" AS "Tone",
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
                    "PlaceBirth.label" AS "Place of Birth",
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
                    "DateHIVtest.value" AS "Date of HIV test",
                    "TestThisPreg.label" AS "When HIV test was done",
                    "HIVtestResult.label" AS "HIV test Result",
                    "HAART.label" AS "HAART",
                    "LengthHAART.label" AS "Length of HAART",
                    "NVPgiven.label" AS "NVP given?",
                    "ANVDRLDate.value" AS "ANVDRLDate",
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
                    "SuckTh.label" AS "Suck Reflex",
                    "FontTh.label" AS "Fontanelle",
                    "ToneTh.label" AS "Tone",
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
                    "ChestAusc" AS "Chest Ausc",
                    "RespSR" AS "Respiratory Support",
                    "RISKCovid.label" AS "RISK for Covid?",
                    "EXTERNALSOURCE.label" AS "External Source",
                    "MatSymptoms.label" AS "Mothers Symptoms",
                    "MothCell.value" AS "Mother Cellphone number",
                    "MothersDiagnosis.label" AS "Mothers Diagnosis",
                    "MotherSatsO2.value" AS "Mother Oxygen saturations",
                    "MotherPresent.label" AS "is mother present?",
                    "EthnicityOther.label" AS "Other Ethnicity",
                    "ManualHR.label" AS "Manual Heart Rate",
                    "MatComorbidities.label" AS "MatComorbidities",
                    "MatComorbidities.value" AS "MatComorbidities.value",
                    "DOBYN.value" AS "DOBYN.value",
                    "AgeEst.label" AS "Age Estimated",
                    "Age.value" AS "Age",
                    "AgeCategory" AS "Age Category",
                    "BirthWeight.value" AS "BirthWeight"
                FROM "derived"."admissions" '''