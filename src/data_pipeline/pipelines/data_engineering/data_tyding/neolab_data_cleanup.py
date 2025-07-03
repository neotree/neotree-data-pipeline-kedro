import logging
import pandas as pd
import sys
from conf.common.format_error import formatError


def neolab_cleanup(df: pd.DataFrame,position):
    
   
    try:
        if "Org1.label" in df.columns:
            if str(df.at[position,"Org1.label"]).lower().strip().find("coagulase negative staph") >-1:
                df.at[position,"Org1.label"] = 'Coagulase negative staphylococcus'
            if df.at[position,"Org1.value"] =='Oth' and "OtherOrg1.value" in df.columns:
                # CONS
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("staphyloc")> -1
                    or str(df.at[position,"OtherOrg1.value"]).lower().find("coagulase negative")>-1
                    or str(df.at[position,"OtherOrg1.value"]).lower().find("stapgylococcus")):
                    df.at[position,"Org1.label"] = 'Coagulase negative staphylococcus'
                    df.at[position,"Org1.value"] = 'CONS'
                    #df.at[position,"OtherOrg1.value"] = None
                # Klebsiella
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("klesiella") > -1
                    or str(df.at[position,"OtherOrg1.value"]).lower().find("klebsiella")>-1
                    or str(df.at[position,"OtherOrg1.value"]).lower().find("kleb")>-1):
                    df.at[position,"Org1.label"] = 'Klebsiella sp.'
                    df.at[position,"Org1.value"] = 'KLS'
                    #df.at[position,"OtherOrg1.value"] = None
                # Streptococcus pyogenes
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("streptococcus pyogenes")> -1
                 or str(df.at[position,"OtherOrg1.value"]).lower().find("streptococcus pygenes")> -1
                 or str(df.at[position,"OtherOrg1.value"]).lower().find("streptococcus pyoges")> -1
                 or str(df.at[position,"OtherOrg1.value"]).lower().find("s payogenes")> -1
                 or str(df.at[position,"OtherOrg1.value"]).lower().find("strptococcus pyogenes")> -1
                 or str(df.at[position,"OtherOrg1.value"]).lower().find("b-haemolytic strep")> -1
                 or str(df.at[position,"OtherOrg1.value"]).lower().find("streptococcus agalactiae")> -1):
                    df.at[position,"Org1.label"] = 'Streptococcus pyogenes (Group A Beta haemolytic Strep)'
                    df.at[position,"Org1.value"] = 'StrepPy'
                    #df.at[position,"OtherOrg1.value"] = None
                #Streptococcus species
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("streptococcus species")> -1
                or str(df.at[position,"OtherOrg1.value"]).lower().find("streptococcus species")> -1):
                    df.at[position,"Org1.label"] = 'Streptococcus sp.'
                    df.at[position,"Org1.value"] = 'StrepSp'
                    #df.at[position,"OtherOrg1.value"] = None

                #Staphylococcus auris
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("s.aureus")> -1):
                    df.at[position,"Org1.label"] = 'Staphylococcus aureus'
                    df.at[position,"Org1.value"] = 'SA'
                    #df.at[position,"OtherOrg1.value"] = None

                # Citrobacter
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("citrobacter")> -1
                or str(df.at[position,"OtherOrg1.value"]).lower().find("citribacter")>-1):
                    df.at[position,"Org1.label"] = 'Citrobacter sp.'
                    df.at[position,"Org1.value"] = 'Cit'
                    #df.at[position,"OtherOrg1.value"] = None

                # Proteus
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("proteus")> -1
                or str(df.at[position,"OtherOrg1.value"]).lower().find("ptoteus")> -1):
                    df.at[position,"Org1.label"] = 'Proteus sp.'
                    df.at[position,"Org1.value"] = 'Prot'
                    #df.at[position,"OtherOrg1.value"] = None

                # Yeasts excluding candida albicans
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("yeasts excluding candida albicans")> -1
                or str(df.at[position,"OtherOrg1.value"]).lower().find("yeasts")> -1):
                    df.at[position,"Org1.label"] = 'Yeasts (excluding candida)'
                    df.at[position,"Org1.value"] = 'Yea'
                    #df.at[position,"OtherOrg1.value"] = None

                # Enterobacter
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("enterobacter")> -1):
                    df.at[position,"Org1.label"] ='Enterobacter sp.'
                    df.at[position,"Org1.value"] = 'Ent'
                    #df.at[position,"OtherOrg1.value"] = None

                # Group D streptococcus species
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("group d")> -1):
                    df.at[position,"Org1.label"] ='Group D Strep'
                    df.at[position,"Org1.value"] = 'GDS'
                    #df.at[position,"OtherOrg1.value"] = None

                # Non-haemolytic strep
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("non-haemolytic strep")> -1):
                    df.at[position,"Org1.label"] ='Non haemolytic streptococcus'
                    df.at[position,"Org1.value"] = 'NHS'
                    #df.at[position,"OtherOrg1.value"] = None
                # Non-lactose fermenter
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("non-haemolytic strep")> -1):
                    df.at[position,"Org1.label"] ='Non-lactose fermenting coliform'
                    df.at[position,"Org1.value"] = 'NLFC'
                    #df.at[position,"OtherOrg1.value"] = None

                # Pseudomonas aeruginosa
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("pseudomonas")> -1):
                    df.at[position,"Org1.label"] ='Pseudomonas aeruginosa'
                    df.at[position,"Org1.value"] = 'Pseud'
                    #df.at[position,"OtherOrg1.value"] = None

                # Viridans Streptococci
                if (str(df.at[position,"OtherOrg1.value"]).lower().find("viridans")> -1):
                    df.at[position,"Org1.label"] ='Viridans streptococcus'
                    df.at[position,"Org1.value"] = 'VirSt'
                    #df.at[position,"OtherOrg1.value"] = None




            else:
                # Remove All White Spaces
               df.at[position,"Org1.label"] = str(df.at[position,"Org1.label"]).strip()

    except Exception as ex:
        logging.error("Something Happened Cleaning Up Neolab")
        logging.error(formatError(ex))
        sys.exit(1)



