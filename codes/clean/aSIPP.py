#! python3

"""
This do file cleans four SIPP panels: 1996, 2001, 2004, and 2008. 

In particular, 
    id-relevant information is generated,
    demographic information is generated, and sample restriction is conducted,
    monthly employment status is generated,
    three types of unemployment spells of interest (ubar, ustar, u) are marked,
    the source and destination occupations' raw information is collected, 
    individual probability weights are calculated.

Input:
    dta files stored in "rawdata" folder.
Output:
    tempdata/temp1996.dta
    tempdata/temp2001.dta
    tempdata/temp2004.dta
    tempdata/temp2008.dta

Wang Wenzhi 
Time: 2024-10-19
"""

# ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
# ?? Code Block 1. Function to Clean SIPP Datasets
# ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??


def SIPP_cleaning(codes_path: str, panel: int) -> None:
    """
    This function wraps all data cleaning procedures into a function, taking
    SIPP panel year and codes_path as arguments, generates a .dta file --
    temp`panel'.dta, and stores the resulting dat file in the tempdata folder.

    Version: 2024-10-19
    """

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 0. import necessary packages
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-0-1. Functions for data paths (stored in codes/util/paths.py)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    import sys

    sys.path.append(codes_path)
    from util.paths import rawdata, tempdata

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-0-2. Dictionaries for value labels
    # -? (stored in codes/util/labels.py)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    from util.labels import val_labs

    # print(val_labs["empl"])  # test value labels

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-0-3. Functions for DataFrame manipulation
    # -? (stored in codes/util/funcsforpandas.py)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    from util.funcsforpandas import df_order

    # help(df_order)  # test functions

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-0-4. Other necessary packages
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    import pandas as pd
    import numpy as np

    pd.set_option("mode.copy_on_write", True)

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 1. construct monthly employment status
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-1-1. load the full dataset and remain only relevant variables
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    if panel == 1996:
        sipp96w1 = pd.read_stata(
            rawdata("sipp96w1.dta"), convert_categoricals=False
        )
        sipp96w2 = pd.read_stata(
            rawdata("sipp96w2.dta"), convert_categoricals=False
        )
        sipp96w3 = pd.read_stata(
            rawdata("sipp96w3.dta"), convert_categoricals=False
        )
        sipp96w4 = pd.read_stata(
            rawdata("sipp96w4.dta"), convert_categoricals=False
        )
        sipp96w5 = pd.read_stata(
            rawdata("sipp96w5.dta"), convert_categoricals=False
        )
        sipp96w6 = pd.read_stata(
            rawdata("sipp96w6.dta"), convert_categoricals=False
        )
        sipp96w7 = pd.read_stata(
            rawdata("sipp96w7.dta"), convert_categoricals=False
        )
        sipp96w8 = pd.read_stata(
            rawdata("sipp96w8.dta"), convert_categoricals=False
        )
        sipp96w9 = pd.read_stata(
            rawdata("sipp96w9.dta"), convert_categoricals=False
        )
        sipp96w10 = pd.read_stata(
            rawdata("sipp96w10.dta"), convert_categoricals=False
        )
        sipp96w11 = pd.read_stata(
            rawdata("sipp96w11.dta"), convert_categoricals=False
        )
        sipp96w12 = pd.read_stata(
            rawdata("sipp96w12.dta"), convert_categoricals=False
        )

        sipp = pd.concat(
            [
                sipp96w1,
                sipp96w2,
                sipp96w3,
                sipp96w4,
                sipp96w5,
                sipp96w6,
                sipp96w7,
                sipp96w8,
                sipp96w9,
                sipp96w10,
                sipp96w11,
                sipp96w12,
            ],
            axis=0,
        )
    elif panel == 2001:
        sipp01w1 = pd.read_stata(
            rawdata("sipp01w1.dta"), convert_categoricals=False
        )
        sipp01w2 = pd.read_stata(
            rawdata("sipp01w2.dta"), convert_categoricals=False
        )
        sipp01w3 = pd.read_stata(
            rawdata("sipp01w3.dta"), convert_categoricals=False
        )
        sipp01w4 = pd.read_stata(
            rawdata("sipp01w4.dta"), convert_categoricals=False
        )
        sipp01w5 = pd.read_stata(
            rawdata("sipp01w5.dta"), convert_categoricals=False
        )
        sipp01w6 = pd.read_stata(
            rawdata("sipp01w6.dta"), convert_categoricals=False
        )
        sipp01w7 = pd.read_stata(
            rawdata("sipp01w7.dta"), convert_categoricals=False
        )
        sipp01w8 = pd.read_stata(
            rawdata("sipp01w8.dta"), convert_categoricals=False
        )
        sipp01w9 = pd.read_stata(
            rawdata("sipp01w9.dta"), convert_categoricals=False
        )
        sipp = pd.concat(
            [
                sipp01w1,
                sipp01w2,
                sipp01w3,
                sipp01w4,
                sipp01w5,
                sipp01w6,
                sipp01w7,
                sipp01w8,
                sipp01w9,
            ],
            axis=0,
        )
    elif panel == 2004:
        sipp04w1 = pd.read_stata(
            rawdata("sipp04w1.dta"), convert_categoricals=False
        )
        sipp04w2 = pd.read_stata(
            rawdata("sipp04w2.dta"), convert_categoricals=False
        )
        sipp04w3 = pd.read_stata(
            rawdata("sipp04w3.dta"), convert_categoricals=False
        )
        sipp04w4 = pd.read_stata(
            rawdata("sipp04w4.dta"), convert_categoricals=False
        )
        sipp04w5 = pd.read_stata(
            rawdata("sipp04w5.dta"), convert_categoricals=False
        )
        sipp04w6 = pd.read_stata(
            rawdata("sipp04w6.dta"), convert_categoricals=False
        )
        sipp04w7 = pd.read_stata(
            rawdata("sipp04w7.dta"), convert_categoricals=False
        )
        sipp04w8 = pd.read_stata(
            rawdata("sipp04w8.dta"), convert_categoricals=False
        )
        sipp04w9 = pd.read_stata(
            rawdata("sipp04w9.dta"), convert_categoricals=False
        )
        sipp04w10 = pd.read_stata(
            rawdata("sipp04w10.dta"), convert_categoricals=False
        )
        sipp04w11 = pd.read_stata(
            rawdata("sipp04w11.dta"), convert_categoricals=False
        )
        sipp04w12 = pd.read_stata(
            rawdata("sipp04w12.dta"), convert_categoricals=False
        )
        sipp = pd.concat(
            [
                sipp04w1,
                sipp04w2,
                sipp04w3,
                sipp04w4,
                sipp04w5,
                sipp04w6,
                sipp04w7,
                sipp04w8,
                sipp04w9,
                sipp04w10,
                sipp04w11,
                sipp04w12,
            ],
            axis=0,
        )
    elif panel == 2008:
        sipp08w1 = pd.read_stata(
            rawdata("sipp08w1.dta"), convert_categoricals=False
        )
        sipp08w2 = pd.read_stata(
            rawdata("sipp08w2.dta"), convert_categoricals=False
        )
        sipp08w3 = pd.read_stata(
            rawdata("sipp08w3.dta"), convert_categoricals=False
        )
        sipp08w4 = pd.read_stata(
            rawdata("sipp08w4.dta"), convert_categoricals=False
        )
        sipp08w5 = pd.read_stata(
            rawdata("sipp08w5.dta"), convert_categoricals=False
        )
        sipp08w6 = pd.read_stata(
            rawdata("sipp08w6.dta"), convert_categoricals=False
        )
        sipp08w7 = pd.read_stata(
            rawdata("sipp08w7.dta"), convert_categoricals=False
        )
        sipp08w8 = pd.read_stata(
            rawdata("sipp08w8.dta"), convert_categoricals=False
        )
        sipp08w9 = pd.read_stata(
            rawdata("sipp08w9.dta"), convert_categoricals=False
        )
        sipp08w10 = pd.read_stata(
            rawdata("sipp08w10.dta"), convert_categoricals=False
        )
        sipp08w11 = pd.read_stata(
            rawdata("sipp08w11.dta"), convert_categoricals=False
        )
        sipp08w12 = pd.read_stata(
            rawdata("sipp08w12.dta"), convert_categoricals=False
        )
        sipp08w13 = pd.read_stata(
            rawdata("sipp08w13.dta"), convert_categoricals=False
        )
        sipp08w14 = pd.read_stata(
            rawdata("sipp08w14.dta"), convert_categoricals=False
        )
        sipp08w15 = pd.read_stata(
            rawdata("sipp08w15.dta"), convert_categoricals=False
        )
        sipp08w16 = pd.read_stata(
            rawdata("sipp08w16.dta"), convert_categoricals=False
        )
        sipp = pd.concat(
            [
                sipp08w1,
                sipp08w2,
                sipp08w3,
                sipp08w4,
                sipp08w5,
                sipp08w6,
                sipp08w7,
                sipp08w8,
                sipp08w9,
                sipp08w10,
                sipp08w11,
                sipp08w12,
                sipp08w13,
                sipp08w14,
                sipp08w15,
                sipp08w16,
            ],
            axis=0,
        )

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-1-2. relevant variables
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    vars_id = [
        "lgtkey",
        "rhcalmn",
        "rhcalyr",
        "swave",
        "ssuid",
        "eentaid",
        "epppnum",
        "srotaton",
    ]

    vars_demogr = [
        "tbyear",
        "ebmnth",
        "esex",
        "ems",
        "eeducate",
        "eafnow",
        "eafever",
        "erace",
        "ebuscntr",
        "ebno1",
        "ebno2",
        "eppintvw",
    ]

    vars_emp = [
        "rmesr",
        "rwkesr1",
        "rwkesr2",
        "rwkesr3",
        "rwkesr4",
        "rwkesr5",
        "ersend1",
        "ersend2",
        "ersnowrk",
    ]

    vars_occ = [
        "eeno1",
        "eeno2",
        "tsjdate1",
        "tsjdate2",
        "tejdate1",
        "tejdate2",
        "ejbhrs1",
        "ejbhrs2",
        "tpmsum1",
        "tpmsum2",
        "eclwrk1",
        "eclwrk2",
        "tjbocc1",
        "ajbocc1",
        "tjbocc2",
        "ajbocc2",
    ]

    vars_earn = ["tpearn", "tptrninc", "tptotinc", "tpothinc", "tpprpinc"]

    vars_wgt = ["wpfinwgt"]

    vars_all = (
        vars_id + vars_demogr + vars_emp + vars_occ + vars_earn + vars_wgt
    )

    temp = sipp[vars_all]

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 2. process vars_id
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    panel_year = panel

    # -? s-2-1 panel information
    temp.loc[:, "panel"] = panel_year
    temp["panel"] = temp["panel"].astype("Int64")

    # -? s-2-2 individual id: add panel prefix to distinguish with other panels' id
    temp["panel_str"] = temp["panel"].astype(str)
    temp["indid"] = (temp["panel_str"] + temp["lgtkey"]).astype("Int64")

    temp = temp.drop(columns=["panel_str"], inplace=False)

    # -? s-2-3 date information
    temp = temp.rename(columns={"rhcalmn": "month", "rhcalyr": "year"})
    temp["ym"] = pd.to_datetime(
        temp["year"].astype(str) + "-" + temp["month"].astype(str),
        format="%Y-%m",
    )

    # -? s-2-4 occurrence counts
    temp["occurrence"] = temp.groupby("indid").cumcount() + 1

    # -? s-2-5 order and sort
    cols_first = (
        "panel",
        "swave",
        "year",
        "month",
        "ym",
        "indid",
        "occurrence",
    )
    temp = df_order(temp, cols_first)
    temp = temp.sort_values(by=["indid", "ym"])

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 3. process vars_demogr and do sample restrictions
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-0. ambiguous interview status: "eppintvw"
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["eppintvw"].value_counts(dropna=False)

    # impt drop observations with ambiguous interview status
    temp = temp.loc[(temp["eppintvw"].isin([1, 2])), :]

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-1. self-employment
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["selfemp"] = np.nan
    temp["selfemp"] = temp["selfemp"].astype("Int64")
    temp.loc[(temp["ebuscntr"] >= 1), "selfemp"] = 1
    temp.loc[
        ((temp["ebno1"] != -1) | (temp["ebno2"] != -1)),
        "selfemp",
    ] = 1
    temp.loc[(temp["selfemp"].isna()), "selfemp"] = 0

    temp["ind_selfemp"] = temp.groupby("indid")["selfemp"].transform("max")

    # impt: drop individuals who have ever been self-employed
    temp = temp.loc[(temp["ind_selfemp"] == 0), :]

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-2. age
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["age"] = np.nan
    temp["age"] = temp["age"].astype("Int64")
    temp["age"] = temp["year"] - temp["tbyear"]

    # impt: drop observations who are too young or too old at interview
    temp = temp.loc[(temp["age"].between(18, 65)), :]

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-3. ever in the armed force
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["armed"] = np.nan
    temp["armed"] = temp["armed"].astype("Int64")
    temp.loc[(temp["eafever"].isin([-1])), "armed"] = -1
    temp.loc[(temp["eafever"].isin([1])), "armed"] = 1
    temp.loc[(temp["eafever"].isin([2])), "armed"] = 0

    temp["ind_armed"] = temp.groupby("indid")["armed"].transform("max")

    # impt: drop observations who have been in the armed force
    temp = temp.loc[(temp["ind_armed"] == 0), :]

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-4. education
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["edu"] = np.nan
    temp["edu"] = temp["edu"].astype("Int64")
    temp.loc[(temp["eeducate"].isin([-1])), "edu"] = -1
    temp.loc[(temp["eeducate"].between(31, 38)), "edu"] = 1
    temp.loc[(temp["eeducate"].isin([39])), "edu"] = 2
    temp.loc[(temp["eeducate"].between(40, 43)), "edu"] = 3
    temp.loc[(temp["eeducate"] == 44), "edu"] = 4
    temp.loc[(temp["eeducate"].between(45, 47)), "edu"] = 5

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-5. race
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["race"] = np.nan
    temp["race"] = temp["race"].astype("Int64")
    temp.loc[(temp["erace"].isin([1])), "race"] = 1
    temp.loc[(temp["erace"].isin([2])), "race"] = 2
    temp.loc[(temp["erace"].isin([3, 4])), "race"] = 3

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-6. male
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["male"] = np.nan
    temp["male"] = temp["male"].astype("Int64")
    temp.loc[(temp["esex"] == 1), "male"] = 1
    temp.loc[(temp["esex"] == 2), "male"] = 0

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-3-7. drop, order, and sort variables
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp = temp.drop(
        columns=[
            "esex",
            "eeducate",
            "eafnow",
            "eafever",
            "erace",
            "ebuscntr",
            "ebno1",
            "ebno2",
        ]
    )

    cols_first = (
        "panel",
        "swave",
        "year",
        "month",
        "ym",
        "indid",
        "occurrence",
        "tbyear",
        "age",
        "male",
        "edu",
        "ems",
    )
    temp = df_order(temp, cols_first)
    temp = temp.sort_values(by=["indid", "ym"])

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 4. process vars_emp
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-4-1. employment status (based on monthly variables)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    temp["mn_empl"] = np.nan
    temp["mn_empl"] = temp["mn_empl"].astype("Int64")
    temp.loc[(temp["rmesr"].isin([1, 2, 3, 4, 5])), "mn_empl"] = 1
    temp.loc[(temp["mn_empl"].isna()), "mn_empl"] = 0

    temp["mn_unempl"] = np.nan
    temp["mn_unempl"] = temp["mn_unempl"].astype("Int64")
    temp.loc[(temp["rmesr"].isin([6, 7])), "mn_unempl"] = 1
    temp.loc[(temp["mn_unempl"].isna()), "mn_unempl"] = 0

    temp["mn_outlf"] = np.nan
    temp["mn_outlf"] = temp["mn_outlf"].astype("Int64")
    temp.loc[(temp["rmesr"] == 8), "mn_outlf"] = 1
    temp.loc[(temp["mn_outlf"].isna()), "mn_outlf"] = 0

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-4-2. employment status (based on monthly and weekly variables)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!
    # !! s-4-2-1. employment
    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!
    # &? There are two cases for our definition of employment.
    # &? Case 1. temp["rmesr"].isin([1, 2, 3]
    # &?      "With job all month, ..."
    # &? Case 2. temp["rwkesr2"].isin([1, 2, 3])
    # &?      "With job/bus, ..."
    # &? That is, if the monthly variable says he has a job all month or if the
    # &? week 2 weekly variable says he has a job all week, he is employed.
    temp["empl"] = np.nan
    temp["empl"] = temp["empl"].astype("Int64")
    temp.loc[
        ((temp["rmesr"].isin([1, 2, 3])) | (temp["rwkesr2"].isin([1, 2, 3]))),
        "empl",
    ] = 1
    temp.loc[(temp["empl"].isna()), "empl"] = 0

    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!
    # !! s-4-2-2. unemployment
    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!

    # !! First, create a set of leads of lags for construction of "unempl"
    for var in [
        "rmesr",
        "rwkesr1",
        "rwkesr2",
        "rwkesr3",
        "rwkesr4",
        "rwkesr5",
    ]:
        temp[var] = temp[var].astype("Int64")

    temp = temp.sort_values(by=["indid", "ym"])

    # &? Generate shifted ([_n]=[_n-1]) observations for the following variables
    # &? ["indid", "ym", "rwkesr2", "rwkesr3", "rwkesr4", "rwkesr5"]
    # &? Identification of unemployment will need these last-occurrence values.
    temp["indid_lstoccur"] = temp["indid"].shift(periods=1)
    temp["ym_lstoccur"] = temp["ym"].shift(periods=1)
    temp["rwkesr2_lstoccur"] = temp["rwkesr2"].shift(periods=1)
    temp["rwkesr3_lstoccur"] = temp["rwkesr3"].shift(periods=1)
    temp["rwkesr4_lstoccur"] = temp["rwkesr4"].shift(periods=1)
    temp["rwkesr5_lstoccur"] = temp["rwkesr5"].shift(periods=1)

    # &? Generate shifted ([_n]=[_n+1]) observations for the following variables
    # &? ["indid", "ym", "empl"]
    # &? Identification of unemployment will need these next-occurrence values.
    temp["empl_nxtoccur"] = temp["empl"].shift(periods=-1)
    temp["indid_nxtoccur"] = temp["indid"].shift(periods=-1)
    temp["ym_nxtoccur"] = temp["ym"].shift(periods=-1)

    # !! There are four cases for our definition of unemployment.
    temp["unempl"] = np.nan
    temp["unempl"] = temp["unempl"].astype("Int64")

    # !! Case 1. rwkesr2==4 and rmesr!=1,2,3
    # &? week 2: ("No job/bus - looking for work or on layoff")
    # &?  & monthly: ~("With job all month...")
    # &? That is, the monthly variable doesn't say he has a job the whole month and
    # &? the week 2 weekly variable says he is looking for a job that week, then he
    # &? is defined as unemployed.
    temp.loc[
        ((~temp["rmesr"].isin([1, 2, 3])) & (temp["rwkesr2"] == 4)),
        "unempl",
    ] = 1

    # !! Case 2. rwkesr2==5 and rwkesr1==4 and rmesr!=1,2,3
    # &? week 2: ("No job/bus - not looking and not on layoff")
    # &? & week 1: ("No job/bus - looking for work or on layoff")
    # &? & monthly: ~("With job all month...")
    # &? That is, the monthly variable doesn't say he has a job the whole month and
    # &? the week 2 weekly variable says he is out of labor force, and the week 1
    # &? weekly variable says he is looking for a job that week, then he is defined
    # &? as unemployed.
    temp.loc[
        (
            (~temp["rmesr"].isin([1, 2, 3]))
            & (temp["rwkesr2"] == 5)
            & (temp["rwkesr1"] == 4)
        ),
        "unempl",
    ] = 1

    # !! Case 3.
    # &? rwkesr2==5
    # &? and (rwkesr5[_n-1]==4|rwkesr4[_n-1]==4|rwkesr3[_n-1]==4|rwkesr2[_n-1]==4)
    # &? and rmesr!=1,2,3
    # &? week 2: ("No job/bus - not looking and not on layoff")
    # &? & last month any week: ("No job/bus - looking for work or on layoff")
    # &? & monthly: ~("With job all month...")
    # &? That is, the monthly variable doesn't say he has a job the whole month and
    # &? the week 2 weekly variable says he is out of labor force, and at any week
    # &? last month, he is looking for a job, then he is defined as unemployed.
    temp.loc[
        (
            (temp["ym"] - pd.DateOffset(months=1) == temp["ym_lstoccur"])
            & (temp["indid"] == temp["indid_lstoccur"])
            & (~temp["rmesr"].isin([1, 2, 3]))
            & (temp["rwkesr2"] == 5)
            & (
                (temp["rwkesr2_lstoccur"] == 4)
                | (temp["rwkesr3_lstoccur"] == 4)
                | (temp["rwkesr4_lstoccur"] == 4)
                | (temp["rwkesr5_lstoccur"] == 4)
            )
        ),
        "unempl",
    ] = 1

    # !! Case 4.
    # &? This is a special case of Case 3 to adjust for the first occurrence of an
    # &? individual. In particular, if this is an individual's first occurrence,
    # &? and rwkesr2==5 and rmesr!=1,2,3 and empl[_n+1]==1 (out of labor force this
    # &? month, but employed next month), then he is defined as unemployed.
    temp.loc[
        (
            (temp["indid"] == temp["indid_nxtoccur"])
            & (temp["indid"] != temp["indid_lstoccur"])
            & (temp["ym"] + pd.DateOffset(months=1) == temp["ym_nxtoccur"])
            & (temp["ym"] != temp["ym_lstoccur"] + pd.DateOffset(months=1))
            & (~temp["rmesr"].isin([1, 2, 3]))
            & (temp["rwkesr2"] == 5)
            & (temp["empl_nxtoccur"] == 1)
        ),
        "unempl",
    ] = 1

    # !! Residual case.
    temp.loc[(temp["unempl"].isna()), "unempl"] = 0

    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!
    # !! s-4-2-3. out of and in the labor force
    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!
    temp["outlf"] = 0
    temp["outlf"] = temp["outlf"].astype("Int64")
    temp.loc[((temp["empl"] == 0) & (temp["unempl"] == 0)), "outlf"] = 1

    temp["inlf"] = 0
    temp["inlf"] = temp["inlf"].astype("Int64")
    temp.loc[((temp["empl"] == 1) | (temp["unempl"] == 1)), "inlf"] = 1

    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!
    # !! s-4-2-5. retirement and entry into labor force
    # !!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!#!!

    # &? There are three cases for retirement.
    temp["retired"] = 0
    temp["retired"] = temp["retired"].astype("Int64")

    # &? Case 1. He declares he is not working because of retirement.
    temp.loc[((temp["ersnowrk"] == 4) & (temp["outlf"] == 1)), "retired"] = 1

    # &? Case 2. He declares quitting last job due to retirement.
    temp.loc[
        (
            (temp["empl"].shift(1) == 1)
            & (temp["outlf"] == 1)
            & ((temp["ersend1"] == 2) | temp["ersend2"] == 2)
        ),
        "retired",
    ] = 1

    # &? Case 3. In earlier months, he has declared to be retired.
    first_retired_month = (
        temp[temp["retired"] == 1].groupby("indid")["ym"].min().reset_index()
    )

    first_retired_month = first_retired_month.rename(
        columns={"ym": "first_retired_month"}
    )
    temp = temp.merge(
        first_retired_month,
        on="indid",
        how="left",
        validate="many_to_one",
        indicator=True,
    )

    temp.loc[(temp["ym"] >= temp["first_retired_month"]), "retired"] = 1
    temp = temp.drop(columns=["_merge", "first_retired_month"])

    # &? Modify employment status according to retirement
    temp.loc[temp["retired"] == 1, "empl"] = 0
    temp.loc[temp["retired"] == 1, "unempl"] = 0
    temp.loc[temp["retired"] == 1, "outlf"] = 1
    temp.loc[temp["retired"] == 1, "inlf"] = 0

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-4-3. government employees
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp["gov1"] = np.nan
    temp["gov2"] = np.nan
    temp["gov"] = np.nan
    temp["ind_gov"] = np.nan
    temp["gov1"] = temp["gov1"].astype("Int64")
    temp["gov2"] = temp["gov2"].astype("Int64")
    temp["gov"] = temp["gov"].astype("Int64")
    temp["ind_gov"] = temp["ind_gov"].astype("Int64")

    temp.loc[
        ((temp["eclwrk1"].isin([3, 4, 5])) & (temp["empl"] == 1)),
        "gov1",
    ] = 1
    temp.loc[
        ((temp["eclwrk2"].isin([3, 4, 5])) & (temp["empl"] == 1)),
        "gov2",
    ] = 1
    temp.loc[((temp["gov1"] == 1) | (temp["gov2"] == 1)), "gov"] = 1
    temp["ind_gov"] = temp.groupby("indid")["gov"].transform("max")

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-4-4. drop, order, and sort columns
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp = temp.drop(
        columns=[
            "indid_lstoccur",
            "indid_nxtoccur",
            "ym_lstoccur",
            "ym_nxtoccur",
            "rwkesr2_lstoccur",
            "rwkesr3_lstoccur",
            "rwkesr4_lstoccur",
            "rwkesr5_lstoccur",
            "empl_nxtoccur",
            "gov1",
            "gov2",
            "gov",
        ]
    )

    cols_first = (
        "indid",
        "occurrence",
        "ym",
        "empl",
        "unempl",
        "outlf",
        "panel",
        "swave",
        "year",
        "month",
        "inlf",
        "retired",
        "mn_empl",
        "mn_unempl",
        "mn_outlf",
        "age",
        "tbyear",
        "male",
        "edu",
        "ems",
    )
    temp = df_order(temp, cols_first)

    temp = temp.sort_values(by=["indid", "ym"])

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 5. mark spells of interest
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-5-1. discontinuous spell (point indicator)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp = temp.sort_values(by=["indid", "ym"])

    temp["next_ym"] = temp.groupby("indid")["ym"].shift(-1)
    temp["next_indid"] = temp["indid"].shift(-1)

    # &? Under the same "indid", if next occurrence month is not next calendar
    # &? month, then it marks the starting point of a discontinuous spell.
    temp["disc_spell"] = np.where(
        (
            (
                (temp["next_indid"] == temp["indid"])
                & (temp["next_indid"].notna())
            )
            & (
                (temp["next_ym"] != temp["ym"] + pd.DateOffset(months=1))
                & (temp["next_ym"].notna())
            )
        ),
        1,
        0,
    )

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-5-2. continuous spell number (spell index)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    # &? This variable documents which continuous spell the individual is in.
    temp["cont_spell_no"] = temp.groupby("indid")["disc_spell"].cumsum()
    temp["cont_spell_no"] = temp["cont_spell_no"] + 1

    # &? Adjust for the last period of a spell (which drops into the next spell).
    temp.loc[temp["disc_spell"] == 1, "cont_spell_no"] = (
        temp["cont_spell_no"] - 1
    )

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-5-3. number of months in that spell
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp["len_cont_spell"] = temp.groupby(["indid", "cont_spell_no"])[
        "indid"
    ].transform("size")

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-5-4. drop, order, and sort
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp = temp.drop(columns=["next_ym", "next_indid"])

    temp = df_order(
        temp,
        (
            "indid",
            "ym",
            "cont_spell_no",
            "len_cont_spell",
            "empl",
            "unempl",
            "outlf",
        ),
    )

    temp = temp.sort_values(by=["indid", "ym"])

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 6. construct different types of EUE spell
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-6-1. e_to_ubar and ubar_to_e under ubar (generalized unemployment)
    # -? if the individual is not employed, he can be defined as ubar
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    # &? ubar is one notion of unemployment used in this project:
    # &? If the person is not employed in a month, regardless of whether he has
    # &? search activities, he is defined as ubar.

    # &? Notice that the person needs to be re-employed so that I can assess his
    # &? occupational mobility states, so, in principle, the unemployment spells
    # &? defined by this notion should at least contain some search activities for
    # &? reemployment. So it is not that bad.

    temp["nxt_empl"] = temp.groupby(["indid", "cont_spell_no"])["empl"].shift(
        -1
    )
    temp["lst_empl"] = temp.groupby(["indid", "cont_spell_no"])["empl"].shift(
        1
    )

    # &? For empl==1 to empl==0 transition inside an indid-cont_spell_no cell,
    # &? start_of_ubar is flagged as 1 at the start of the unemployment month,
    # &? month when empl==0.
    temp["start_of_ubar"] = 0
    temp["start_of_ubar"] = temp["start_of_ubar"].astype("Int64")
    temp.loc[
        ((temp["lst_empl"] == 1) & (temp["empl"] == 0)),
        "start_of_ubar",
    ] = 1

    # &? For empl==0 to empl==1 transition inside an indid-cont_spell_no cell,
    # &? end_of_ubar is flagged as 1 at the end of the unemployment month,
    # &? month when empl==0.
    temp["end_of_ubar"] = 0
    temp["end_of_ubar"] = temp["end_of_ubar"].astype("Int64")
    temp.loc[
        ((temp["nxt_empl"] == 1) & (temp["empl"] == 0)),
        "end_of_ubar",
    ] = 1

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-6-2. give id to continuous unemployment spells ("ubar_spell_no")
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp["temp_period_id"] = (
        temp.groupby(["indid", "cont_spell_no"])["start_of_ubar"].cumsum() + 1
    )
    temp.loc[(temp["empl"] == 1), "temp_period_id"] = np.nan

    temp["ubar_spell_no"] = (
        temp.groupby(["indid", "cont_spell_no", "temp_period_id"]).ngroup() + 1
    )
    temp["ubar_spell_no"] = temp["ubar_spell_no"].astype("Int64")

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-6-3. modify "ubar_spell_no" - non-missing only for E(UBAR)E spells
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    # &?? For a continuous unemployment spell index by "ubar_spell_no", if it has
    # &?? both the "start_of_ubar" and the "end_of_ubar" indicator, then it is
    # &?? definitely an E(UBAR)E spell.

    temp["spellmax_start_of_ubar"] = temp.groupby("ubar_spell_no")[
        "start_of_ubar"
    ].transform("max")
    temp["spellmax_end_of_ubar"] = temp.groupby("ubar_spell_no")[
        "end_of_ubar"
    ].transform("max")

    # &? Set ubar_spell_no for other continuous unemployment spells.
    temp.loc[
        (
            (temp["spellmax_start_of_ubar"] == 0)
            | (temp["spellmax_end_of_ubar"] == 0)
        ),
        "ubar_spell_no",
    ] = np.nan
    # && 14,131 E(UBAR)E spells left

    # !! length of a ubar spell
    temp["len_ubar_spell"] = temp.groupby("ubar_spell_no")[
        "ubar_spell_no"
    ].transform("count")

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-6-4. identify E(USTAR)E spells
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    # &? ustar is another notion of unemployment used in this project:
    # &? For an nonemployment spell, if the worker reports at least one month of
    # &? unemployment, i.e., search activities, then this spell is called ustar.

    # &?? For a continuous nonemployment spell index by "ubar_spell_no", if it has
    # &?? one month with unempl==1, then it is an E(USTAR)E spell.
    temp["spellmax_unempl"] = temp.groupby("ubar_spell_no")[
        "unempl"
    ].transform("max")

    temp["ustar_spell_no"] = temp["ubar_spell_no"]
    temp.loc[(temp["spellmax_unempl"] == 0), "ustar_spell_no"] = np.nan

    temp["spellmax_unempl"].value_counts()

    # !! length of a ubar spell
    temp["len_ustar_spell"] = temp.groupby("ustar_spell_no")[
        "ustar_spell_no"
    ].transform("count")

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-6-4. identify E(U)E spells
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    # &? u is another notion of unemployment used in this project:
    # &? For an nonemployment spell, if the person always report to have some
    # &? search activities, then this spell is called u.

    # &?? For a continuous nonemployment spell index by "ubar_spell_no", if the
    # &?? length of the spell equals to the number of months with unempl==1, then
    # &?? it is an E(U)E spell.
    temp["spellsum_unempl"] = temp.groupby("ubar_spell_no")[
        "unempl"
    ].transform("sum")

    temp["u_spell_no"] = temp["ubar_spell_no"]
    temp.loc[
        (temp["spellsum_unempl"] != temp["len_ubar_spell"]),
        "u_spell_no",
    ] = np.nan

    # !! length of a u spell
    temp["len_u_spell"] = temp.groupby("u_spell_no")["u_spell_no"].transform(
        "count"
    )

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-6-z. drop, order, and sort
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp = temp.drop(
        columns=[
            "nxt_empl",
            "lst_empl",
            "temp_period_id",
            "spellmax_start_of_ubar",
            "spellmax_end_of_ubar",
            "spellmax_unempl",
            "spellsum_unempl",
        ]
    )

    temp = df_order(
        temp,
        (
            "ym",
            "indid",
            "cont_spell_no",
            "len_cont_spell",
            "ubar_spell_no",
            "len_ubar_spell",
            "ustar_spell_no",
            "len_ustar_spell",
            "u_spell_no",
            "len_u_spell",
            "empl",
            "unempl",
            "outlf",
            "start_of_ubar",
            "end_of_ubar",
        ),
    )

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 7. process vars_occ
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-7-1. firm id
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp["firmid"] = np.nan
    temp["firmid"] = temp["firmid"].astype("Int64")

    # !! Case 1. One firm id is missing while the other is not.
    cond_nonmissingid_1 = (
        (temp["eeno1"] != -1) & (temp["eeno2"] == -1) & (temp["empl"] == 1)
    )
    temp.loc[cond_nonmissingid_1, "firmid"] = temp["eeno1"]

    cond_nonmissingid_2 = (
        (temp["eeno1"] == -1)
        & (temp["eeno2"] != -1)
        & (temp["empl"] == 1)
        & (temp["firmid"].isna())
    )
    temp.loc[cond_nonmissingid_2, "firmid"] = temp["eeno2"]

    # !! Case 2. Both firm ids are nonmissing, and they both have nonmissing start
    # !! and end date. Then we pick one has more reasonable start and end date.

    # &? type conversion
    temp["tsjdate1"] = pd.to_datetime(
        temp["tsjdate1"], format="%Y%m%d", errors="coerce"
    )
    temp["tsjdate2"] = pd.to_datetime(
        temp["tsjdate2"], format="%Y%m%d", errors="coerce"
    )
    temp["tejdate1"] = pd.to_datetime(
        temp["tejdate1"], format="%Y%m%d", errors="coerce"
    )
    temp["tejdate2"] = pd.to_datetime(
        temp["tejdate2"], format="%Y%m%d", errors="coerce"
    )

    # &? Basically, I require that job 1 doesn't start too late while end too early
    # &? and job 2 either starts too late or ends too early.
    cond_date_1 = (
        (temp["eeno1"] != -1)
        & (temp["eeno2"] != -1)
        & (temp["tsjdate1"] != -1)
        & (temp["tsjdate2"] != -1)
        & (temp["tejdate1"] != -1)
        & (temp["tejdate2"] != -1)
        & (temp["tsjdate1"] <= temp["ym"] + pd.DateOffset(days=14))
        & (temp["tejdate1"] >= temp["ym"] + pd.DateOffset(days=21))
        & (
            (temp["tsjdate2"] > temp["ym"] + pd.DateOffset(days=14))
            | (temp["tejdate2"] < temp["ym"] + pd.DateOffset(days=7))
        )
    )
    temp.loc[cond_date_1, "firmid"] = temp["eeno1"]

    # &? Similarly, I require that job 2 doesn't start too late while end too early
    # &? and job 1 either starts too late or ends too early.
    cond_date_2 = (
        (temp["eeno1"] != -1)
        & (temp["eeno2"] != -1)
        & (temp["tsjdate1"] != -1)
        & (temp["tsjdate2"] != -1)
        & (temp["tejdate1"] != -1)
        & (temp["tejdate2"] != -1)
        & (temp["tsjdate2"] <= temp["ym"] + pd.DateOffset(days=14))
        & (temp["tejdate2"] >= temp["ym"] + pd.DateOffset(days=21))
        & (
            (temp["tsjdate1"] > temp["ym"] + pd.DateOffset(days=14))
            | (temp["tejdate1"] < temp["ym"] + pd.DateOffset(days=7))
        )
    )
    temp.loc[cond_date_2, "firmid"] = temp["eeno2"]

    # !! Case 3. Both firm ids are nonmissing, pick one that has longer hours.
    cond_hr_1 = (
        (temp["eeno1"] != -1)
        & (temp["eeno2"] != -1)
        & (temp["empl"] == 1)
        & (temp["firmid"].isna())
        & (temp["ejbhrs1"] != -1)
        & (temp["ejbhrs2"] != -1)
        & (temp["ejbhrs1"] > temp["ejbhrs2"])
    )
    temp.loc[cond_hr_1, "firmid"] = temp["eeno1"]

    cond_hr_2 = (
        (temp["eeno1"] != -1)
        & (temp["eeno2"] != -1)
        & (temp["empl"] == 1)
        & (temp["firmid"].isna())
        & (temp["ejbhrs1"] != -1)
        & (temp["ejbhrs2"] != -1)
        & (temp["ejbhrs2"] > temp["ejbhrs1"])
    )
    temp.loc[cond_hr_2, "firmid"] = temp["eeno2"]

    # !! Case 4. Both firm ids are nonmissing, pick one that has higher wages.
    cond_wage_1 = (
        (temp["eeno1"] != -1)
        & (temp["eeno2"] != -1)
        & (temp["empl"] == 1)
        & (temp["firmid"].isna())
        & (temp["tpmsum1"] >= temp["tpmsum2"])
    )
    temp.loc[cond_wage_1, "firmid"] = temp["eeno1"]

    cond_wage_2 = (
        (temp["eeno1"] != -1)
        & (temp["eeno2"] != -1)
        & (temp["empl"] == 1)
        & (temp["firmid"].isna())
        & (temp["tpmsum2"] > temp["tpmsum1"])
    )
    temp.loc[cond_wage_2, "firmid"] = temp["eeno2"]

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-7-2. raw occupation code (the main job)
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp["occ_raw"] = np.nan
    temp["occ_raw"] = temp["occ_raw"].astype("Int64")
    temp.loc[
        ((temp["firmid"] == temp["eeno1"]) & (temp["ajbocc1"] == 0)),
        "occ_raw",
    ] = temp["tjbocc1"]
    temp.loc[
        ((temp["firmid"] == temp["eeno2"]) & (temp["ajbocc2"] == 0)),
        "occ_raw",
    ] = temp["tjbocc2"]

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-7-3. obtain source and destination occupations
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    # !! s-7-3-1. source occupation
    # &? The way I am obtaining the source information is to collect start months
    # &? of the ubar spells of interest, the last month for a start month stores
    # &? a worker's employed occupation.
    start_of_ubar_info = temp.loc[
        ((temp["start_of_ubar"] == 1) & (temp["ubar_spell_no"].notna())),
        ["indid", "ym"],
    ].reset_index(drop=False, names="original_index")

    start_of_ubar_info["last_empl_month"] = start_of_ubar_info[
        "ym"
    ] - pd.DateOffset(months=1)

    start_of_ubar_info = start_of_ubar_info.merge(
        temp[["indid", "ym", "occ_raw"]],
        how="left",
        left_on=["indid", "last_empl_month"],
        right_on=["indid", "ym"],
        validate="one_to_one",
        indicator=True,
    )
    start_of_ubar_info = start_of_ubar_info[["indid", "ym_x", "occ_raw"]]
    start_of_ubar_info = start_of_ubar_info.rename(
        columns={"ym_x": "ym", "occ_raw": "source_occ_raw"}
    )

    # !! s-7-3-2. destination occupation
    # &? The way I am obtaining the destination information is to collect start
    # &? months of the ubar spells of interest, the next month for a start month
    # &? stores a worker's employed occupation.
    end_of_ubar_info = temp.loc[
        ((temp["end_of_ubar"] == 1) & (temp["ubar_spell_no"].notna())),
        ["indid", "ym"],
    ].reset_index(drop=False, names="original_index")

    end_of_ubar_info["next_empl_month"] = end_of_ubar_info[
        "ym"
    ] + pd.DateOffset(months=1)

    end_of_ubar_info = end_of_ubar_info.merge(
        temp[["indid", "ym", "occ_raw"]],
        how="left",
        left_on=["indid", "next_empl_month"],
        right_on=["indid", "ym"],
        validate="one_to_one",
        indicator=True,
    )
    end_of_ubar_info = end_of_ubar_info[["indid", "ym_x", "occ_raw"]]
    end_of_ubar_info = end_of_ubar_info.rename(
        columns={"ym_x": "ym", "occ_raw": "destination_occ_raw"}
    )

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-7-4. merge datasets to obtain the source and destination occupations
    # -?        for an unemployment spell
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp = temp.merge(
        start_of_ubar_info,
        how="outer",
        left_on=["indid", "ym"],
        right_on=["indid", "ym"],
        validate="one_to_one",
        indicator=True,
    )
    temp = temp.drop(columns=["_merge"])

    temp = temp.merge(
        end_of_ubar_info,
        how="outer",
        left_on=["indid", "ym"],
        right_on=["indid", "ym"],
        validate="one_to_one",
        indicator=True,
    )
    temp = temp.drop(columns=["_merge"])

    # &? Currently, the two source and destination occupation variables are only
    # &? defined at the start and end of a ubar spell, we need to make a
    # &? spell-level variable.

    mean_source_occ_raw = temp.groupby("ubar_spell_no")[
        "source_occ_raw"
    ].transform("mean")
    temp["source_occ_raw"] = temp["source_occ_raw"].fillna(mean_source_occ_raw)

    mean_destination_occ_raw = temp.groupby("ubar_spell_no")[
        "destination_occ_raw"
    ].transform("mean")
    temp["destination_occ_raw"] = temp["destination_occ_raw"].fillna(
        mean_destination_occ_raw
    )

    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?
    # -? s-7-4. order columns
    # -?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?#-?

    temp = df_order(
        temp,
        (
            "indid",
            "ym",
            "len_ubar_spell",
            "ubar_spell_no",
            "ustar_spell_no",
            "u_spell_no",
            "source_occ_raw",
            "destination_occ_raw",
            "tpearn",
            "occ_raw",
            "empl",
            "unempl",
            "outlf",
            "panel",
            "swave",
            "year",
            "month",
            "age",
            "tbyear",
            "male",
            "edu",
            "ems",
            "race",
            "rmesr",
            "rwkesr1",
            "rwkesr2",
            "rwkesr3",
            "rwkesr4",
            "rwkesr5",
            "lgtkey",
            "ssuid",
            "eentaid",
            "epppnum",
        ),
    )

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step 8. normalize weights
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # &? This procedure follows the "Carlos Carrillo-Tudela and Ludo Visschers,
    # &? Unemployment and Endogenous Reallocation Over the Business Cycle,
    # &? Econometrica 91, no. 3 (2023): 1119–53".

    # &? Relevant procedure quoted below:

    # &? We use the person weights per wave ("wpfinwgt", and equivalent), but
    # &? normalize these such that the average weight within a panel is equal to 1.
    # &? This is done because the size of panels is not constant, and we do not
    # &? want to weigh panels with fewer observations more heavily as within
    # &? a wave of a panel "wpfinwgt" adds up to population totals and thus is
    # &? higher on average when sample size is smaller.

    # &? We think of our normalization as a reasonably agnostic approach that
    # &? keeps the relative weights within a panel intact, but also takes into
    # &? account the number of available observations.

    temp["sum_weights"] = temp.groupby(["panel"])["wpfinwgt"].transform("sum")
    temp["pweights"] = temp["wpfinwgt"] / temp["sum_weights"]

    temp = df_order(temp, ("pweights",))

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step x. redefine un/non-employment spell id
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    temp["ubar_spell_no"] = temp["ubar_spell_no"] + panel_year * 100000
    temp["ustar_spell_no"] = temp["ustar_spell_no"] + panel_year * 100000
    temp["u_spell_no"] = temp["u_spell_no"] + panel_year * 100000

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step y. print some useful information for each panel
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

    # -? panel year
    print(f"\nPanel = {panel_year}")

    # -? number of E(UBAR)E spells
    num_ubar_spell_no = temp["ubar_spell_no"].nunique()
    print(f"Number of E(UBAR)E spells: {num_ubar_spell_no}")

    # -? number of E(USTAR)E spells
    num_ustar_spell_no = temp["ustar_spell_no"].nunique()
    print(f"Number of E(USTAR)E spells: {num_ustar_spell_no}")

    # -? number of E(U)E spells
    num_u_spell_no = temp["u_spell_no"].nunique()
    print(f"Number of E(U)E spells: {num_u_spell_no}")

    # -? number of individuals in E(UBAR)E spells
    num_indid_ubar = temp.loc[temp["ubar_spell_no"].notna(), "indid"].nunique()
    print(
        f"Number of num individuals in all E(UBAR)E spells: {num_indid_ubar}"
    )

    # -? number of individuals in E(USTR)E spells
    num_indid_ustar = temp.loc[
        temp["ustar_spell_no"].notna(), "indid"
    ].nunique()
    print(
        f"Number of num individuals in all E(UBAR)E spells: {num_indid_ustar}"
    )

    # -? number of individuals in E(U)E spells
    num_indid_u = temp.loc[temp["u_spell_no"].notna(), "indid"].nunique()
    print(f"Number of num individuals in all E(UBAR)E spells: {num_indid_u}")

    # -? number of employed observations without a firmid
    no_firmid = temp.loc[temp["empl"] == 1, "firmid"].isna().sum()
    print(f"Number of employed observations without a firmid: {no_firmid}")

    # -? number of employed observations without an occ_raw
    no_occraw = temp.loc[(temp["empl"] == 1), "occ_raw"].isna().sum()
    print(f"Number of employed observations without an occ_raw: {no_occraw}")

    # -? number of E(UBAR)E spells with identified occupation information
    num_ubar_occ = temp.loc[
        (
            (temp["source_occ_raw"].notna())
            & (temp["destination_occ_raw"].notna())
        ),
        "ubar_spell_no",
    ].nunique()
    print(
        f"Number of E(UBAR)E spells with identified occ info: {num_ubar_occ}"
    )

    # -? number of E(USTAR)E spells with identified occupation information
    num_ustar_occ = temp.loc[
        (
            (temp["source_occ_raw"].notna())
            & (temp["destination_occ_raw"].notna())
        ),
        "ustar_spell_no",
    ].nunique()
    print(
        f"Number of E(USTAR)E spells with identified occ info: {num_ustar_occ}"
    )

    # -? number of E(U)E spells with identified occupation information
    num_u_occ = temp.loc[
        (
            (temp["source_occ_raw"].notna())
            & (temp["destination_occ_raw"].notna())
        ),
        "u_spell_no",
    ].nunique()
    print(f"Number of E(U)E spells with identified occ info: {num_u_occ}")

    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    # ?? step z. export as dta file
    # ??         (used in further occupation recoding procedures)
    # ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
    temp["indid"] = temp["indid"].astype("str")
    dta_name = f"temp{panel}.dta"
    temp.to_stata(tempdata(dta_name), write_index=False)


# ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??
# ?? Code Block 2. Run the Function
# ??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??#??

if __name__ == "__main__":
    codes_path = r"E:\\Projects\\OccupationalMobilityInEUE\\codes"
    SIPP_cleaning(codes_path, panel=1996)
    SIPP_cleaning(codes_path, panel=2001)
    SIPP_cleaning(codes_path, panel=2004)
    SIPP_cleaning(codes_path, panel=2008)

"""
Panel = 1996
Number of E(UBAR)E spells: 14131
Number of E(USTAR)E spells: 7698
Number of E(U)E spells: 5069
Number of num individuals in all E(UBAR)E spells: 9673
Number of num individuals in all E(UBAR)E spells: 5751
Number of num individuals in all E(UBAR)E spells: 3961
Number of employed observations without a firmid: 101
Number of employed observations without an occ_raw: 15391
Number of E(UBAR)E spells with identified occ info: 13213
Number of E(USTAR)E spells with identified occ info: 7236
Number of E(U)E spells with identified occ info: 4741

Panel = 2001
Number of E(UBAR)E spells: 9014
Number of E(USTAR)E spells: 5102
Number of E(U)E spells: 3341
Number of num individuals in all E(UBAR)E spells: 6878
Number of num individuals in all E(UBAR)E spells: 4141
Number of num individuals in all E(UBAR)E spells: 2805
Number of employed observations without a firmid: 87
Number of employed observations without an occ_raw: 24753
Number of E(UBAR)E spells with identified occ info: 8252
Number of E(USTAR)E spells with identified occ info: 4720
Number of E(U)E spells with identified occ info: 3086

Panel = 2004
Number of E(UBAR)E spells: 13047
Number of E(USTAR)E spells: 6786
Number of E(U)E spells: 4241
Number of num individuals in all E(UBAR)E spells: 9448
Number of num individuals in all E(UBAR)E spells: 5367
Number of num individuals in all E(UBAR)E spells: 3495
Number of employed observations without a firmid: 134
Number of employed observations without an occ_raw: 24206
Number of E(UBAR)E spells with identified occ info: 11828
Number of E(USTAR)E spells with identified occ info: 6197
Number of E(U)E spells with identified occ info: 3896

Panel = 2008
Number of E(UBAR)E spells: 14286
Number of E(USTAR)E spells: 9408
Number of E(U)E spells: 6615
Number of num individuals in all E(UBAR)E spells: 9923
Number of num individuals in all E(UBAR)E spells: 6935
Number of num individuals in all E(UBAR)E spells: 5079
Number of employed observations without a firmid: 170
Number of employed observations without an occ_raw: 25678
Number of E(UBAR)E spells with identified occ info: 13166
Number of E(USTAR)E spells with identified occ info: 8788
Number of E(U)E spells with identified occ info: 6180
"""
