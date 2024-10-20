#! python3

# ?? This script stores variable and value labels for tempfiles and finalfiles


original_val_labs = {
    "rwkesr2": {
        -1: "Not in universe",
        1: "With job/bus - working",
        2: "With job/bus - not on layoff, absent w/out pay",
        3: "With job/bus - on layoff, absent w/out pay",
        4: "No job/bus - looking for work or on layoff",
        5: "No job/bus - not looking and not on layoff",
    },
    "rmesr": {
        -1: "Not in universe",
        1: "With a job entire month, worked all weeks",
        2: "With a job all month, absent from work w/out pay 1+ weeks, absence not due to layoff",
        3: "With job all month, absent from work w/out pay 1+ weeks, absence due to layoff",
        4: "With a job at least 1 but not all weeks, no time on layoff and no time looking for work",
        5: "With job at least 1 but not all weeks, some weeks on layoff or looking for work",
        6: "No job all month, on layoff or looking for work all weeks",
        7: "No job, at least one but not all weeks on layoff or looking for work",
        8: "No job, no time on layoff and no time looking for work",
    },
}

val_labs = {
    "ind_self_empl": {
        1: "ind-level, =1, if the individual has ever been self-employed in the dataset",
        0: "ind-level, =0, if the individual has never been self-employed in the dataset",
    },
    "ind_armed": {
        -1: "Not in universe",
        1: "ind-level, =1, if the individual has ever been in the armed force",
        0: "ind-level, =0, if the individual has never been in the armed force",
    },
    "educ": {
        -1: "Not in universe",
        1: "Below high school",
        2: "High school graduate",
        3: "Some college but no degree, or Diploma or certificate from a voc, tech, trade or bus school beyond high school, or Associate degree in college - Occupational/vocational program, or Associate Degree in college - Academic program",
        4: "Bachelors degree (For example: BA, AB, BS)",
        5: "Master's, or Professional School, or Doctorate degree",
    },
    "race": {
        1: "White",
        2: "Black",
        3: "Residual",
    },
    "ems": {
        1: "Married, spouse present",
        2: "Married, spouse absent",
        3: "Widowed",
        4: "Divorced",
        5: "Separated",
        6: "Never Married",
    },
    "mn_empl": {
        1: "ind-month level, =1, if rmesr==1 | 2 | 3 | 4 | 5 (employed based solely on monthly indicators)",
        0: "ind-month level, =0, otherwise",
    },
    "mn_unempl": {
        1: "ind-month level, =1, if rmesr==6 | 7 (unemployed based solely on monthly indicators)",
        0: "ind-month level, =0, otherwise",
    },
    "mn_outlf": {
        1: "ind-month level, =1, if rmesr==8 (out of labor force based solely on monthly indicators)",
        0: "ind-month level, =0, otherwise",
    },
    "empl": {
        1: "ind-month level, =1, if the individual is employed based on both monthly and weekly indicators (if the monthly variable says he has a job all month or if the week 2 weekly variable says he has a job all week, he is employed)",
        0: "ind-month level, =0, otherwise",
    },
    "unempl": {
        1: "ind-month level, =1, if the individual is unemployed based on both monthly and weekly indicators",
        0: "ind-month level, =0, otherwise",
    },
    "outlf": {
        1: "ind-month level, =1 if empl==0 & unempl==0",
        0: "ind-month level, =0, otherwise",
    },
    "inlf": {
        1: "ind-month level, =1 if empl==1 | unempl==1",
        0: "ind-month level, =0, otherwise",
    },
    "retired": {
        1: "ind-month level, =1 if the individual declares retirement (1. He declares he is not working because of retirement. 2. He declares quitting last job to retire. 3. He has previously declared quitting last job to retire.)",
        0: "ind-month level, =0, otherwise",
    },
    "ind_gov": {
        1: "ind level, =1, if the individual has ever been a government employee",
        0: "ind level, =0, otherwise",
    },
    "disc_spell": {
        1: "ind-month level, -1, if the next occurrence month for that individual is not next calendar month; in other words, starting from next occurrence month, it is another spell that has discountinuous gaps in the dataset",
        0: "otherwise",
    },
    "cont_spell_no": {
        "index of a continuous time spell an individual is in, range: [1, 6]"
    },
    "len_cont_spell": {
        "length (number of months) of that spell number, range: [1, 48]"
    },
    "u_bar": {
        "ubar is the most general unemployment notion. If an individual is not employed in a month, he is defined as ubar."
    },
    "start_of_ubar": {
        "ind-month level, =1, in the month when an individual is employed (empl==1) last month, but ubar (empl==0) this month"
    },
    "end_of_ubar": {
        "ind-month level, =1, in the month when an individual is employed (empl==1) next month, but ubar (empl==0) this month"
    },
    "ubar_spell_no": {
        "spell level, nonmissing only for those ubar spells with both observable start_of_ubar and observable end_of_ubar"
    },
    "len_ubar_spell": {
        "spell level, length of a E(UBAR)E spell, nonmissing only for those ubar spells with both observable start_of_ubar and observable end_of_ubar"
    },
    "ustar": {
        "ustar is a slightly weaker notion of unemployment. For a ubar spell, if the individual has some search activities (unempl==1) at least one month, this spell is defined as ustar"
    },
    "ustar_spell_no": {
        "spell level, nonmissing only for those E(UBAR)E spells (that is, ubar spells with observable start_of_ubar and observable end_of_ubar) that include at least one month of search (unempl==1)"
    },
    "len_ustar_spell": {
        "spell level, length of a E(USTAR)E spell, nonmissing only for those E(UBAR)E spells (that is, ubar spells with observable start_of_ubar and observable end_of_ubar) that include at least one month of search (unempl==1)"
    },
    "u": {
        "u is the most strict notion of unemployment spell. For a ubar spell, if the individual has some search activities (unempl==1) for all months, this spell is defined as u"
    },
    "len_u_spell": {
        "spell level, nonmissing only for those E(U)E spells (that is, ubar spells with observable start_of_ubar and observable end_of_ubar) that individuals have search activities in all months (unempl==1)"
    },
    "occ1990dd": {
        # ?? A.1 Executive, Administrative, and Managerial Occupations
        4: "Chief executives and general administrators, public administration",
        7: "Financial managers",
        8: "Human resources and labor relations managers",
        13: "Managers and specialists in marketing, advertising, and public relations",
        14: "Managers in education and related fields",
        15: "Managers of medicine and health occupations",
        18: "Managers of properties and real estate",
        19: "Funeral directors",
        22: "Managers and administrators, n.e.c.",
        # ?? A.2 Management Related Occupations
        23: "Accountants and auditors",
        24: "Insurance underwriters",
        25: "Other financial specialists",
        26: "Management analysts",
        27: "Personnel, HR, training, and labor rel. specialists",
        28: "Purchasing agents and buyers of farm products",
        29: "Buyers, wholesale and retail trade",
        33: "Purchasing managers, agents, and buyers, n.e.c.",
        34: "Business and promotion agents",
        35: "Construction inspectors",
        36: "Inspectors and compliance officers, outside",
        37: "Management support occupations",
        # ?? A.3 Professional Specialty Occupations
        43: "Architects",
        44: "Aerospace engineers",
        45: "Metallurgical and materials engineers",
        47: "Petroleum, mining, and geological engineers",
        48: "Chemical engineers",
        53: "Civil engineers",
        55: "Electrical engineers",
        56: "Industrial engineers",
        57: "Mechanical engineers",
        59: "Engineers and other professionals, n.e.c.s",
        64: "Computer systems analysts and computer scientists",
        65: "Operations and systems researchers and analysts",
        66: "Actuaries",
        68: "Mathematicians and statisticians",
        69: "Physicists and astronomists",
        73: "Chemists",
        74: "Atmospheric and space scientists",
        75: "Geologists",
        76: "Physical scientists, n.e.c.",
        77: "Agricultural and food scientists",
        78: "Biological scientists",
        79: "Foresters and conservation scientists",
        83: "Medical scientists",
        84: "Physicians",
        85: "Dentists",
        86: "Veterinarians",
        87: "Optometrists",
        88: "Podiatrists",
        89: "Other health and therapy occupations",
        95: "Registered nurses",
        96: "Pharmacists",
        97: "Dieticians and nutritionists",
        98: "Respiratory therapists",
        99: "Occupational therapists",
        103: "Physical therapists",
        104: "Speech therapists",
        105: "Therapists, n.e.c.",
        106: "Physicians' assistants",
        154: "Subject instructors, college",
        155: "Kindergarten and earlier school teachers",
        156: "Primary school teachers",
        157: "Secondary school teachers",
        158: "Special education teachers",
        159: "Teachers, n.e.c.",
        163: "Vocational and educational counselors",
        164: "Librarians",
        165: "Archivists and curators",
        166: "Economists, market and survey researchers",
        167: "Psychologists",
        169: "Social scientists and sociologists, n.e.c.",
        173: "Urban and regional planners",
        174: "Social workers",
        176: "Clergy and religious workers",
        177: "Welfare service workers",
        178: "Lawyers and judges",
        183: "Writers and authors",
        184: "Technical writers",
        185: "Designers",
        186: "Musicians and composers",
        187: "Actors, directors, and producers",
        188: "Painters, sculptors, craft-artists, and print-makers",
        189: "Photographers",
        193: "Dancers",
        194: "Art/entertainment performers and related occupations",
        195: "Editors and reporters",
        198: " Announcers",
        199: "Athletes, sports instructors, and officials",
    },
}
