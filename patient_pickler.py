"""
patient_pickler.py
version 3.0
package github.com/ajk77/PatientPy
Created by AndrewJKing.com|@andrewsjourney

This file uses database connections to query and store patient data. 

Connections are specific to HiDENIC and MARS data stucture. 

Data types handled include:
demographics (demo)
laboratory tests, vital signs, and venilator settings (root)
medication orders (med)
intake and output measurements (io)
micro biology (micro)
procedures (procedures)

---DEPENDENCIES---
A pickle directory (pkl_dir) must be created.
^The following subdirectories must be created as well: 'root_data/','flag_data/','med_data/','procedure_data/','micro_data/','io_data/','demo_data/'
The database table connected to in the determine_cases_days_and_times() method should contain only the patient cases of interest (e.g. after location, year, and diagnosis selection)

---TODO---
[] Setup database connections to NOC
[] Setup database connections to MIMIC III

---LICENSE---
This file is part of PatientPy

PatientPy is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or 
any later version.

PatientPy is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with PatientPy.  If not, see <https://www.gnu.org/licenses/>.
"""
from patientpy_utils import load_info_from_pickle_file, load_case_day_mapping, determine_case_times
import pickle
import unicodedata
import os
import time
import datetime

# crisma (or convert to NOC server)
from models import a_demographics
from models import a_ClinicalEvents
from models import a_HomeMeds
from models import a_ICUpatients
from models import a_IO
from models import a_Medication
from models import a_Micro
from models import a_Surgical
from models import a_Ventilator
# local_lemr (or convert to NOC server)
from models import lab_739
from models import marstorootcodes
from models import rootgroupmember
from models import discrete_result_mapping


def query_and_store_root_information(root_info_pickle_file):
    """
    This file queries and stores the root information from the different tables.
    Uses three database connections
    """
    def load_marstoroot():
        """
        Database connection to lab code mapping table
        """
        mtr = {}  # mars to root
        rtm = {}  # root to mars
        results = marstorootcodes.objects.all()  # database connection for mapping file
        for result in results:
            mtr[result.marscode] = result.rootcode
            if result.rootcode in rtm.keys():
                rtm[result.rootcode].append(result.marscode)
            else:
                rtm[result.rootcode] = [result.marscode]

        # # skip these mappings
        rtm.pop('X', None)
        rtm.pop('U', None)

        return [mtr, rtm]

    def load_marstoroot_include_vitals_and_vent():
        """
        Returns mars to root mappings and root to mars mappings.
        Root to mars mappings include vitals and vents.
        Since the vital and vent mapping are nott in the mapping table, they are manually added.
        """
        mtr, rtm = load_marstoroot()

        rtm['MODE'] = ['Mode']
        rtm['VSTRIA'] = ['Trial extubation']
        rtm['VSTUBE'] = ['Tube Status']
        rtm['VSVENT'] = ['Vent Status']
        rtm['O2VENT'] = ['FIO2']
        rtm['VTCPP'] = ['CPP']
        rtm['VTCVP'] = ['CVP']
        rtm['VTDIAA'] = ['Pulmonary artery diastolic']
        rtm['VTDIAV'] = ['Diastolic BP']
        rtm['VTHR'] = ['HR']
        rtm['VTICP'] = ['ICP']
        rtm['VTRR'] = ['RR']
        rtm['VTSO2A'] = ['SaO2']
        rtm['VTSO2V'] = ['SvO2']
        rtm['VTSYSA'] = ['Pulmonary artery systolic']
        rtm['VTSYSV'] = ['Systolic BP']
        rtm['VTTEMA'] = ['Temperature']
        rtm['VTWEIG'] = ['Weight']
        rtm['VTWGEA'] = ['Pulmonary artery wedge']

        return mtr, rtm

    def load_rootgroupmember():
        """
        Database connection to group membership table
        """
        groups = {}  # [groupname] = [root1, root2, ...]
        lab_group_order = [0] * 19
        rtn = {}  # root to name
        rtt = {}  # root to table
        results = rootgroupmember.objects.all()  # database connection for group membership
        for result in results:
            if result.groupname in groups.keys():
                groups[result.groupname].append(result.root)
            else:
                groups[result.groupname] = [result.root]
                if result.grouprank < 20:
                    lab_group_order[result.grouprank-1] = result.groupname
            rtn[result.root] = result.labname
            rtt[result.root] = result.datatable
        return [groups, lab_group_order, rtn, rtt]

    def load_root_to_datatype():
        """
        Returns a dict of [root] -> datatype
        datatypes can be binary, interval, nominal, ordinal, unused-nom
        """
        rtdt = {}  # root to data type
        results = rootgroupmember.objects.all()  # Database connection from group membership for datatype
        for result in results:
            rtdt[result.root] = result.datatype
        return rtdt

    def load_discrete_result_mapping():
        """
        Database connection to discrete result mapping table
        """
        drm = {}  # discrete result mapping
        results = discrete_result_mapping.objects.all()  # Database connection for discrete result mapping
        for result in results:
            if result.root not in drm.keys():
                drm[result.root] = [[], [], [], []]  # [[text maps to 1], [to 2], [to 3], [to 4]]
            drm[result.root][int(result.mapvalue)-1].append(result.eventtext)
        return drm

    mtr, rtm = load_marstoroot_include_vitals_and_vent()  # mars to root, root to mars
    groups, lab_group_order, rtn, rtt = load_rootgroupmember()  # root to name, root to table
    rtdt = load_root_to_datatype()  # root to data type
    drm = load_discrete_result_mapping()  # discrete result mapping
    root_order = rtm.keys()  # use this order for all loops b/c roots need to be consistent order.

    pickle_data = [mtr, rtm, groups, lab_group_order, rtn, rtt, rtdt, drm, root_order]

    with open(root_info_pickle_file, 'wb') as f:
        pickle.dump(pickle_data, f)

    return


def query_and_store_other_information(other_info_pickle_file):
    """
    This file queries and stores the med, procedure, micro, io, and demographic information from the different tables.
    Uses three database connections
    """
    # ## query and store medications and home medications
    med_order = []
    mtt = {}
    results = a_Medication.objects.using('remote').order_by().values('name').distinct()  # Database connection to medication table
    for result in results:
        if result['name'] not in med_order:
            med_order.append(result['name'])
            mtt[result['name']] = 'a_Medication'
    results = a_HomeMeds.objects.using('remote').order_by().values('genericname').distinct()  # Database connection to home medication table
    for result in results:
        if result['genericname'] not in med_order:
            med_order.append(result['genericname'])
            mtt[result['genericname']] = 'a_HomeMeds'
        else:
            if mtt[result['genericname']] == 'a_Medication':  # catch meds that are both types
                mtt[result['genericname']] = 'both'
    
    # ## query and store procedures
    procedure_order = []
    results = a_Surgical.objects.using('remote').order_by().values('procedure').distinct()  # Database connection to procedure table
    for result in results:
        if result['procedure'] not in procedure_order:
            procedure_order.append(result['procedure'])
    
    # ## Store micro biology, intake and output, and demographics orders. No connection becasue these are short, manually entered lists
    micro_order = ['Blood Culture (C&S)', 'MRSA Screen for Infection Control (Nose', 'VRE Screen',
                   'Urine Culture (C&S)', 'Clostridium difficile Toxin (Stool)', 'Fungus Culture (C&S)',
                   'Anaerobic Culture (C&S)', 'Sputum Culture (C&S) (with Gram Stain)',
                   'Acinetobacter Screen Infection Control', 'MICRO_ANY']  # These were selected to only include frequent cultures
    io_order = ['Urine', 'Everything Else', 'Oral', 'Intravenous', 'Blood Products', 'Other or unknown', 'Net']  
    demo_order = ['age', 'sex', 'height', 'weight', 'bmi', 'race']  

    pickle_data = [med_order, mtt, procedure_order, micro_order, io_order, demo_order]

    with open(other_info_pickle_file, 'wb') as f:
        pickle.dump(pickle_data, f)

    return


def determine_cases_days_and_times(out_file):
    """
    This function determine the number of days and cuttimes for each case day.
    Uses one database connection
    """
    out_file = open(out_file, 'w')
    out_file.write('#PatientVisitId,los,cutoff\n')
    visited_case_ids = []
    results = a_ICUpatients.objects.using('remote').all()  # Database connection to patient admit/discharge table
    for result in results:
        if result.patientvisitid not in visited_case_ids:
            visited_case_ids.append(result.patientvisitid)
    for case_id in visited_case_ids:
        results = a_ICUpatients.objects.using('remote').filter(patientvisitid=case_id)  # Database connection to patient admit/discharge table
        t_icu_admit = 0
        t_icu_discharge = 0
        first = True
        current_admits_dischs = []
        # find earliest icu admission
        for result in results:
            curr_admit = (time.mktime(result.ICUadmit.timetuple()) - 18000)  # subtrackt to adjust for time zone
            curr_disch = (time.mktime(result.ICUdischarge.timetuple()) - 18000)  
            current_admits_dischs.append([curr_admit, curr_disch])
            if first:
                t_icu_admit = curr_admit
                t_icu_discharge = curr_disch
                first = False
            else:
                t_icu_admit = min(t_icu_admit, curr_admit)
                t_icu_discharge = max(t_icu_discharge, curr_disch)
                # it is possible that some of the cut times occur in between ICU admissions.  This is handeled below

        day_diff = (t_icu_discharge - t_icu_admit) // 86400
        midnight_admit = (t_icu_admit // 86400) * 86400
        eight_am_admit = midnight_admit + 28800
        for los in range(int(day_diff+1)):
            current_day_eight_am = eight_am_admit + (86400 * los)
            for admission in current_admits_dischs:  
                if (admission[0] - 43200) < current_day_eight_am < (admission[1] + 43200):  # insure current day is between an admission and discharge
                    out_file.write(str(case_id)+','+str(los)+','+str(current_day_eight_am)+'\n')
                    break
    out_file.close()
    return


def load_a_patients_root_data(case_id, root_order, rtm, rtt, rtdt):
    """
    Loads all root based data for a PatientVisitId
    Uses three database connections
    """
    def is_num(s):
        """
        Returns True if string is a number, otherwise False
        """
        try:
            float(s)
            return True
        except (ValueError, TypeError):
            pass
        return False

    def parse_to_num(text):
        """
        Removes special characters from string so it can be parsed to a float
        """
        for ch in ['<', '=', '>', '%']:
            if text and ch in text:
                text = text.replace(ch, "")
        return text

    def unicode_to_str(text):
        """
        Returns string from unicode input. Django's default is unicode.
        """
        return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore')

    root_data = {}  # stores all of a patients root related data

    # ## load all of the patient's data
    for root in root_order:
        root_data[root] = []
        # do processing below convert each data point into tuples
        for marsCode in rtm[root]:
            if rtt[root] == 'lab_739':
                results = lab_739.objects.filter(eventcode=marsCode, patientvisitid=case_id)  # Database connection to laboratory test result table
                if rtdt[root] == 'interval':
                    for result in results:
                        t = (time.mktime(result.eventdate.timetuple()) - 18000) * 1000
                        if is_num(result.eventvalue):
                            root_data[root].append((t, float(result.eventvalue)))
                        else:
                            curr_text = parse_to_num(result.eventtext)
                            if is_num(curr_text):
                                root_data[root].append((t, float(curr_text)))
                else:
                    for result in results:
                        t = (time.mktime(result.eventdate.timetuple()) - 18000) * 1000
                        if result.eventtext is not None:
                            root_data[root].append((t, unicode_to_str(result.eventtext)))
            elif rtt[root] == 'a_clinicalevents':
                results = a_ClinicalEvents.objects.using('remote').filter(rollname=marsCode, patientvisitid=case_id)  # Database connection to vital sign table
                # this table is always interval
                for result in results:
                    t = (time.mktime(result.date.timetuple()) - 18000) * 1000
                    if result.rollval is not None:
                        root_data[root].append((t, float(result.rollval)))
            elif rtt[root] == 'a_Ventilator':
                results = a_Ventilator.objects.using('remote').filter(eventname=marsCode, patientvisitid=case_id)  # Database connection to ventilator setting table
                # this table is never interval
                for result in results:
                    t = (time.mktime(result.date.timetuple()) - 18000) * 1000
                    if result.resultval is not None:
                        root_data[root].append((t, unicode_to_str(result.resultval)))
            else:
                print '***should never print this (data_table)***'

        # ## sort data by date
        root_data[root].sort(key=lambda tup: tup[0])

    return root_data


def load_and_pickle_patients_flag_data(case_id, root_order, rtm, rtt):
    """
    This function loads and pickles all patients flag data (flags for abnormal tests).
    Uses one database connection
    """
    flag_data = {}  # stores all flag data for patient
    for root in root_order:
        flag_data[root] = []
        if rtt[root] == 'lab_739':
            for marsCode in rtm[root]:
                results = lab_739.objects.filter(eventcode=marsCode, patientvisitid=case_id)  # Database connection to test result table.
                for result in results:
                    t = (time.mktime(result.eventdate.timetuple()) - 18000) * 1000
                    flag_data[root].append((t, result.flag))

        # ## sort data by date
        flag_data[root].sort(key=lambda tup: tup[0])

    return flag_data


def load_and_pickle_patients_med_data(case_id, med_order):
    """
    This function loads and pickles all patients med data.
    Uses two database connections.
    """
    med_data = {}  # stores all med data for patient
    pat_med_results = a_Medication.objects.using('remote').filter(patientvisitid=case_id)  # Database connection to medication order table
    pat_home_med_results = a_HomeMeds.objects.using('remote').filter(patientvisitid=case_id)  # Database connection to home medication perscription table.
    for med in med_order:
        med_data[med] = []
    for result in pat_med_results:
        t = (time.mktime(result.date.timetuple()) - 18000) * 1000
        med_data[result.name].append((t, result.dose))
    for result in pat_home_med_results:
        t = (time.mktime(result.date.timetuple()) - 18000) * 1000
        med_data[result.genericname].append((t, result.dose))

    # ## sort data by date
    for med in med_order:
        med_data[med].sort(key=lambda tup: tup[0])

    return med_data


def load_and_pickle_patients_procedure_data(case_id, procedure_order):
    """
    This function loads and pickles all patients procedure data.
    Uses one database connection.
    """
    procedure_data = {}  # stores all flag data for patient
    pat_procedure_data = a_Surgical.objects.using('remote').filter(patientvisitid=case_id)  # Database connection to procedures table.
    for procedure in procedure_order:
        procedure_data[procedure] = []
    for result in pat_procedure_data:
        t = (time.mktime(result.date.timetuple()) - 18000) * 1000
        procedure_data[result.procedure].append((t, 1))

    # ## sort data by date
    for procedure in procedure_order:
        procedure_data[procedure].sort(key=lambda tup: tup[0])
    return procedure_data


def load_and_pickle_patients_micro_data(case_id, micro_order):
    """
    This function loads and pickles all patients Micro data.
    Uses one database connection.
    """
    micro_data = {}  # stores all flag data for patient
    pat_micro_data = a_Micro.objects.using('remote').filter(patientvisitid=case_id)  # Database connection to micro biology table
    for micro in micro_order:
        micro_data[micro] = []
    for result in pat_micro_data:
        t = (time.mktime(result.date.timetuple()) - 18000) * 1000
        micro_data['MICRO_ANY'].append((t, 1))  # boolean for if ordered
        if result.ordername in micro_order:
            micro_data[result.ordername].append((t, 1))
    
    # ## sort data by date
    for micro in micro_order:
        micro_data[micro].sort(key=lambda tup: tup[0])
    return micro_data


def load_and_pickle_patients_io_data(case_id):
    """
    This function loads and pickles all patients io data.
    Uses one database connection
    """
    def load_io(case_id):
        curr_data = [[], [], [], [], [], [], []]  # urine, everything else, oral, intravenous, blood products, other, net
        existing_dates = []
        results = a_IO.objects.using('remote').filter(patientvisitid=case_id)  # Database connection to intake and output table
        for result in results:
            t = (time.mktime(result.date.timetuple()) - 18000) * 1000
            day = ((t // 86400000) * 86400000)  # reduce to day

            if result.type == 'Output':
                if result.name == 'Urine Output':
                    io_type = 0
                else:
                    io_type = 1
            else:
                if result.category == 6:
                    io_type = 2
                elif result.category in [1, 2, 3, 4, 5, 8, 9, 10]:
                    io_type = 3
                elif result.category == 7:
                    io_type = 4
                else:  # category > 10
                    io_type = 5

            if day not in existing_dates:
                existing_dates.append(day)
                for i in range(7):
                    curr_data[i].append([day, 0])
            curr_data[io_type][existing_dates.index(day)][1] += round(result.volume, 2)

        # make output negative
        for i in range(len(curr_data[1])):
            curr_data[0][i][1] = -curr_data[0][i][1]
            curr_data[1][i][1] = -curr_data[1][i][1]
        # calculate net
        low_high = [1, -1]
        for i in range(len(existing_dates)):
            day_net = 0
            day_pos = 0
            day_neg = 0
            for q in range(6):
                day_net += curr_data[q][i][1]
                if curr_data[q][i][1] > 0:
                    day_pos += curr_data[q][i][1]
                if curr_data[q][i][1] < 0:
                    day_neg += curr_data[q][i][1]
            curr_data[6][i][1] = day_net
            low_high[0] = min(low_high[0], day_neg)
            low_high[1] = max(low_high[1], day_pos)

        dict_results = [{"name": 'Urine', "step": 1, "data": io_to_day(curr_data[0]), "stack": "a"},
                        {"name": 'Everything Else', "step": 1, "data": io_to_day(curr_data[1]), "stack": "a"},
                        {"name": 'Oral', "step": 1, "data": io_to_day(curr_data[2]), "stack": "a"},
                        {"name": 'Intravenous', "step": 1, "data": io_to_day(curr_data[3]), "stack": "a"},
                        {"name": 'Blood Products', "step": 1, "data": io_to_day(curr_data[4]), "stack": "a"},
                        {"name": 'Other or unknown', "step": 1, "data": io_to_day(curr_data[5]), "stack": "a"},
                        {"name": 'Net', "step": 1, "data": io_to_day(curr_data[6]), "stack": "b"}]

        return [dict_results, low_high]

    io_data = {}
    io_dict, discard = load_io(case_id, 1577840461000)  # include all times
    for io_category_data in io_dict:
        io_data[io_category_data['name']] = io_category_data['data']

    return io_data


def load_and_pickle_patients_demo_data(case_id):
    """
    This function loads and pickles all patients io and demographic data.
    Uses one database connection.
    """
    pat_demo_data = a_demographics.objects.using('remote').get(patientvisitid=case_id)  # Database connection to demographics table.
    if pat_demo_data.sex is None or pat_demo_data.sex == 'M':
        sex = 0
    else:
        sex = 1
    if pat_demo_data.race == 'White':
        race = 0
    else:
        race = 1

    demo_data = [{"age": pat_demo_data.age, "sex": sex, "height": pat_demo_data.height,
                  "weight": pat_demo_data.weight, "bmi": pat_demo_data.bmi, "race": race}]

    return demo_data


def pickle_patient_data(root_dir, pickle_dir, case_day_filename='case_day_mapping.txt'):
    """
    Creates and stores patient data and root_information as binary
    """
    # ## create and store information structures
    query_and_store_root_information(pickle_dir+'root_info.pickle')  # store root information structures
    query_and_store_other_information(pickle_dir+'other_info.pickle')  # store med, procedure, micro, intake and output, and dempgraphic mapping information
    
    # ## load stored information structures
    mtr, rtm, groups, lab_group_order, rtn, rtt, rtdt, drm, root_order = load_info_from_pickle_file(pickle_dir+'root_info.pickle')
    med_order, mtt, procedure_order, micro_order, io_order, demo_order = load_info_from_pickle_file(pickle_dir+'other_info.pickle')
    # ^mtr=mars2root, rtm=root2mars, rtn=root2name, rtt=root2table, rtdt=root2datatype drm=discrete result mapping
    # ^^mtt=med2table
    
    # ## create patient case day file
    determine_cases_days_and_times(root_dir+case_day_filename)

    # ## load case day file info (order of cases, patient los days, time points of those los days)
    patient_order, patient_days, patient_cut_times = load_case_day_mapping(root_dir+case_day_filename)  

    # ## for each patient case
    for i in range(len(patient_order)):
        print patient_order[i], '\t', i  # print progress

        # ## the root data
        curr_patient_data = load_a_patients_root_data(patient_order[i], root_order, rtm, rtt, rtdt)
        with open(pickle_dir+'root_data/'+patient_order[i]+'.pickle', 'wb') as f:
            pickle.dump(curr_patient_data, f)
        # ## the (root) flag data function stores flag data separately
        curr_pat_flag_data = load_and_pickle_patients_flag_data(patient_order[i], root_order, rtm, rtt)
        with open(pickle_dir+'flag_data/'+patient_order[i]+'.pickle', 'wb') as f:
            pickle.dump(curr_pat_flag_data, f)
        # ## the medication data
        curr_pat_med_data = load_and_pickle_patients_med_data(patient_order[i], med_order)
        with open(pickle_dir+'med_data/'+patient_order[i]+'.pickle', 'wb') as f:
            pickle.dump(curr_pat_med_data, f)
        # ## the procedure data
        curr_pat_procedure_data = load_and_pickle_patients_procedure_data(patient_order[i], procedure_order)
        with open(pickle_dir+'procedure_data/'+patient_order[i]+'.pickle', 'wb') as f:
            pickle.dump(curr_pat_procedure_data, f)
        # ## the micro_data
        curr_pat_micro_data = load_and_pickle_patients_micro_data(patient_order[i], micro_order)
        with open(pickle_dir+'micro_data/'+patient_order[i]+'.pickle', 'wb') as f:
            pickle.dump(curr_pat_micro_data, f)
        # ## i/o  data
        curr_pat_io_data = load_and_pickle_patients_io_data(patient_order[i])
        with open(pickle_dir+'io_data/'+patient_order[i]+'.pickle', 'wb') as f:
            pickle.dump(curr_pat_io_data, f)
        # ## demographic  data
        curr_pat_demo_data = load_and_pickle_patients_demo_data(patient_order[i])
        with open(pickle_dir+'demo_data/'+patient_order[i]+'.pickle', 'wb') as f:
            pickle.dump(curr_pat_demo_data, f)

    return


if __name__ == '__main__':
    # ## set pickle storage directory
    root_dir = '//modeling_folder/'
    pkl_dir = '//modeling_folder/all_data_pickle_files/'
    case_day_filename = 'case_day_mapping-01Jan2018.txt'  # running this file will create 
    
    # ## will need to rund this once on database to generate case_day_mapping file
    if False:
        determine_case_times(root_dir + case_day_filename)

    # ## run pickle_patient_data
    pickle_patient_data(root_dir, pickle_dir, case_day_filename)
