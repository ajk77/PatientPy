"""
patient_pickler.py
version 1.0
package patientpy
Created by AndrewJKing.com

This file uses database connections to query and store patient data. 

Connections are specific to HiDENIC and MARS data stucture. 

Data types handled include:
demographics (demo)
laboratory tests and vital signs (root)
medication orders (med)
intake and output measurements (io)
micro biology (micro)
procedures (procedures)

"""
# crisma server
from models import a_demographics
from models import a_ClinicalEvents
from models import a_HomeMeds
from models import a_ICDCPT
from models import a_ICUpatients
from models import a_IO
from models import a_Medication
from models import a_Micro
from models import a_MicroReport
from models import a_Surgical
from models import a_Ventilator
# local_lemr
from models import lab_739

from loaddata import load_io
from utils import *
from data_featurizer import *

import os
import time
import datetime
import pickle
import json
import unicodedata
import re  # regex
import numpy as np


def load_hidenic_rollnames():
    """
    Function that returns hidenic_rollnames and frewuent_rollnames
    """
    hidenic_rollnames = ['GLU', 'K', 'LACT', 'NA', 'BUN', 'CL', 'CO2', 'CREAT', 'PCO2', 'PO2', 'HCT', 'HGB', 'WBC',
                         'PLT', 'TROPN', 'INR', 'BAND', 'CRP', 'ALB', 'ALT', 'AST', 'TBILI', 'LDH', 'VTSO2V',
                         'VTWEIG', 'VTWGEA']

    frequent_rollnames = ['VTCPP', 'VTCVP', 'VTDIAA', 'VTDIAV', 'VTHR', 'VTICP', 'VTRR', 'VTSO2A', 'VTSYSA',
                          'VTSYSV', 'VTTEMA']
    '''
    # ## Reduce and rename root_order
    for root in hidenic_events_order:
        root_order.remove(root)
    for root in frequent_vitals_order:
        root_order.remove(root)
    if False:
        with open(feature_dir + 'root_feature_columns.txt', 'w') as f:
            f.write('\n'.join(root_order))
        with open(feature_dir + 'hidenic_feature_columns.txt', 'w') as f:
            f.write('\n'.join(hidenic_events_order))
        with open(feature_dir + 'frequent_feature_columns.txt', 'w') as f:
            f.write('\n'.join(frequent_vitals_order))
    '''
    return [hidenic_rollnames, frequent_rollnames]


def load_a_patients_root_data(case_id, root_order, rtm, rtt, rtdt):
    """
    Loads all root based data for a PatientVisitId
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
                results = lab_739.objects.filter(eventcode=marsCode, patientvisitid=case_id)
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
                results = a_ClinicalEvents.objects.using('remote').filter(rollname=marsCode, patientvisitid=case_id)
                # this table is always interval
                for result in results:
                    t = (time.mktime(result.date.timetuple()) - 18000) * 1000
                    if result.rollval is not None:
                        root_data[root].append((t, float(result.rollval)))
            elif rtt[root] == 'a_Ventilator':
                results = a_Ventilator.objects.using('remote').filter(eventname=marsCode, patientvisitid=case_id)
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
    """
    flag_data = {}  # stores all flag data for patient
    for root in root_order:
        flag_data[root] = []
        if rtt[root] == 'lab_739':
            for marsCode in rtm[root]:
                results = lab_739.objects.filter(eventcode=marsCode, patientvisitid=case_id)
                for result in results:
                    t = (time.mktime(result.eventdate.timetuple()) - 18000) * 1000
                    flag_data[root].append((t, result.flag))

        # ## sort data by date
        flag_data[root].sort(key=lambda tup: tup[0])

    return flag_data


def load_and_pickle_patients_med_data(case_id, med_order):
    """
    This function loads and pickles all patients med data.
    """
    med_data = {}  # stores all med data for patient
    pat_med_results = a_Medication.objects.using('remote').filter(patientvisitid=case_id)
    pat_home_med_results = a_HomeMeds.objects.using('remote').filter(patientvisitid=case_id)
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
    """
    procedure_data = {}  # stores all flag data for patient
    pat_procedure_data = a_Surgical.objects.using('remote').filter(patientvisitid=case_id)
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
    """
    micro_data = {}  # stores all flag data for patient
    pat_micro_data = a_Micro.objects.using('remote').filter(patientvisitid=case_id)
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
    """
    io_data = {}
    io_dict, discard = load_io(case_id, 1577840461000)  # include all times
    for io_category_data in io_dict:
        io_data[io_category_data['name']] = io_category_data['data']

    return io_data


def load_and_pickle_patients_demo_data(case_id):
    """
    This function loads and pickles all patients io and demographic data.
    """
    pat_demo_data = a_demographics.objects.using('remote').get(patientvisitid=case_id)
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


def pickle_patient_data(pickle_dir, feature_dir):
    """
    Creates and stores patient data and root_information as binary
    """
    # ## load root information structures
    if False:  # only needs to be run once. Stored file is loaded on the next statement
        query_and_store_root_information(pickle_dir+'root_info.pickle')
    # ## mtr=mars2root, rtm=root2mars, rtn=root2name, rtt=root2table, rtdt=root2datatype drm=discrete result mapping
    mtr, rtm, groups, lab_group_order, rtn, rtt, rtdt, drm, root_order = \
        load_info_from_pickle_file(pickle_dir+'root_info.pickle')

    # ## load med, procedure, and micro structures
    if False:  # only needs to be run once. Stored file is loaded on the next statement
        query_and_store_other_information(pickle_dir+'other_info.pickle')
    # ## mtt=med2table
    med_order, mtt, procedure_order, micro_order, io_order, demo_order = \
        load_info_from_pickle_file(pickle_dir+'other_info.pickle')

    # ## save feature orders to text files
    if False:
        with open(feature_dir + 'root_feature_columns.txt', 'w') as f:
            f.write('\n'.join(root_order))
        with open(feature_dir + 'med_feature_columns.txt', 'w') as f:
            f.write('\n'.join(med_order))
        with open(feature_dir + 'procedure_feature_columns.txt', 'w') as f:
            f.write('\n'.join(procedure_order))
        with open(feature_dir + 'micro_feature_columns.txt', 'w') as f:
            f.write('\n'.join(micro_order))
        with open(feature_dir + 'io_feature_columns.txt', 'w') as f:
            f.write('\n'.join(io_order))
        with open(feature_dir + 'demo_feature_columns.txt', 'w') as f:
            f.write('\n'.join(demo_order))

    # ## load patient list
    patient_order, patient_days, patient_cut_times = load_case_day_mapping()  # this is a pre-generated file

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

