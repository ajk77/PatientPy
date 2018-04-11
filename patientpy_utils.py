"""
patientpy_utils.py
version 1.0
package patientpy
Created by AndrewJKing.com

This file contains utility functions for the patientpy package.
"""
import pickle
import os
from os import listdir
from os.path import isfile, join


def load_file_list(curr_path):
    """
    Returns a list of each file in curr_path
    """
    only_files = [f for f in listdir(curr_path) if isfile(join(curr_path, f))]
    return only_files


def load_list(curr_path):
    """
    Loads a newline separated file into a list
    """
    loaded_list = []
    with open(curr_path, 'r') as f:
        for full_line in f:
            if full_line[0] == '#':  # skip comment line
                continue
            line = full_line.rstrip()
            if line:  # if line not blank
                loaded_list.append(line)
    return loaded_list


def load_dict(curr_path, key_col=0):
    """
    Loads a comma delimited file into dict
    """
    return_dict = {}
    temp_list = load_list(curr_path)
    for row in temp_list:
        s_row = row.split(',')
        if len(s_row) > max(key_col, 1):
            return_dict[s_row[key_col]] = s_row[:key_col] + s_row[key_col+1:]
    return return_dict


def load_info_from_pickle_file(pickle_file):
    """
    This file loads and returns information from the pickle file
    """
    with open(pickle_file, 'rb') as f:
        pickle_data = pickle.load(f)
    return pickle_data


def sr(val, round_to=3):
    """
    Rounds a number and casts it to a string
    """
    return str(round(val, round_to))


def delete_folder_contents(folder_path):
    """
    This function deletes all of a folders contents
    """
    for the_file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)
    return


def load_case_day_mapping(case_day_file):
    """
    Loads data from a file that contains each case day in the lab_739 dataset.
    That file was created with the one_time_scripts.py determine_case_times()
    """
    patient_order = []  # the order of the patients
    patient_days = {}
    patient_cuttimes = {}
    
    loaded_list = load_list(case_day_file)
    
    for idx in range(len(loaded_list)):
        split_line = loaded_list[idx].split(',')
        if len(split_line) == 3:
            if split_line[0] not in patient_order:  # new case line
                patient_order.append(split_line[0])
                patient_days[split_line[0]] = []
                patient_cuttimes[split_line[0]] = []
            patient_days[split_line[0]].append(split_line[1])
            patient_cuttimes[split_line[0]].append(float(split_line[2]))
    return [patient_order, patient_days, patient_cuttimes]
