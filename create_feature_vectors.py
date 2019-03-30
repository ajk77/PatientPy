"""
create_feature_vectors.py
version 3.0
package github.com/ajk77/PatientPy
Created by AndrewJKing.com|@andrewsjourney

This file loads stored patient data and constructs feature vectors to represent the patient state. 

---DEPENDENCIES---
Must run patient_pickler.py first to store data structures that are used here.
^ set pkl_dir to the same value as was used in patient_pickler.
Must create a feature directory.
^The following subdirectories must be created as well: 'root_data/','med_data/','procedure_data/','micro_data/','io_data/','demo_data/'
Must create labeled_case_list file and linked participant_info files. See resource folder for examples. 

---TODO---
After connections to both NOC and MIMIC III are established (see patient_pickler TODO)
[] Generalize the project to ensure it works when connected to either NOC or MIMIC. 

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

from patientpy_utils import load_list, load_info_from_pickle_file, load_case_day_mapping, delete_folder_contents
from data_featurizer import *
import pickle


def load_labeled_cases(labeled_case_file, participant_source_dir):
    """
    Returns the the labeled case days so they that features can be extracted for them
        labeled_cases is a list of the cases that were labeled
        labeled_days_dict is a dict [case_id] = labeled day that shows
    """
    labeled_cases = []
    participant_source = []  # participant for each of the labeled_cases (stored in same order)
    labeled_days_dict = {}
    labeled_cut_times_dict = {}
    with open(labeled_case_file, 'r') as f:
        for line in f:
            s_line = line.rstrip().split('\t')
            if len(s_line) == 2:
                if s_line[1] not in labeled_cases:
                    labeled_cases.append(s_line[1])
                    participant_source.append(s_line[0])

    for i in range(len(labeled_cases)):
        with open(participant_source_dir + participant_source[i] + '.txt', 'r') as f:
            for line in f:
                s_line = line.split(',')
                if s_line[0] == labeled_cases[i]:
                    labeled_cut_times_dict[labeled_cases[i]] = s_line[2]
                    labeled_days_dict[labeled_cases[i]] = s_line[4]

    return labeled_cases, labeled_days_dict, labeled_cut_times_dict


def create_complete_feature_files_main(feature_dir, pickle_dir, labeled_cases_data_3, case_day_data_3, root_mapping_data_9, med_mapping_data_6, reset_case_order=True):
    """
    Creates the feature vector files for each sample
    """
    # ## expand parameters
    labeled_cases, labeled_days_dict, labeled_cut_times_dict = labeled_cases_data_3
    patient_order, patient_days, patient_cut_times = case_day_data_3
    mtr, rtm, groups, lab_group_order, rtn, rtt, rtdt, drm, root_order = root_mapping_data_9
    med_order, mtt, procedure_order, micro_order, io_order, demo_order = med_mapping_data_6

    def remove_before_cutoff(data, cuttime):
        """
        Returns data with data points occurring before cuttime and data occurring within a day after cuttime
        Data is in the form of [(x, y), (x, y), ...]
        Input data must be sorted by data
        """
        cut_index_1 = len(data)
        cut_index_2 = len(data)
        first_cut_not_found = True
        for w, (x, y) in enumerate(data):
            if x / 1000 > cuttime and first_cut_not_found:  # /1000 to convert ms to seconds
                cut_index_1 = w
                cut_index_2 = w
                first_cut_not_found = False
            else:
                if x / 1000 <= (cuttime + 86400):  # plus 1 day in seconds
                    cut_index_2 = w
                else:
                    break

        return [data[0: cut_index_1], data[cut_index_1: cut_index_2]]


    def get_drm_dict(curr_root, full_drm):
        """
        Returns discrete_result_mapping dictionary for given root
        """
        drm_dict = {}
        if curr_root in full_drm.keys():
            drm_dict = {}
            for cat in range(0, 4):
                for text in full_drm[curr_root][cat]:
                    drm_dict[text] = cat + 1
        return drm_dict

    # ## save feature orders to text files
    if True:
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

    if reset_case_order:
        # ## reset case order (linked to print case order below, where each case is appended ('a+'))
        with open(feature_dir + 'case_order_rows.txt', 'w') as f:
            f.write('')

    # ## for each patient case
    first_iteration = True  # used for deleting old files
    for idx, curr_pid in enumerate(patient_order):
        # ## check if current case is in the labeled case list; skip if not
        if curr_pid not in labeled_cases:
            continue
        # ## print details of progress
        if idx % 25 == 0:  # print progress
            print curr_pid, '\t'

        # #############    Print case order (rows)         ############# #
        if reset_case_order:
            for q in range(len(patient_days[curr_pid])):  # q is the index during a patient's los
                if patient_days[curr_pid][q] != labeled_days_dict[curr_pid]:  # used to filter days to only labeled ones
                    continue
                else:
                    # ## print case order
                    with open(feature_dir + 'case_order_rows.txt', 'a+') as f:
                        f.write(curr_pid + '\n')

        # #############    ROOT features extraction        ############# #
        if True:  # can be used to skip root feature generation
            root_feature_files = feature_dir + 'root_feature_files/'
            # ## delete old root feature files
            if first_iteration:
                delete_folder_contents(root_feature_files)

            # ## load all patient's data
            f = open(pickle_dir + 'root_data/' + curr_pid + '.pickle', 'rb')
            curr_p_data = pickle.load(f)
            f.close()
            # ## load all patient's flag data
            f = open(pickle_dir + 'flag_data/' + curr_pid + '.pickle', 'rb')
            curr_p_flag_data = pickle.load(f)
            f.close()

            p_day_data = {}  # data up to timecut for each day
            p_plus_day_data = {}  # data 24 hours after timecut for each day
            p_day_flag_data = {}  # data on test result flags (e.g. high, low, abnormal)

            # ## for each patient day
            for q in range(len(patient_days[curr_pid])):  # q is the index during a patient's los
                if patient_days[curr_pid][q] != labeled_days_dict[curr_pid]:  # used to filter days to only labeled ones
                    continue

                # ## reduce data to day and day after
                for root in root_order:
                    p_day_data[root], p_plus_day_data[root] = remove_before_cutoff(
                        curr_p_data[root], patient_cut_times[curr_pid][q])
                    p_day_flag_data[root], unused = remove_before_cutoff(
                        curr_p_flag_data[root], patient_cut_times[curr_pid][q])

                # ## ever_occurred ## #
                # if False:
                with open(root_feature_files + 'root-ever_occurred.txt', 'a+') as f:
                    result_list = [ever_occurred(p_day_data[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## last_value, second_to_last_value, first_value ## #
                # if False:
                features = ['root-last_value', 'root-second_to_last_value', 'root-first_value']
                params = [-1, -2, 0]
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [return_position_value(p_day_data[root], params[i], get_drm_dict(root, drm),
                                                             rtdt[root]) for root in root_order]
                        f.write(','.join(result_list) + '\n')

                # ## categorical last_value, second_to_last_value, first_value ## #
                # if False:
                features = ['root-last_value', 'root-second_to_last_value', 'root-first_value']
                params = [-1, -2, 0]
                for i in range(len(features)):
                    for c in range(1, 5):  # four different categories
                        with open(root_feature_files + features[i] + '_' + str(c) + '.txt', 'a+') as f:
                            result_list = [return_position_root_category
                                           (p_day_data[root], params[i], get_drm_dict(root, drm), c, rtdt[root])
                                           for root in root_order]
                            f.write(','.join(result_list) + '\n')

                # ## days_since_last ## #
                # if False:
                with open(root_feature_files + 'root-days_since_last_value.txt', 'a+') as f:
                    result_list = [days_since_position(p_day_data[root], patient_cut_times[curr_pid][q], -1)
                                   for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## new_event_ordered ## #
                # if False:
                with open(root_feature_files + 'root-new_event_ordered.txt', 'a+') as f:
                    result_list = [ever_occurred(p_plus_day_data[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_last_change ## #
                # if False:
                with open(root_feature_files + 'root-days_since_last_change.txt', 'a+') as f:
                    result_list = [days_since_last_change(p_day_data[root], patient_cut_times[curr_pid][q])
                                   for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## count_different_values ## #
                # if False:
                with open(root_feature_files + 'root-count_different_values.txt', 'a+') as f:
                    result_list = [count_different_values(p_day_data[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## event_frequency_variation ## #
                # if False:
                with open(root_feature_files + 'root-event_frequency_variation.txt', 'a+') as f:
                    result_list = [event_frequency_variation(p_day_data[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## baseline_value ## #
                # if False:
                with open(root_feature_files + 'root-baseline_value.txt', 'a+') as f:
                    result_list = [get_baseline_value(p_day_data[root], rtdt[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## apex_value ## #
                # if False:
                with open(root_feature_files + 'root-apex_value.txt', 'a+') as f:
                    result_list = [get_apex_value(p_day_data[root], rtdt[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## nadir_value ## #
                # if False:
                with open(root_feature_files + 'root-nadir_value.txt', 'a+') as f:
                    result_list = [get_nadir_value(p_day_data[root], rtdt[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## difference_between_positions ## #
                # if False:
                features = ['root-diff_between_last_second_last', 'root-diff_between_last_first']
                params = [(-1, -2), (-1, 0)]
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [difference_between_positions(p_day_data[root], params[i][0], params[i][1],
                                                                    rtdt[root]) for root in root_order]
                        f.write(','.join(result_list) + '\n')

                # ## percentage_change_between_positions ## #
                # if False:
                features = ['root-percentage_change_between_last_second_last',
                            'root-percentage_change_between_last_first']
                params = [(-1, -2), (-1, 0)]
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [
                            percentage_change_between_positions(p_day_data[root], params[i][0], params[i][1],
                                                                rtdt[root]) for root in root_order]
                        f.write(','.join(result_list) + '\n')

                # ## slope_between_positions ## #
                # if False:
                features = ['root-slope_between_last_second_last', 'root-slope_between_last_first']
                params = [(-1, -2), (-1, 0)]
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [
                            slope_between_positions(p_day_data[root], params[i][0], params[i][1],
                                                    rtdt[root]) for root in root_order]
                        f.write(','.join(result_list) + '\n')

                # ## difference_between_last_and_value ## #
                # if False:
                features = ['root-diff_between_last_baseline', 'root-diff_between_last_apex',
                            'root-diff_between_last_nadir']
                params = ['baseline', 'apex', 'nadir']
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [difference_between_last_and_value(p_day_data[root], params[i], rtdt[root])
                                       for root in root_order]
                        f.write(','.join(result_list) + '\n')

                # ## percentage_change_between_last_and_value ## #
                # if False:
                features = ['root-percentage_change_between_last_baseline',
                            'root-percentage_change_between_last_apex',
                            'root-percentage_change_between_last_nadir']
                params = ['baseline', 'apex', 'nadir']
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [percentage_change_between_last_and_value(p_day_data[root], params[i],
                                                                                rtdt[root]) for root in root_order]
                        f.write(','.join(result_list) + '\n')

                # ## slope_between_last_and_tuple ## #
                # if False:
                features = ['root-slope_between_last_apex', 'root-slope_between_last_nadir']
                params = ['apex', 'nadir']
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [slope_between_last_and_tuple(p_day_data[root], params[i], rtdt[root])
                                       for root in root_order]
                        f.write(','.join(result_list) + '\n')

                # ## flag_is categories ## #
                # if True:
                features = ['root-flag_is_null', 'root-flag_is_H', 'root-flag_is_L', 'root-flag_is_A']
                params = [None, 'H', 'L', 'A']
                for i in range(len(features)):
                    with open(root_feature_files + features[i] + '.txt', 'a+') as f:
                        result_list = [flag_is_f(curr_p_flag_data[root], rtt[root], params[i]) for root in
                                       root_order]
                        f.write(','.join(result_list) + '\n')

                # ##### additional features taken from limited feature set ##### #
                # ## inverse_days ## #
                # if False:
                with open(root_feature_files + 'root-inverse_days_since_last_value.txt', 'a+') as f:
                    result_list = [inverse_days(p_day_data[root], patient_cut_times[curr_pid][q])
                                   for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## abs_slope ## #
                # if False:
                with open(root_feature_files + 'root-abs_slope_between_last_second_last.txt', 'a+') as f:
                    result_list = [abs_slope(p_day_data[root], rtdt[root]) for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## max_30_hr ## #
                # if False:
                with open(root_feature_files + 'root-max_30_hr.txt', 'a+') as f:
                    result_list = [max_30_hr(p_day_data[root], patient_cut_times[curr_pid][q], rtdt[root])
                                   for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## min_30_hr ## #
                # if False:
                with open(root_feature_files + 'root-min_30_hr.txt', 'a+') as f:
                    result_list = [min_30_hr(p_day_data[root], patient_cut_times[curr_pid][q], rtdt[root])
                                   for root in root_order]
                    f.write(','.join(result_list) + '\n')

                # ## mean_30_hr ## #
                # if False:
                with open(root_feature_files + 'root-mean_30_hr.txt', 'a+') as f:
                    result_list = [mean_30_hr(p_day_data[root], patient_cut_times[curr_pid][q], rtdt[root])
                                   for root in root_order]
                    f.write(','.join(result_list) + '\n')

        # #############  MEDS features extraction       #############  #
        med_feature_files = feature_dir + 'med_feature_files/'
        if True:
            # ## delete old root feature files
            if first_iteration:
                delete_folder_contents(med_feature_files)
            # ## load all patient's data
            f = open(pickle_dir + 'med_data/' + curr_pid + '.pickle', 'rb')
            curr_p_data = pickle.load(f)
            f.close()

            p_day_data = {}  # data up to timecut for each day
            p_plus_day_data = {}  # data 24 hours after timecut for each day

            # ## for each patient day
            for q in range(len(patient_days[curr_pid])):  # q is the index during a patient's los
                if patient_days[curr_pid][q] != labeled_days_dict[curr_pid]:  # used to filter days to only labeled ones
                    continue

                # ## reduce data to day and day after
                for med in med_order:
                    p_day_data[med], p_plus_day_data[med] = remove_before_cutoff(
                        curr_p_data[med], patient_cut_times[curr_pid][q])

                # ## ever_occurred ## #
                # if False:
                with open(med_feature_files + 'med-ever_occurred.txt', 'a+') as f:
                    result_list = [ever_occurred(p_day_data[med]) for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_last_value ## #
                # if False:
                with open(med_feature_files + 'med-days_since_last_value.txt', 'a+') as f:
                    result_list = [days_since_position(p_day_data[med], patient_cut_times[curr_pid][q], -1)
                                   for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_last_change ## #
                # if False:
                with open(med_feature_files + 'med-days_since_last_change.txt', 'a+') as f:
                    result_list = [days_since_last_change(p_day_data[med], patient_cut_times[curr_pid][q])
                                   for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ## event_frequency_variation ## #
                # if False:
                with open(med_feature_files + 'med-event_frequency_variation.txt', 'a+') as f:
                    result_list = [event_frequency_variation(p_day_data[med]) for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_first_value ## #
                # if False:
                with open(med_feature_files + 'med-days_since_first_value.txt', 'a+') as f:
                    result_list = [days_since_position(p_day_data[med], patient_cut_times[curr_pid][q], 0)
                                   for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ## event_is_ongoing ## #
                # if False:
                with open(med_feature_files + 'med-event_is_ongoing.txt', 'a+') as f:
                    result_list = [event_is_ongoing(p_day_data[med], patient_cut_times[curr_pid][q],
                                                    p_plus_day_data[med]) for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ## count_sequential_days_of_event ## #
                # if False:
                with open(med_feature_files + 'med-count_sequential_days_of_event.txt', 'a+') as f:
                    result_list = [count_sequential_days_of_event(p_day_data[med], patient_cut_times[curr_pid][0:q])
                                   for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ## count_sequential_days_of_event ## #
                # if False:
                with open(med_feature_files + 'med-recency_of_sequential_days.txt', 'a+') as f:
                    result_list = [recency_of_sequential_days(p_day_data[med], patient_cut_times[curr_pid][0:q])
                                   for med in med_order]
                    f.write(','.join(result_list) + '\n')

                # ##### additional features taken from limited feature set ##### #
                # ## inverse_days ## #
                # if False:
                with open(med_feature_files + 'med-inverse_days_since_last_value.txt', 'a+') as f:
                    result_list = [inverse_days(p_day_data[med], patient_cut_times[curr_pid][q])
                                   for med in med_order]
                    f.write(','.join(result_list)+'\n')

        # ############# PROCEDURES features extraction  ############# #
        procedure_feature_files = feature_dir + 'procedure_feature_files/'
        if True:
            # ## delete old root feature files
            if first_iteration:
                delete_folder_contents(procedure_feature_files)
            # ## load all patient's data
            f = open(pickle_dir + 'procedure_data/' + curr_pid + '.pickle', 'rb')
            curr_p_data = pickle.load(f)
            f.close()

            p_day_data = {}  # data up to timecut for each day
            p_plus_day_data = {}  # data 24 hours after timecut for each day

            # ## for each patient day
            for q in range(len(patient_days[curr_pid])):  # q is the index during a patient's los
                if patient_days[curr_pid][q] != labeled_days_dict[curr_pid]:  # used to filter days to only labeled ones
                    continue

                # ## reduce data to day and day after
                for root in procedure_order:
                    p_day_data[root], p_plus_day_data[root] = remove_before_cutoff(
                        curr_p_data[root], patient_cut_times[curr_pid][q])

                # ## ever_occurred ## #
                # if False:
                with open(procedure_feature_files + 'procedure-ever_occurred.txt', 'a+') as f:
                    result_list = [ever_occurred(p_day_data[procedure]) for procedure in procedure_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_last_value ## #
                # if False:
                with open(procedure_feature_files + 'procedure-days_since_last_value.txt', 'a+') as f:
                    result_list = [days_since_position(p_day_data[procedure], patient_cut_times[curr_pid][q], -1)
                                   for procedure in procedure_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_first_value ## #
                # if False:
                with open(procedure_feature_files + 'procedure-days_since_first_value.txt', 'a+') as f:
                    result_list = [days_since_position(p_day_data[procedure], patient_cut_times[curr_pid][q], 0)
                                   for procedure in procedure_order]
                    f.write(','.join(result_list) + '\n')

                # ##### additional features taken from limited feature set ##### #
                # ## inverse_days ## #
                # if False:
                with open(procedure_feature_files + 'procedure-inverse_days_since_last_value.txt', 'a+') as f:
                    result_list = [inverse_days(p_day_data[procedure], patient_cut_times[curr_pid][q])
                                   for procedure in procedure_order]
                    f.write(','.join(result_list)+'\n')

        # ############# MICRO features extraction       ############# #
        micro_feature_files = feature_dir + 'micro_feature_files/'
        if True:
            # ## delete old root feature files
            if first_iteration:
                delete_folder_contents(micro_feature_files)
            # ## load all patient's data
            f = open(pickle_dir + 'micro_data/' + curr_pid + '.pickle', 'rb')
            curr_p_data = pickle.load(f)
            f.close()

            p_day_data = {}  # data up to timecut for each day
            p_plus_day_data = {}  # data 24 hours after timecut for each day

            # ## for each patient day
            for q in range(len(patient_days[curr_pid])):  # q is the index during a patient's los
                if patient_days[curr_pid][q] != labeled_days_dict[curr_pid]:  # used to filter days to only labeled ones
                    continue

                # ## reduce data to day and day after
                for root in micro_order:
                    p_day_data[root], p_plus_day_data[root] = remove_before_cutoff(
                        curr_p_data[root], patient_cut_times[curr_pid][q])

                # ## ever_occurred ## #
                # if False:
                with open(micro_feature_files + 'micro-ever_occurred.txt', 'a+') as f:
                    result_list = [ever_occurred(p_day_data[micro]) for micro in micro_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_last_value ## #
                # if False:
                with open(micro_feature_files + 'micro-days_since_last_value.txt', 'a+') as f:
                    result_list = [days_since_position(p_day_data[micro], patient_cut_times[curr_pid][q], -1)
                                   for micro in micro_order]
                    f.write(','.join(result_list) + '\n')

                # ## days_since_first_event ## #
                # if False:
                with open(micro_feature_files + 'micro-days_since_first_value.txt', 'a+') as f:
                    result_list = [days_since_position(p_day_data[micro], patient_cut_times[curr_pid][q], 0)
                                   for micro in micro_order]
                    f.write(','.join(result_list) + '\n')

                # ##### additional features taken from limited feature set ##### #
                # ## inverse_days ## #
                # if False:
                with open(micro_feature_files + 'micro-inverse_days_since_last_value.txt', 'a+') as f:
                    result_list = [inverse_days(p_day_data[micro], patient_cut_times[curr_pid][q])
                                   for micro in micro_order]
                    f.write(','.join(result_list)+'\n')

        # ############# IO features extraction          ############# #
        io_feature_files = feature_dir + 'io_feature_files/'
        if True:
            # ## delete old root feature files
            if first_iteration:
                delete_folder_contents(io_feature_files)
            # ## load all patient's data
            f = open(pickle_dir + 'io_data/' + curr_pid + '.pickle', 'rb')
            curr_p_data = pickle.load(f)
            f.close()

            p_day_data = {}  # data up to timecut for each day
            p_plus_day_data = {}  # data 24 hours after timecut for each day

            # ## for each patient day
            for q in range(len(patient_days[curr_pid])):  # q is the index during a patient's los
                if patient_days[curr_pid][q] != labeled_days_dict[curr_pid]:  # used to filter days to only labeled ones
                    continue

                # ## reduce data to day and day after
                for io in io_order:
                    p_day_data[io], p_plus_day_data[io] = remove_before_cutoff(
                        curr_p_data[io], patient_cut_times[curr_pid][q])

                # ## daily_io_features ## #
                # if False:
                with open(io_feature_files + 'daily_io_features.txt', 'a+') as f:
                    result_list = [get_daily_io_of_type(p_day_data[io], patient_cut_times[curr_pid][q])
                                   for io in io_order]
                    f.write(','.join(result_list) + '\n')

                # ## los_io_features ## #
                # if False:
                with open(io_feature_files + 'los_io_features.txt', 'a+') as f:
                    result_list = [get_los_io_of_type(p_day_data[io]) for io in io_order]
                    f.write(','.join(result_list) + '\n')

        # ############# DEMOGRAPHICS features extraction ############# #
        demo_feature_files = feature_dir + 'demo_feature_files/'
        if True:
            # ## delete old root feature files
            if first_iteration:
                delete_folder_contents(demo_feature_files)
            # ## load all patient's data
            f = open(pickle_dir + 'demo_data/' + curr_pid + '.pickle', 'rb')
            curr_p_data = pickle.load(f)
            f.close()

            # ## for each patient day
            for q in range(len(patient_days[curr_pid])):  # q is the index during a patient's los
                if patient_days[curr_pid][q] != labeled_days_dict[curr_pid]:  # used to filter days to only labeled ones
                    continue

                # ## demographics at admission
                with open(demo_feature_files + 'demo_features.txt', 'a+') as f:
                    result_list = [str(curr_p_data[0][demo]) for demo in demo_order]
                    f.write(','.join(result_list) + '\n')

        # ## set first_iteration to false to stop directory clearing
        if first_iteration:
            first_iteration = False

    return


if __name__ == '__main__':

    # where the data was stored from patient_pickler.py
    root_dir = '//modeling_folder/'
    pkl_dir = '//modeling_folder/all_data_pickle_files/'
    case_day_filename = 'case_day_mapping-01Jan2018.txt'  # expample provided in /resources/load_case_day_mapping-file/
    
    # ## feature directories. Each of the cases below will populate the feature directories for a different set of labeled cases
    # The loaded file must be generated. See resource folder for examples. 
    feature_dir = root_dir + 'complete_feature_files_demo/'
    labeled_cases_data = load_labeled_cases(feature_dir + 'demo_case_list.txt', '//labeling_study/participant_info/')  
    # ^ examples of necessary files provided in /patientpy/resources/load_labled_cases-param1 & load_labeled_cases-participant_files

    # ## load patient case order, patient days, and patient day cut times
    case_day_data = load_case_day_mapping(root_dir+case_day_filename) 
    # ^ [patient_order, patient_days, patient_cuttimes]

    # ## load lab and vital (root) mapping information
    root_mapping_data = load_info_from_pickle_file(pkl_dir+'root_info.pickle')
    # returns mtr, rtm, groups, lab_group_order, rtn, rtt, rtdt, drm, root_order
    # ^[mtr=mars2root, rtm=root2mars, rtn=root2name, rtt=root2table, rtdt=root2datatype drm=discrete result mapping]

    # ## load med, procedure, micro, intake and output, and dempgraphic mapping information
    med_order, mtt, procedure_order, micro_order, io_order, demo_order = load_info_from_pickle_file(pkl_dir+'other_info.pickle')
    # returns med_order, mtt, procedure_order, micro_order, io_order, demo_order
    # ^mtt=med2table

    create_complete_feature_files_main(feature_dir, labeled_cases_data, case_day_data, root_mapping_data, med_mapping_data, True)
    ''' ^
        (feature_dir, [labeled_cases, labeled_days_dict, labeled_cut_times_dict], [patient_order, patient_days, patient_cuttimes], 
        [mtr, rtm, groups, lab_group_order, rtn, rtt, rtdt, drm, root_order], [med_order, mtt, procedure_order, micro_order, io_order, demo_order], 
        reset_case_order=True) 
    '''
