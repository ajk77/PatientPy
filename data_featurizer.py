"""
data_featurizer.py
version 3.0
package github.com/ajk77/PatientPy
Created by AndrewJKing.com

This is a feature construction file.
Input is typically, data -> [time, value), (time, value), ...] sorted by time.

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

from math import log10

def ever_occurred(data):
    """
    Returns 1 if any instance before cut_off, false 0
    """
    if len(data):
        return '1'
    else:
        return '0'


def return_position_value(data, pos, drm_dict, data_type):
    """
    In sorted list, returns value in position pos
    Used for
        last_value
        second_to_last_value
        first_value
    """
    try:
        if data_type == 'interval':
            return str(data[pos][1])
        elif data_type == 'ordinal' or data_type == 'ordinal ':
            return str(drm_dict[data[pos][1]])
        elif data_type == 'binary':
            if data[pos][1] in drm_dict:
                return '0'
            else:
                return '1'
    except (IndexError, KeyError):
        pass

    return ''


def return_position_root_category(data, pos, drm_dict, c, data_type):
    """
    Returns 1 if value in position pos is equal in discrete value mapping drm category c, otherwise 0
    Used for
        last_value_category_1 through 4
        second_to_last_category_1 through 4
        first_value_category_1 through 4
    """
    try:
        if data_type == 'nominal':
            if data[pos][1] in drm_dict and drm_dict[data[pos][1]] == c:
                return '1'
            else:
                return '0'
    except (IndexError, KeyError):
        pass

    return '0'  # 0 as default for encoded category


def days_since_position(data, cut_time, pos):
    """
    Returns days since the value in position pos occurred
    """
    try:
        second_diff = cut_time - (data[pos][0]/1000)
        return str(second_diff / 60 / 60 / 24)  # seconds/minutes/hours/day
    except IndexError:
        pass
    return ''


def days_since_last_change(data, cut_time):
    """
    Returns days since the last new value of this root type
    """
    try:
        value = data[-1][1]
        for i in range(len(data)-1, -1, -1):
            if data[i][1] != value:
                last_change = i + 1  # We care when last newest value occurred, so need to find the one before it 1st
                return days_since_position(data, cut_time, last_change)  # will return on 1st occurrence to break loop
    except IndexError:
        pass
    return ''


def count_different_values(data):
    """
    Returns a count of the different values in data
    """
    return str(len(set([y for x, y in data])))


def event_frequency_variation(data):
    """
    Returns average time between values divided by time in between the most recent two values
    """
    if len(data) > 1:  # needs to be at least 2 to calculate a diff
        differences = []
        last_time = False
        for x, y in data:
            if not last_time:
                last_time = x
            else:
                differences.append(x - last_time)
        average_diff = float(sum(differences))/len(differences)
        last_diff = data[-1][0]/data[-2][0]
        return str(average_diff / last_diff / 1000 / 60 / 60 / 24)  # convert to days
    else:
        return ''


def get_baseline_value(data, data_type):
    """
    Returns baseline (average) value
    """
    if data_type == 'interval':
        if len(data):
            total = float(sum([y for x, y in data]))
            return str(total/len(data))
    return ''


def get_apex_tuple(data, data_type):
    """
    Returns tuple of apex (max) value
    Favors more recent if tie
    """
    if data_type == 'interval':
        max_tuple = ''
        for i in range(len(data)-1, -1, -1):
            if i == len(data)-1:  # first iteration
                max_tuple = data[i]
            elif data[i][1] > max_tuple[1]:  # if older values are larger
                max_tuple = data[i]
        return max_tuple
    return ''


def get_apex_value(data, data_type):
    """
    Returns apex value after calling get_apex_tuple
    """
    apex_tuple = get_apex_tuple(data, data_type)
    if len(apex_tuple):
        return str(apex_tuple[1])
    else:
        return ''


def get_nadir_tuple(data, data_type):
    """
    Returns tuple of apex (min) value
    Favors more recent if tie
    """
    if data_type == 'interval':
        min_tuple = ''
        for i in range(len(data)-1, -1, -1):
            if i == len(data)-1:  # first iteration
                min_tuple = data[i]
            elif data[i][1] < min_tuple[1]:  # if older values are smaller
                min_tuple = data[i]
        return min_tuple
    return ''


def get_nadir_value(data, data_type):
    """
    Returns nadir value after calling get_nadir_tuple
    """
    nadir_tuple = get_nadir_tuple(data, data_type)
    if len(nadir_tuple):
        return str(nadir_tuple[1])
    else:
        return ''


def difference_between_positions(data, position1, position2, data_type):
    """
    Returns difference between position p1 and position p2
    Used for
        difference_between_last_second_last, _last_first
    """
    if data_type == 'interval':
        if len(data) > 1:
            return str(data[position1][1] - data[position2][1])
    return ''


def percentage_change_between_positions(data, position1, position2, data_type):
    """
    Returns the absolute value of the percentage change between position p1 and position p2
    Used for
        percentage_change_between_last_second_last, _last_first
    """
    if data_type == 'interval':
        if len(data) > 1:
            if data[position2][1]:  # check for not divide by 0
                return str((data[position1][1] - data[position2][1]) / data[position2][1] * 100)
    return ''


def slope_between_positions(data, position1, position2, data_type):
    """
    Returns slope between position p1 and position p2
    Used for
        slope_between_last_second_last, _last_first
    """
    if data_type == 'interval':
        if len(data) > 1:
            rise = (data[position1][1] - data[position2][1])
            run = (data[position1][0] - data[position2][0]) / 1000 / 60 / 60 / 24  # convert to days
            if run:  # check for not divide by 0
                return str(rise/run)
    return ''


def difference_between_last_and_value(data, value_type, data_type):
    """
    Returns difference between last value and value_type value
    """
    if data_type == 'interval':
        if len(data):
            value = ''
            if value_type == 'baseline':
                value = get_baseline_value(data, data_type)
            elif value_type == 'apex':
                value = get_apex_value(data, data_type)
            elif value_type == 'nadir':
                value = get_nadir_value(data, data_type)
            return str(data[-1][1] - float(value))
    return ''


def percentage_change_between_last_and_value(data, value_type, data_type):
    """
    Returns absolute value of the percentage change between last value and value_type value
    """
    if data_type == 'interval':
        if len(data):
            value = ''
            if value_type == 'baseline':
                value = get_baseline_value(data, data_type)
            elif value_type == 'apex':
                value = get_apex_value(data, data_type)
            elif value_type == 'nadir':
                value = get_nadir_value(data, data_type)

            value = float(value)
            if value:  # check for not divide by 0
                return str((data[-1][1] - value) / value * 100)
    return ''


def slope_between_last_and_tuple(data, tuple_type, data_type):
    """
    Returns slope between last tuple and tuple_type tuple
    """
    if data_type == 'interval':
        if len(data):
            curr_tuple = ''
            if tuple_type == 'apex':
                curr_tuple = get_apex_tuple(data, data_type)
            elif tuple_type == 'nadir':
                curr_tuple = get_nadir_tuple(data, data_type)

            if len(curr_tuple):  # check for tuple exists
                rise = (data[-1][1] - curr_tuple[1])
                run = (data[-1][0] - curr_tuple[0]) / 1000 / 60 / 60 / 24  # convert to days
                if run:  # check for not divide by 0
                    return str(rise / run)
    return ''


def flag_is_f(flag_data, data_table, f):
    """
    Returns 1 if most recent flag is equal to flag f, otherwise 0
    Used for
        flag_is_null H, L, and A
    """
    if data_table == 'lab_739':
        if len(flag_data):
            if flag_data[-1][1] == f or flag_data[-1][1] is f:  # the or is needed for when f is None
                return '1'
            else:
                return '0'
    return '0'


def event_is_ongoing(data, cut_time, data_plus):
    """
    Returns 1 if event occurred within the last 24 hours and in the next 24 hours, otherwise 0
    Theory here is that we would know what orders are pending
    """
    days_since_last = days_since_position(data, cut_time, -1)
    if days_since_last:
        if 0 < float(days_since_last) < 1 and int(ever_occurred(data_plus)) == 1:
            return '1'
        else:
            return '0'
    else:
        return '0'


def count_sequential_days_of_event(data, cut_times):
    """
    Returns the number of days this event occurred in a row starting with the most recent day
    """
    count_seq_days = 0
    for i in range(len(cut_times)-1, 0, -1):
        not_found = True
        for (t, value) in data:
            if not_found and cut_times[i-1] < t/1000 < cut_times[i]:
                not_found = False
        if not_found:  # day without a data pint
            break
        else:  # there was a day with a data point
            count_seq_days += 1

    return str(count_seq_days)


def recency_of_sequential_days(data, cut_times):
    """
    Returns one over the square root of the number of days this event occurred.
    Rational: the newer a standing order, the more likely it is to be relevant
    """
    count_seq_days = float(count_sequential_days_of_event(data, cut_times))
    if count_seq_days:
        count_seq_days = 1/(count_seq_days**0.5)
    return str(count_seq_days)


def get_daily_io_of_type(data, t):
    """
    Returns the last 24 hour value of io type t.
    """
    if data:
        if (t - data[-1][0]/1000) == 28800.0:
            return str(data[-1][1])
        else:
            return '0'
    return ''


def get_los_io_of_type(data):
    """
    Returns length of stay net value of io type t
    """
    if data:
        net = 0
        for (t, value) in data:
            net += value
    else:
        net = '0'
    return str(net)


def inverse_days(data, cut_time):
    """
    Returns 1 / days since event, where within last 24 hrs = 1, never = 0
    """
    try:
        second_diff = cut_time - (data[-1][0]/1000)
        day_diff = max((second_diff / 60 / 60 / 24), 1)  # seconds/minutes/hours/day; round up to full day
        return str(1.0/day_diff)
    except IndexError:  # if there is no data of this type
        pass
    return '0'


def abs_slope(data, data_type='interval'):
    """
    Returns the absolute value of the slope between the last two values or 0
    """
    if data_type == 'interval':
        if len(data) > 1:
            rise = (data[-1][1] - data[-2][1])
            run = (data[-1][0] - data[-2][0]) / 1000.0 / 60 / 60 / 24  # convert to days
            if run:  # check for not divide by 0
                return str(abs(rise/run))
    return '0'


def max_30_hr(data, cut_time, data_type='interval'):
    """
    Returns the max value that occurred in the last 30 hours or null
    """
    if data_type == 'interval':
        first = True
        max_val = ''
        ms_2_hrs = (60 * 60 * 24)
        for (t, val) in data:
            if (cut_time - t/1000)/ms_2_hrs <= 1.25:  # within last 30 hours
                if first:
                    max_val = val
                    first = False
                else:
                    max_val = max(val, max_val)
        return str(max_val)
    return ''


def min_30_hr(data, cut_time, data_type='interval'):
    """
    Returns the min value that occurred in the last 30 hours or null
    """
    if data_type == 'interval':
        first = True
        min_val = ''
        ms_2_hrs = (60 * 60 * 24)
        for (t, val) in data:
            if (cut_time - t/1000)/ms_2_hrs <= 1.25:  # within last 30 hours
                if first:
                    min_val = val
                    first = False
                else:
                    min_val = min(val, min_val)
        return str(min_val)
    return ''


def mean_30_hr(data, cut_time, data_type='interval'):
    """
    Returns the mean value that occurred in the last 30 hours or null
    """
    if data_type == 'interval':
        first = True
        min_val = ''
        ms_2_hrs = (60 * 60 * 24)
        values = []
        for (t, val) in data:
            if (cut_time - t/1000)/ms_2_hrs <= 1.25:  # within last 30 hours
                values.append(val)
        if len(values):
            return str(float(sum(values))/len(values))
        else:
            return ''
    return ''
