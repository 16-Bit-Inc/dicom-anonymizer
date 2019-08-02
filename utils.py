from __future__ import print_function

import os
import psutil

import time
import datetime

import json


def load_json(file_name):
    with open(file_name) as data_file:
        data = json.load(data_file)
    return data


def save_json(data, file_name):
    with open(file_name, 'w') as outfile:
        json.dump(obj=data, fp=outfile, sort_keys=True, indent=4, separators=(',', ': '))


def load_link_log(logger, link_log_path, file_name, message):
    if (link_log_path is not None) and os.path.exists(os.path.join(link_log_path, file_name)):
        link_dict = load_json(os.path.join(link_log_path, file_name))
    else:
        link_dict = {}
        print(message)
        logger.info(message)
    return link_dict


def calculate_age(study_date, dob):
    if study_date and dob:
        d1 = datetime.datetime.strptime(study_date, "%Y%m%d")
        d2 = datetime.datetime.strptime(dob, "%Y%m%d")
        a = abs((d1 - d2).days)/365
        # format with leading 0
        age = str('%03d' % a)+'Y'
    else:
        age = ''
    return age


def clean_string(string):
    chars_to_remove = ['/', '(', ')', '^', '[', ']', ';', ':']
    for char in chars_to_remove:
        string = string.replace(char, "")
    string = string.replace(" ", "-")
    return string


def calculate_space(user_space, out_dir):
    # Make sure space provided by user does not exceed available disk space
    free_disk_space = float(psutil.disk_usage(out_dir).free)

    space = min(user_space, free_disk_space)

    # As a precautionary measure, account for the 1.024 to 1.000 byte ratio for every 10**3 "metric" bytes.
    # (Difference between "metric system" (powers of 10) vs "computer byte system" (powers of 2).)
    # This factor is 1.024**3 at GB scale (1.024 for every 10**3 "metric" bytes).
    # TODO: Verify if this precautionary measure is absolutely necessary.
    space /= (1.024**3)

    # Further precautionary reduction, for example in the case where a dedicated segment of drive is preserved.
    space -= (10**9)

    return space


def find_max(link_dict):
    if link_dict:
        max_value = max(link_dict.values())
    else:
        max_value = 0
    return max_value


def calculate_progress(count, file_num, start_time):
    end_cycle_time = time.time()
    time_left = (end_cycle_time - start_time)/count*(file_num - count)/60.0
    print("---------------" + str(round((count/float(file_num))*100.0, 1)) + "% Complete (" + str(count) + "/" + str(file_num) + " | " + str(round(time_left, 1)) + " minutes remaining)----------------------")
