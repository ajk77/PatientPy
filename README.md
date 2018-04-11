# patientpy

Patient state construction from clinical databases for machine learning.

## Getting Started

Download this package. You will need to set the database connections to your own database structure. Recommended tables are:
ICUpatients: contains patient ids, ICU admit dates, and ICU discharge dates. 
Demographics: contains demographic info (age, sex, height, weight, bmi, race)
Laboratory: contains laboratory test results
Vital Signs: contains vital sign measurements. Might be combined with laboratory data. 
Ventilator settings: contains ventilator settings. Might be combined with vitals or laboratory data. 
Medications: contains medication order data. Might have a separate table for Home Medications.
Micro biology: contains micro biology data. 
Procedures: contains surgical procedures. 
Intake and output: contains the intake and output data. 
Other useful tables:
A mapping table that maps clinical events, such a glucose test results from different hospitals, together.
A mapping table that maps clinical event codes to human readable names, table membership, group membership, and data type (discrete, interval, binary). 
A mapping table that maps discrete clinical event results to boolean values. 

### Prerequisites

Standard python packages are used. (Later versions may add peewee for database connectivity).

### Installing

Populate ICUpatients table with only the patients of interest, i.e., patients after selection for location, date, and diagnoses.
Add database connections.
In patient_pickler.py and create_feature_vectors.py, set: root_dir, pkl_dir, and case_day_filename.
In create_feature_vectors.py, set: feature_dir and parameters for load_labeled_cases().
Create directory structure for pkl_dir: create pkl_dir folder and sub folders ('root_data/','flag_data/','med_data/','procedure_data/','micro_data/','io_data/','demo_data/').
Create directory structure for feature_dir: create feature_dir folder and sub folders ('root_data/','med_data/','procedure_data/','micro_data/','io_data/','demo_data/').
Must create labeled_case_list file and linked participant_info files. See resource folder for examples. 
Labeled case list file lists the exact cases of interest. Participant info files provide length of stay cut times. 

## Deployment

Run patient_pickler.py once.
Run create_feature_vectors.py once for each desired patient set, updating feature_dir and load_labeled_cases() parameters each time. 

## Versioning

Version 1.0. For the versions available, see https://github.com/ajk77/patientpy

## Authors

Andrew J King - Doctoral Candidate
Shyam Visweswaran - PI
Gregory F Cooper - Doctoral Advisor 

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Gilles Clermont and Milos Hauskrecht 
