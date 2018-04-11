# patientpy

Patient state construction from clinical databases for machine learning.

## Getting Started

See FeaturesConstructed.md for a list of extracted and constructed features. <br />
Download this package. You will need to set the database connections to your own database structure. <br />
<br />Recommended tables are:<br />
ICUpatients: contains patient ids, ICU admit dates, and ICU discharge dates. <br />
Demographics: contains demographic info (age, sex, height, weight, bmi, race)<br />
Laboratory: contains laboratory test results<br />
Vital Signs: contains vital sign measurements. Might be combined with laboratory data. <br />
Ventilator settings: contains ventilator settings. Might be combined with vitals or laboratory data. <br />
Medications: contains medication order data. Might have a separate table for Home Medications.<br />
Micro biology: contains micro biology data. <br />
Procedures: contains surgical procedures. <br />
Intake and output: contains the intake and output data. <br />
<br />Other useful tables:<br />
A mapping table that maps clinical events, such a glucose test results from different hospitals, together.<br />
A mapping table that maps clinical event codes to human readable names, table membership, group membership, and data type (discrete, interval, binary). <br />
A mapping table that maps discrete clinical event results to boolean values.

### Prerequisites

Standard python packages are used. (Later versions may add peewee for database connectivity).

### Installing

Populate ICUpatients table with only the patients of interest, i.e., patients after selection for location, date, and diagnoses.<br />
Add database connections.<br />
In patient_pickler.py and create_feature_vectors.py, set: root_dir, pkl_dir, and case_day_filename.<br />
In create_feature_vectors.py, set: feature_dir and parameters for load_labeled_cases().<br />
Create directory structure for pkl_dir: create pkl_dir folder and sub folders ('root_data/', 'flag_data/', 'med_data/','procedure_data/', 'micro_data/', 'io_data/',' demo_data/').<br />
Create directory structure for feature_dir: create feature_dir folder and sub folders ('root_data/', 'med_data/', 'procedure_data/', 'micro_data/', 'io_data/', 'demo_data/').<br />
Must create labeled_case_list file and linked participant_info files. See resource folder for examples. <br />
Labeled case list file lists the exact cases of interest. Participant info files provide length of stay cut times. 

## Deployment

Run patient_pickler.py once.<br />
Run create_feature_vectors.py once for each desired patient set, updating feature_dir and load_labeled_cases() parameters each time. 

## Versioning

Version 1.0. For the versions available, see https://github.com/ajk77/patientpy

## Authors

Andrew J King - Doctoral Candidate<br />
Shyam Visweswaran - PI<br />
Gregory F Cooper - Doctoral Advisor 

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Gilles Clermont and Milos Hauskrecht 
