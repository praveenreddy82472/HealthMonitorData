import csv
from google.cloud import storage
from google.cloud import spanner

# Config - replace with your values
GCS_BUCKET = 'healthmont'
GCS_FILE = 'hospital_data_analysis.csv'
INSTANCE_ID = 'healthmono'
DATABASE_ID = 'healthmono'
SPANNER_TABLE = 'PatientInfo'  # e.g. 'Patients'



# Set up Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.bucket('healthmont')
blob1 = bucket.blob('hospital_data_analysis.csv')

# Read CSV file contents
csv_data1 = blob1.download_as_string().decode('utf-8')
csv_reader1 = csv.DictReader(csv_data1.splitlines())


# Set up Cloud Spanner client
spanner_client = spanner.Client()
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)


# Insert data into Cloud Spanner table
with database.batch() as batch:
    for row in csv_reader1:
        try:
            # Convert fields to correct types
            patient_id = int(row["Patient_ID"])
            age = int(row["Age"])
            gender = row["Gender"]
            condition = row["Condition"]
            procedure = row["Procedure"]
            cost = float(row["Cost"])
            length_of_stay = int(row["Length_of_Stay"])
            readmission = row["Readmission"]
            outcome = row["Outcome"]
            satisfaction = int(row["Satisfaction"])

            columns = (
                "Patient_ID",
                "Age",
                "Gender",
                "Condition",
                "Procedure",
                "Cost",
                "Length_of_Stay",
                "Readmission",
                "Outcome",
                "Satisfaction",
            )
            values = [
                (
                    patient_id,
                    age,
                    gender,
                    condition,
                    procedure,
                    cost,
                    length_of_stay,
                    readmission,
                    outcome,
                    satisfaction,
                )
            ]

            batch.insert(table=SPANNER_TABLE, columns=columns, values=values)

        except Exception as e:
            print(f"Error inserting row {row}: {str(e)}")


print('Inserted data.')