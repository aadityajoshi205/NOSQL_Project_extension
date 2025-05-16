import pandas as pd
from sqlalchemy import create_engine

# Load the CSV into a DataFrame
df = pd.read_csv('student_course_grades.csv')

# Database connection string
engine = create_engine('postgresql+psycopg2://nande:050309@localhost:5432/doshte')

# Upload to PostgreSQL
df.to_sql('student_course_grades', engine, if_exists='append', index=False)

print("CSV uploaded to PostgreSQL successfully!")
