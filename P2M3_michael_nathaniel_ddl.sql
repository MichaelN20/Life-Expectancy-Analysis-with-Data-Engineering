'''
=================================================
Program ini dibuat untuk melakukan automisasi query database
seperti DDL, DML, TCL, DCL, dan DQL untuk datanya dan
menghasilkan query untuk membuat tabel tertentu yang akan
digunakan untuk bahan analisa.
=================================================
'''

-- Point start transactional block query
BEGIN;

-- Membuat tabel bernama 'table_m3'
CREATE TABLE table_m3 (
    "ID" INT NOT NULL PRIMARY KEY,
    "Age" INT,
    "Gender" VARCHAR(255),
    "Bedtime" VARCHAR(255),
    "Wakeup time" VARCHAR(255),
    "Sleep duration" FLOAT,
    "Sleep efficiency" FLOAT,
    "REM sleep percentage" INT,
    "Deep sleep percentage" INT,
    "Light sleep percentage" INT,
    "Awakenings" FLOAT,
    "Caffeine consumption" FLOAT,
    "Alcohol consumption" FLOAT,
    "Smoking status" VARCHAR(255),
    "Exercise frequency" FLOAT
);

-- Menginput data dari file '.csv' kedalam tabel 'table_m3'
COPY table_m3
FROM 'C:\tmp\P2M3_michael_nathaniel_data_raw.csv'
DELIMITER ','
CSV HEADER;

-- Point start transactional block query
COMMIT;