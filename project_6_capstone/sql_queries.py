create_f_immigrations = """
CREATE TABLE IF NOT EXISTS public.f_immigration (
    cicid    FLOAT PRIMARY KEY,
    year     FLOAT,
    month    FLOAT,
    city_code    FLOAT,
    state_code      FLOAT,
    mode     FLOAT,
    visa     FLOAT,
    arrive_date  FLOAT,
    departure_date  FLOAT,
    country   VARCHAR(1)
);
"""

drop_f_immigrations = "DROP TABLE IF EXISTS immigrations;"

f_immigration_insert = ("""
INSERT INTO immigrations (cicid, year, month, city_code, state_code, mode, visa, arrive_date, departure_date, country) \
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")

create_dim_imm_personal = """
CREATE TABLE IF NOT EXISTS public.demographics (
    imm_personal_id       INT PRIMARY KEY,
    cic_id                 FLOAT,
    citzen_country         VARCHAR,
    residence_country      VARCHAR,
    birth_year             INT,
    gender                 VARCHAR,
    ins_num                FLOAT
);
"""

drop_dim_imm_personal = "DROP TABLE IF EXISTS dim_imm_personal;"

dim_imm_personal_insert = """
INSERT INTO dim_imm_personal (imm_personal_id, cic_id, citzen_country, residence_country, birth_year, gender, ins_num) 
VALUES (%s, %s, %s, %s, %s, %s, %s)"""


create_dim_temperature = """
CREATE TABLE IF NOT EXISTS temperature (
    dt                     DATE,
    city_code              FLOAT,
    state_code             FLOAT,
    avg_temp               FLOAT,
    avg_temp_uncertainty   FLOAT,
    year                   FLOAT,
    month                  FLOAT
);
"""

dim_temperature_insert = ("""
INSERT INTO temperature (dt, city_code, state_code, avg_temp, avg_temp_uncertainty, year, month) 
VALUES (%s, %s, %s, %s, %s, %s, %s)""")

drop_dim_temperature = "DROP TABLE IF EXISTS dim_temperature;"

drop_table_queries = [drop_f_immigrations, drop_dim_imm_personal, drop_dim_temperature]
create_table_queries = [create_f_immigrations, create_dim_imm_personal, create_dim_temperature]