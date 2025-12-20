
create_table_sql = """
        CREATE TABLE IF NOT EXISTS adzuna_jobs (
            id BIGSERIAL UNIQUE,
            job_id TEXT PRIMARY KEY NOT NULL,
            company_display_name TEXT,
            location TEXT,
            title TEXT,
            latitude DECIMAL,
            longitude DECIMAL,
            redirect_url TEXT,
            description TEXT,
            category_tag TEXT,
            contract_time TEXT,
            created TEXT
        );
        """

insert_sql = """
INSERT INTO adzuna_jobs (
    job_id,
    company_display_name,
    location,
    title,
    latitude,
    longitude,
    redirect_url,
    description,
    category_tag,
    contract_time,
    created
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (job_id) DO NOTHING;
"""

