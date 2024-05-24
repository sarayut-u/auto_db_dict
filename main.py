import psycopg2
import csv

# Database connection
conn = psycopg2.connect(
    dbname="dama86dd4g3vj6",
    user="z_new_user",
    password="MJ4MXjmK4TSK",
    host="staging-db.cas2tln5cone.us-east-1.rds.amazonaws.com",
    port="5732"
)

# Queries to extract schema information
queries = {
    # "tables": "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY table_schema, table_name;",
    # "columns": "SELECT table_schema, table_name, column_name, data_type, character_maximum_length, numeric_precision, is_nullable FROM information_schema.columns WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY table_schema, table_name, ordinal_position;",
    # "primary_keys": "SELECT kcu.table_schema, kcu.table_name, kcu.column_name, tco.constraint_name FROM information_schema.table_constraints tco JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name AND kcu.constraint_schema = tco.constraint_schema AND kcu.constraint_name = tco.constraint_name WHERE tco.constraint_type = 'PRIMARY KEY' ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position;",
    # "foreign_keys": "SELECT tc.table_schema, tc.table_name, kcu.column_name, ccu.table_schema AS foreign_table_schema, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE tc.constraint_type = 'FOREIGN KEY' ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position;",
    # "indexes": "SELECT schemaname as table_schema, tablename as table_name, indexname as index_name, indexdef as index_definition FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY schemaname, tablename, indexname;",
    "cronjobs": "SELECT jobid, schedule, command, nodename, nodeport, database, username FROM cron.job;",
    "triggers": "SELECT n.nspname AS schema_name, c.relname AS table_name, t.tgname AS trigger_name, pg_catalog.pg_get_triggerdef(t.oid) AS trigger_definition FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE NOT t.tgisinternal AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' ORDER BY schema_name, table_name, trigger_name;",
    "functions": "SELECT n.nspname AS schema_name, p.proname AS function_name, pg_catalog.pg_get_functiondef(p.oid) AS function_definition FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE pg_catalog.pg_function_is_visible(p.oid) AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' ORDER BY schema_name, function_name;"
}

# Export data to CSV files
for key, query in queries.items():
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        
        with open(f"{key}.csv", mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)

# Close the database connection
conn.close()