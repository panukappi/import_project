import sqlite3
import sys

def create_connection(db_file):
    """ create a database connection to the SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn

def insert_data(conn, table, data):
    """ insert data into table """
    try:
        c = conn.cursor()
        placeholders = ', '.join(['?'] * len(data))
        c.execute(f"INSERT INTO {table} VALUES ({placeholders})", data)
        conn.commit()
        print(f"Updated table '{table}'")
    except sqlite3.Error as e:
        print(e)
        
def insert_from_ids(conn1, conn2, table, id_name, ids):
    records = []
    for id in ids:
        query = f"SELECT * FROM {table} WHERE {id_name} = '{id}'"
        records += conn2.cursor().execute(query).fetchall()
        
    for record in records:
        insert_data(conn1, table, record)
        
def get_ids(conn, select, table, id_name, ids):
    temp = []
    for id in ids:
        query = f"SELECT {select} FROM {table} WHERE {id_name} = '{id}'"
        temp += conn.cursor().execute(query).fetchall()
    temp = set(temp)
    return [row[0] for row in temp]

def main():
    database1 = "data/database/pythagora.db"
    if len(sys.argv) != 3:
        print("Usage: import_project.py <path_to_database> <project_name>")
        sys.exit(1)
    database2 = sys.argv[1]
    project_name = sys.argv[2]
    
    conn1 = create_connection(database1)
    conn2 = create_connection(database2)

    if conn1 is not None and conn2 is not None:
        try:
        # Query the project from database2
            query_project = f"SELECT * FROM projects WHERE name = '{project_name}'"
            project_record = conn2.cursor().execute(query_project).fetchone()

            if project_record:
                # Insert the project into database1
                insert_data(conn1, "projects", project_record)

                # Query and insert branches
                query_branches = f"SELECT * FROM branches WHERE project_id = '{project_record[0]}'"
                branches_records = conn2.cursor().execute(query_branches).fetchall()

                for branch_record in branches_records:
                    insert_data(conn1, "branches", branch_record)

                # Get the inserted branches' ids
                branch_fetch = conn1.cursor().execute(f"SELECT id FROM branches WHERE project_id = '{project_record[0]}'").fetchall()
                branch_ids = [row[0] for row in branch_fetch]
                    
                insert_from_ids(conn1, conn2, "project_states", "branch_id", branch_ids)
                    
                # Get the inserted project_states' ids
                project_state_fetch = conn1.cursor().execute(f"SELECT id FROM project_states WHERE branch_id IN (\"{', '.join(map(str, branch_ids))}\")").fetchall()
                project_state_ids = [row[0] for row in project_state_fetch]
                
                insert_from_ids(conn1, conn2, "exec_logs", "project_state_id", project_state_ids)
                insert_from_ids(conn1, conn2, "llm_requests", "project_state_id", project_state_ids)
                insert_from_ids(conn1, conn2, "user_inputs", "project_state_id", project_state_ids)
                insert_from_ids(conn1, conn2, "files", "project_state_id", project_state_ids)
                
                # Get the inserted files' content_ids
                content_ids = get_ids(conn2, "content_id", "files", "project_state_id", project_state_ids)
                insert_from_ids(conn1, conn2, "file_contents", "id", content_ids)
                
                specification_ids = get_ids(conn2, "specification_id", "project_states", "id", project_state_ids)
                insert_from_ids(conn1, conn2, "specifications", "id", specification_ids)
                    
            else:
                print(f"No project found with name {project_name}")
        except sqlite3.Error as e:
            print(e)
        
    else:
        print("Error! cannot create the database connection.")
    print("Import completed")

if __name__ == '__main__':
    main()