import sqlite3
import sys
import os


def create_connection(db_file):
    """create a database connection to the SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn


def insert_data(conn, table, data):
    """insert data into table"""
    try:
        c = conn.cursor()
        placeholders = ", ".join(["?"] * len(data))
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


def create_database(conn):
    if conn is not None:
        # Create tables
        try:
            c = conn.cursor()
            c.execute(
                """CREATE TABLE IF NOT EXISTS "alembic_version" (
	"version_num"	VARCHAR(32) NOT NULL,
	CONSTRAINT "alembic_version_pkc" PRIMARY KEY("version_num")
)
"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "branches" (
	"id"	CHAR(32) NOT NULL,
	"project_id"	CHAR(32) NOT NULL,
	"created_at"	DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
	"name"	VARCHAR NOT NULL,
	CONSTRAINT "pk_branches" PRIMARY KEY("id"),
	CONSTRAINT "fk_branches_project_id_projects" FOREIGN KEY("project_id") REFERENCES "projects"("id") ON DELETE CASCADE
)
"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "exec_logs" (
	"id"	INTEGER NOT NULL,
	"branch_id"	CHAR(32) NOT NULL,
	"project_state_id"	CHAR(32),
	"started_at"	DATETIME NOT NULL,
	"duration"	FLOAT NOT NULL,
	"cmd"	VARCHAR NOT NULL,
	"cwd"	VARCHAR NOT NULL,
	"env"	JSON NOT NULL,
	"timeout"	FLOAT,
	"status_code"	INTEGER,
	"stdout"	VARCHAR NOT NULL,
	"stderr"	VARCHAR NOT NULL,
	"analysis"	VARCHAR NOT NULL,
	"success"	BOOLEAN NOT NULL,
	CONSTRAINT "pk_exec_logs" PRIMARY KEY("id"),
	CONSTRAINT "fk_exec_logs_branch_id_branches" FOREIGN KEY("branch_id") REFERENCES "branches"("id") ON DELETE CASCADE,
	CONSTRAINT "fk_exec_logs_project_state_id_project_states" FOREIGN KEY("project_state_id") REFERENCES "project_states"("id") ON DELETE SET NULL
)"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "file_contents" (
	"id"	VARCHAR NOT NULL,
	"content"	VARCHAR NOT NULL,
	CONSTRAINT "pk_file_contents" PRIMARY KEY("id")
)"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "files" (
	"id"	INTEGER NOT NULL,
	"project_state_id"	CHAR(32) NOT NULL,
	"content_id"	VARCHAR NOT NULL,
	"path"	VARCHAR NOT NULL,
	"meta"	JSON NOT NULL DEFAULT '{}',
	CONSTRAINT "pk_files" PRIMARY KEY("id"),
	CONSTRAINT "uq_files_project_state_id" UNIQUE("project_state_id","path"),
	CONSTRAINT "fk_files_content_id_file_contents" FOREIGN KEY("content_id") REFERENCES "file_contents"("id") ON DELETE RESTRICT,
	CONSTRAINT "fk_files_project_state_id_project_states" FOREIGN KEY("project_state_id") REFERENCES "project_states"("id") ON DELETE CASCADE
)"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "llm_requests" (
	"id"	INTEGER NOT NULL,
	"branch_id"	CHAR(32) NOT NULL,
	"project_state_id"	CHAR(32),
	"started_at"	DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
	"agent"	VARCHAR,
	"provider"	VARCHAR NOT NULL,
	"model"	VARCHAR NOT NULL,
	"temperature"	FLOAT NOT NULL,
	"messages"	JSON NOT NULL,
	"prompts"	JSON NOT NULL DEFAULT '[]',
	"response"	VARCHAR,
	"prompt_tokens"	INTEGER NOT NULL,
	"completion_tokens"	INTEGER NOT NULL,
	"duration"	FLOAT NOT NULL,
	"status"	VARCHAR NOT NULL,
	"error"	VARCHAR,
	CONSTRAINT "pk_llm_requests" PRIMARY KEY("id"),
	CONSTRAINT "fk_llm_requests_branch_id_branches" FOREIGN KEY("branch_id") REFERENCES "branches"("id") ON DELETE CASCADE,
	CONSTRAINT "fk_llm_requests_project_state_id_project_states" FOREIGN KEY("project_state_id") REFERENCES "project_states"("id") ON DELETE SET NULL
)"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "project_states" (
	"id"	CHAR(32) NOT NULL,
	"branch_id"	CHAR(32) NOT NULL,
	"prev_state_id"	CHAR(32),
	"specification_id"	INTEGER NOT NULL,
	"created_at"	DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
	"step_index"	INTEGER NOT NULL DEFAULT '1',
	"epics"	JSON NOT NULL,
	"tasks"	JSON NOT NULL,
	"steps"	JSON NOT NULL,
	"iterations"	JSON NOT NULL,
	"relevant_files"	JSON,
	"modified_files"	JSON NOT NULL,
	"run_command"	VARCHAR,
	"action"	VARCHAR,
	"docs"	JSON,
	"knowledge_base"	JSON NOT NULL DEFAULT '{}',
	CONSTRAINT "uq_project_states_branch_id" UNIQUE("branch_id","step_index"),
	CONSTRAINT "pk_project_states" PRIMARY KEY("id"),
	CONSTRAINT "uq_project_states_prev_state_id" UNIQUE("prev_state_id"),
	CONSTRAINT "fk_project_states_branch_id_branches" FOREIGN KEY("branch_id") REFERENCES "branches"("id") ON DELETE CASCADE,
	CONSTRAINT "fk_project_states_prev_state_id_project_states" FOREIGN KEY("prev_state_id") REFERENCES "project_states"("id") ON DELETE CASCADE,
	CONSTRAINT "fk_project_states_specification_id_specifications" FOREIGN KEY("specification_id") REFERENCES "specifications"("id")
)"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "projects" (
	"id"	CHAR(32) NOT NULL,
	"name"	VARCHAR NOT NULL,
	"created_at"	DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
	"folder_name"	VARCHAR NOT NULL,
	CONSTRAINT "pk_projects" PRIMARY KEY("id")
)"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "specifications" (
	"id"	INTEGER NOT NULL,
	"description"	VARCHAR NOT NULL,
	"architecture"	VARCHAR NOT NULL,
	"system_dependencies"	JSON NOT NULL,
	"package_dependencies"	JSON NOT NULL,
	"complexity"	VARCHAR NOT NULL DEFAULT 'hard',
	"example_project"	VARCHAR,
	"templates"	JSON,
	"original_description"	VARCHAR,
	"template_summary"	VARCHAR,
	CONSTRAINT "pk_specifications" PRIMARY KEY("id")
)"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS "user_inputs" (
	"id"	INTEGER NOT NULL,
	"branch_id"	CHAR(32) NOT NULL,
	"project_state_id"	CHAR(32),
	"created_at"	DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP),
	"question"	VARCHAR NOT NULL,
	"answer_text"	VARCHAR,
	"answer_button"	VARCHAR,
	"cancelled"	BOOLEAN NOT NULL,
	CONSTRAINT "pk_user_inputs" PRIMARY KEY("id"),
	CONSTRAINT "fk_user_inputs_branch_id_branches" FOREIGN KEY("branch_id") REFERENCES "branches"("id") ON DELETE CASCADE,
	CONSTRAINT "fk_user_inputs_project_state_id_project_states" FOREIGN KEY("project_state_id") REFERENCES "project_states"("id") ON DELETE SET NULL
)"""
            )
            conn.commit()
            print("Tables created")
        except sqlite3.Error as e:
            print(e)
    else:
        print("Error! cannot create the database connection.")


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ["import", "export"]:
        print("Usage: import_project.py <import/export> <project_name>")
        sys.exit(1)
    mode = sys.argv[1]
    project_name = sys.argv[2]
    if mode == "export":
        database2 = "data/database/pythagora.db"
    else:
        if os.path.exists(f"{project_name}.db"):
            database2 = f"{project_name}.db"
        else:
            print(f"Error: {project_name}.db not found.")
            sys.exit(1)

    database1 = "data/database/pythagora.db"

    if mode == "export":
        database1 = f"{project_name}.db"

    conn2 = create_connection(database2)

    if conn2 is not None:
        try:
            # Query the project from database2
            query_project = f"SELECT * FROM projects WHERE name = '{project_name}'"
            project_record = conn2.cursor().execute(query_project).fetchone()

            if project_record:
                conn1 = create_connection(database1)
                if mode == "export":
                    create_database(conn1)
                # Insert the project into database1
                insert_data(conn1, "projects", project_record)

                # Query and insert branches
                query_branches = (
                    f"SELECT * FROM branches WHERE project_id = '{project_record[0]}'"
                )
                branches_records = conn2.cursor().execute(query_branches).fetchall()

                for branch_record in branches_records:
                    insert_data(conn1, "branches", branch_record)

                # Get the inserted branches' ids
                branch_fetch = (
                    conn1.cursor()
                    .execute(
                        f"SELECT id FROM branches WHERE project_id = '{project_record[0]}'"
                    )
                    .fetchall()
                )
                branch_ids = [row[0] for row in branch_fetch]

                insert_from_ids(conn1, conn2, "project_states", "branch_id", branch_ids)

                # Get the inserted project_states' ids
                project_state_fetch = (
                    conn1.cursor()
                    .execute(
                        f"SELECT id FROM project_states WHERE branch_id IN (\"{', '.join(map(str, branch_ids))}\")"
                    )
                    .fetchall()
                )
                project_state_ids = [row[0] for row in project_state_fetch]

                insert_from_ids(
                    conn1, conn2, "exec_logs", "project_state_id", project_state_ids
                )
                insert_from_ids(
                    conn1, conn2, "llm_requests", "project_state_id", project_state_ids
                )
                insert_from_ids(
                    conn1, conn2, "user_inputs", "project_state_id", project_state_ids
                )
                insert_from_ids(
                    conn1, conn2, "files", "project_state_id", project_state_ids
                )

                # Get the inserted files' content_ids
                content_ids = get_ids(
                    conn2, "content_id", "files", "project_state_id", project_state_ids
                )
                insert_from_ids(conn1, conn2, "file_contents", "id", content_ids)

                specification_ids = get_ids(
                    conn2, "specification_id", "project_states", "id", project_state_ids
                )
                insert_from_ids(conn1, conn2, "specifications", "id", specification_ids)

                if mode == "export":
                    print(
                        f"Export completed to file {project_name}.db\nRun 'python import_project.py import {project_name}' to import the project"
                    )
                else:
                    print("Import completed")

            else:
                print(f"No project found with name {project_name}")
        except sqlite3.Error as e:
            print(e)

    else:
        print("Error! cannot create the database connection.")


if __name__ == "__main__":
    main()
