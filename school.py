import sqlite3
import pandas as pd


def genDepartment(cursor):
    query = """
        CREATE TABLE Department(
            dep_id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            chair_name VARCHAR(255),
            faculty_count INT,
            CHECK (faculty_count > 0),
            CHECK (name LIKE 'Department %')
        );
    """

    cursor.execute(query)


def genStudent(cursor):
    query = """
        CREATE TABLE Student(
            stud_id INTEGER PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            initial VARCHAR(255) NOT NULL,
            CHECK (initial LIKE '__%')
        );
    """
    cursor.execute(query)


def genEvent(cursor):
    query = """
        CREATE TABLE Event(
            event_id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            CHECK (start_date < end_date),
            CHECK ('2021-12-05' < start_date),
            CHECK ('2021-12-05' < end_date)
        );
    """
    cursor.execute(query)


def genMajor(cursor):
    query = """
        CREATE TABLE Major(
            major_id VARCHAR(8) PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            dep_id INTEGER,
            FOREIGN KEY(dep_id) REFERENCES Department(dep_id) 
                ON DELETE SET NULL 
                ON UPDATE CASCADE,
            CHECK (major_id LIKE '___')
        );
    """
    cursor.execute(query)


def genMajorRecord(cursor):
    query = """
        CREATE TABLE MajorRecord(
            major_id VARCHAR(8),
            stud_id INTEGER,
            PRIMARY KEY (major_id, stud_id),
            FOREIGN KEY(major_id) REFERENCES Major(major_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            FOREIGN KEY(stud_id) REFERENCES Student(stud_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );
    """
    cursor.execute(query)


def genEventHost(cursor):
    query = """
        CREATE TABLE EventHost(
            event_id INTEGER,
            dep_id INTEGER,
            PRIMARY KEY(event_id, dep_id),
            FOREIGN KEY(event_id) REFERENCES Event(event_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            FOREIGN KEY(dep_id) REFERENCES Department(dep_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );
    """
    cursor.execute(query)


def genEventAttend(cursor):
    query = """
        CREATE TABLE EventAttendance(
            event_id INTEGER,
            stud_id INTEGER,
            PRIMARY KEY (event_id, stud_id),
            FOREIGN KEY(event_id) REFERENCES Event(event_id),
            FOREIGN KEY(stud_id) REFERENCES Student(stud_id)
        );
    """
    cursor.execute(query)


def deleteTables(cursor):
    queries = ["""DELETE FROM Department;""",
               """DELETE FROM Student;""",
               """DELETE FROM Major;""",
               """DELETE FROM MajorRecord;""",
               """DELETE FROM Event;""",
               """DELETE FROM EventHost;""",
               """DELETE FROM EventAttendance;"""]

    for q in queries:
        cursor.execute(q)


def fillTables(cursor):
    query = """
        INSERT INTO Department(name, chair_name, faculty_count)
        VALUES 
            ('Department of Biology', 'John Doe', 100),
            ('Department of Computer Science', 'Dark Bark', 205),
            ('Department of Engineering', 'Eng Ineer', 50),
            ('Department of Mathematics', 'Math Metician', 14),
            ('Department of Chemistry', 'Walter White', 235);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Major(major_id, name, dep_id)
        VALUES 
            ('BIO', 'Biology', 1),
            ('CSC', 'Computer Science', 2),
            ('ECE', 'Electrical Engineering', 3),
            ('MTH', 'Mathematics', 4),
            ('CHM', 'Chemistry', 5);
    """
    cursor.execute(query)

    query = """
        INSERT INTO Student(first_name, last_name, initial)
        VALUES 
            ('Jin', 'Curia', 'JC'),
            ('Red', 'Blue', 'RB'),
            ('Green', 'Black', 'GB'),
            ('John', 'Johnson', 'JJ'),
            ('Rat', 'Ratson', 'RR');
    """
    cursor.execute(query)

    query = """
        INSERT INTO MajorRecord(major_id, stud_id)
        VALUES 
            ('BIO', 1),
            ('CSC', 2),
            ('CSC', 3),
            ('ECE', 4),
            ('MTH', 5);
        """
    cursor.execute(query)

    query = """
        INSERT INTO Event(name, start_date, end_date)
        VALUES 
            ('Christmas Party', '2021-12-25', '2021-12-26'),
            ('Halloween Party', '2022-11-29', '2022-11-30'),
            ('February Jam', '2022-02-04', '2022-02-05'),
            ('June Jam', '2022-06-01', '2022-06-02'),
            ('August Party', '2022-08-01', '2022-08-02');
    """
    cursor.execute(query)

    query = """
        INSERT INTO EventHost(event_id, dep_id)
        VALUES 
            (1,1),
            (2,1),
            (3,3),
            (4,4),
            (5,5);
    """
    cursor.execute(query)

    query = """
        INSERT INTO EventAttendance(event_id, stud_id)
        VALUES 
            (1,1),
            (2,2),
            (3,3),
            (4,4),
            (5,5);
    """
    cursor.execute(query)


def displayTables(cursor):
    queries = ["""SELECT * FROM Department;""",
               """SELECT * FROM Student;""",
               """SELECT * FROM Major;""",
               """SELECT * FROM Event;""",
               """SELECT * FROM MajorRecord;""",
               """SELECT * FROM EventHost;""",
               """SELECT * FROM EventAttendance;"""]
    for q in queries:
        cursor.execute(q)
        # Extract column names from cursor
        column_names = [row[0] for row in cursor.description]

        # Fetch data and load into a pandas dataframe
        table_data = cursor.fetchall()
        df = pd.DataFrame(table_data, columns=column_names)

        # Examine dataframe
        print(df)


def query_db(cursor):
    """
    1. List records of EventAttendance but include student names and event names
    2. List records of declared majors, showing the student name and major name
    3. Event names and the name of their hosts.
    4. List the names of Majors and the department names they belong to.
    5. List students and departments they belong to.
    """
    queries = [
        """
            SELECT Ev.name AS event_name, Stud.first_name, Stud.last_name
            FROM Event Ev, Student Stud, EventAttendance Att
            WHERE Ev.event_id = Att.event_id
            AND Stud.stud_id = Att.stud_id;
        """,
        """
            SELECT Stud.first_name, Stud.last_name, Maj.name AS major_name
            FROM Student Stud, Major Maj, MajorRecord Rec
            WHERE Stud.stud_id = Rec.stud_id
                AND Maj.major_id = Rec.major_id;
        """,
        """
            SELECT Ev.name AS event_name, Host.name AS host_name
            FROM Event Ev, Department Host, EventHost Rec
            WHERE Ev.event_id = Rec.event_id
            AND Host.dep_id = Rec.dep_id;
        """,
        """
            SELECT Maj.major_id AS major_code, Maj.name AS major_name, Dep.name AS dep_name
            FROM Major Maj, Department Dep
            WHERE Maj.dep_id = Dep.dep_id;
        """,
        """
            SELECT Stud.first_name, Stud.last_name, Dep.name AS dep_name
            FROM Student Stud, Department Dep, Major Maj, MajorRecord Rec
            WHERE Stud.stud_id = Rec.stud_id
                AND Maj.major_id = Rec.major_id
                AND Maj.dep_id = Dep.dep_id;
        """]
    for q in queries:
        cursor.execute(q)
        # Extract column names from cursor
        column_names = [row[0] for row in cursor.description]

        # Fetch data and load into a pandas dataframe
        table_data = cursor.fetchall()
        df = pd.DataFrame(table_data, columns=column_names)

        # Examine dataframe
        print(df)


def genTables(cursor):
    genDepartment(cursor)
    genStudent(cursor)
    genMajor(cursor)
    genMajorRecord(cursor)
    genEvent(cursor)
    genEventHost(cursor)
    genEventAttend(cursor)


def main():

    db_connect = sqlite3.connect('school.db')
    cursor = db_connect.cursor()

    # Creates tables based on schema
    genTables(cursor)

    # Inserts values into tables
    fillTables(cursor)

    # This function prints out the tables after inserting the previous values
    # displayTables(cursor)

    query_db(cursor)
    # Commit any changes to the database
    db_connect.commit()

    # Close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    db_connect.close()


main()
