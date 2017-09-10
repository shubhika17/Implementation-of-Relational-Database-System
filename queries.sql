-------------------------------------------------------------------------------
-- Database Setup
-------------------------------------------------------------------------------
-- Database 

DROP INDEX IX_Age;
DROP TABLE Students;
DROP TABLE Courses;
DROP TABLE Grades;
DROP TABLE Foo;

CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);
CREATE TABLE Courses (cid INTEGER, title STRING(50));
CREATE TABLE Grades (gsid INTEGER, gcid INTEGER, points FLOAT);
CREATE TABLE Foo (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);


--start: test create and dop index
CREATE INDEX IX_Age ON Students(Age);
DROP INDEX IX_Age;
CREATE INDEX IX_Age ON Students(Age);

--invalid create index (duplicate index)
CREATE INDEX IX_AGE ON Students(Age);
--invalid create index (bad table name)
CREATE INDEX IX_BAD ON Invalid(sid);
--invalid create index (bad key)
CREATE INDEX IX_BAD2 ON Courses(Bad);
--Cleanup any invalid indexes incorrectly created
DROP INDEX IX_BAD;
DROP INDEX IX_BAD2;
--invalid drop index
DROP INDEX IX_INVALID;
--end: test create and dop index

--start: test describe
DESCRIBE Courses;
--invalid table
DESCRIBE Invalid;
--end: test describe

--start: test insert and select from single table
INSERT INTO Courses VALUES (448, 'DB Fun');
INSERT INTO Courses VALUES (348, 'Less Cool');
INSERT INTO Courses VALUES (542, 'More Fun');

SELECT * FROM Courses;

--test catalog record counts after insert
SELECT * FROM mb_rel WHERE relName = 'Courses';

--invalid insert: bad table
INSERT INTO INVALID VALUES (448, 'DB Fun');
--invalid insert: bad values
INSERT INTO Courses VALUES ('Bad Field', 500);

--Test invalid insert didn't change anything
SELECT * FROM Courses;

--Test projection
SELECT cid FROM Courses;
--test basic selection
SELECT * FROM Courses WHERE cid = 448;

--invalid select: bad table
SELECT cid FROM INVALID;
--invalid select: bad projection
SELECT sid FROM Courses;
--invalid select: invalid predicate
SELECT cid FROM Courses WHERE sid = 500;
--end: test insert and select from single table


--start: test update
UPDATE Courses SET title = 'Cool enough' WHERE cid = 348;
SELECT * FROM Courses;
--invalid update: bad table
UPDATE INVALID SET title = 'Cool enough' WHERE cid = 348;
--invalid update: bad update field
UPDATE Courses SET sid = 500 WHERE cid = 348;
--invalid update: bad update values
UPDATE Courses SET title = 500 WHERE cid = 348;
--invalid update: bad predicates 
UPDATE Courses SET title = 500 WHERE name = 'Invalid';
SELECT * FROM Courses;

--end: test update

--start: test delete
DELETE Courses WHERE cid = 542;
SELECT * FROM Courses;

--test catalog record counts after delete
SELECT * FROM mb_rel WHERE relName = 'Courses';

--insert a new entry to bring us back up to 3
INSERT INTO Courses VALUES (541, 'Very Cool');
SELECT * FROM Courses;

--invalid delete: bad table
DELETE INVALID WHERE cid = 542;
--invalid delete: bad predicates
DELETE Courses WHERE name = 500;
SELECT * FROM Courses;
--end: test delete

INSERT INTO Students VALUES (1, 'Alice', 25.67);
INSERT INTO Students VALUES (2, 'Chris', 12.34);
INSERT INTO Students VALUES (3, 'Bob', 30.0);
INSERT INTO Students VALUES (4, 'Andy', 50.0);
INSERT INTO Students VALUES (5, 'Ron', 30.0);

CREATE INDEX IX_Name ON Students(Name);

INSERT INTO Grades VALUES (2, 448, 4.0);
INSERT INTO Grades VALUES (3, 348, 2.5);
INSERT INTO Grades VALUES (1, 348, 3.1);
INSERT INTO Grades VALUES (4, 542, 2.8);
INSERT INTO Grades VALUES (5, 542, 3.0);

INSERT INTO Foo VALUES (1, 2, 8, 4, 5);

SELECT * FROM mb_rel;

--start: test pushing selections and join ordering
EXPLAIN SELECT sid, a,name, points ,gcid FROM Students, Grades, Courses, foo WHERE  sid = gsid and cid = gcid and sid = a and a = 1;
SELECT sid, a,name, points ,gcid FROM Students, Grades, Courses, foo WHERE  sid = gsid and cid = gcid and sid = a and a = 1;
--end: test join order
EXPLAIN SELECT sid, points, name FROM Students, Grades, Courses, foo WHERE sid = gsid and cid = gcid and a = sid and e = 1 or sid = 5;

quit

