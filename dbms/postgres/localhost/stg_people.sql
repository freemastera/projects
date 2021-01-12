DROP TABLE IF EXISTS stg.people;
CREATE TABLE stg.people(
   Person VARCHAR(17) NOT NULL PRIMARY KEY
  ,Region VARCHAR(7) NOT NULL
);
INSERT INTO stg.people(Person,Region) VALUES ('Anna Andreadi','West');
INSERT INTO stg.people(Person,Region) VALUES ('Chuck Magee','East');
INSERT INTO stg.people(Person,Region) VALUES ('Kelly Williams','Central');
INSERT INTO stg.people(Person,Region) VALUES ('Cassandra Brandow','South');