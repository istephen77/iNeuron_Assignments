CREATE WAREHOUSE iNeuron_SQL_Project_1 WITH WAREHOUSE_SIZE = 'MEDIUM' 
WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 600 AUTO_RESUME = TRUE COMMENT = '--iNeuron_SQL_Project_1';

CREATE DATABASE INEURON_SQL_PROJECT1;

USE INEURON_SQL_PROJECT1;

-- TASK : 1 SHOPPING TRANSACTIONS

CREATE TABLE SHOPPING_HISTORY (
PRODUCT VARCHAR(50) NOT NULL,
QUANTITY INTEGER NOT NULL,
UNIT_PRICE INTEGER NOT NULL
);

INSERT INTO SHOPPING_HISTORY VALUES ('MILK', 3, 10);
INSERT INTO SHOPPING_HISTORY VALUES ('MILK', 3, 10);
INSERT INTO SHOPPING_HISTORY VALUES ('MILK',3,10);
INSERT INTO SHOPPING_HISTORY VALUES ('MILK',3,10);
INSERT INTO SHOPPING_HISTORY VALUES ('BREAD',5,7);
INSERT INTO SHOPPING_HISTORY VALUES ('JAM',2,20);
INSERT INTO SHOPPING_HISTORY VALUES ('BUTTER',4,12);
INSERT INTO SHOPPING_HISTORY VALUES ('APPLE',10,24);
INSERT INTO SHOPPING_HISTORY VALUES ('MILK',6,8);
INSERT INTO SHOPPING_HISTORY VALUES ('VEGETABLES',2,18);
INSERT INTO SHOPPING_HISTORY VALUES ('BUTTER',1,13);
INSERT INTO SHOPPING_HISTORY VALUES ('APPLE',10,14);
INSERT INTO SHOPPING_HISTORY VALUES ('BANANA',12,15);
INSERT INTO SHOPPING_HISTORY VALUES ('MILK',10,17);
INSERT INTO SHOPPING_HISTORY VALUES ('BANANA',7,13);


SELECT * FROM SHOPPING_HISTORY;

-- OUTPUT QUERY : 
SELECT PRODUCT, SUM(QUANTITY*UNIT_PRICE) AS TOTAL_PRICE 
FROM SHOPPING_HISTORY GROUP BY PRODUCT ORDER BY PRODUCT DESC;


-- TASK 2 : TELECOMMUNICATIONS COMPANY 

CREATE TABLE PHONES ( 
`NAME` VARCHAR(20) NOT NULL UNIQUE,
PHONE_NUMBER INT NOT NULL UNIQUE
);

CREATE TABLE CALLS (
ID INT NOT NULL,
CALLER INT NOT NULL,
CALLEE  INT NOT NULL,
DURATION INT NOT NULL,
UNIQUE(ID)
);

INSERT INTO PHONES VALUES ('Jack',1234);
INSERT INTO PHONES VALUES ('Lena',3333);
INSERT INTO PHONES VALUES ('Mark',9999);
INSERT INTO PHONES VALUES ('Anna',7582);
INSERT INTO PHONES VALUES ('John',6356);
INSERT INTO PHONES VALUES ('Addison',4315);
INSERT INTO PHONES VALUES ('Kate',8003);
INSERT INTO PHONES VALUES ('Ginny',9831);

SELECT * FROM PHONES;

INSERT INTO CALLS VALUES (25,1234,7582,8);
INSERT INTO CALLS VALUES (7,9999,7582,1);
INSERT INTO CALLS VALUES (18,9999,3333,4);
INSERT INTO CALLS VALUES (2,7582,3333,3);
INSERT INTO CALLS VALUES (3,3333,1234,1);
INSERT INTO CALLS VALUES (21,3333,1234,1);
INSERT INTO CALLS VALUES (65,8003,9831,7);
INSERT INTO CALLS VALUES (100,9831,8003,3);
INSERT INTO CALLS VALUES (145,4315,9831,18);

SELECT * FROM CALLS;

-- OUTPUT QUERY : 

WITH CALL_DURATION AS (
SELECT CALLER AS PHONE_NUMBER, SUM(DURATION) AS DURATION FROM CALLS GROUP BY CALLER
UNION ALL
SELECT CALLEE AS PHONE_NUMBER, SUM(DURATION) AS DURATION FROM CALLS GROUP BY CALLEE
)
SELECT `NAME` 
FROM PHONES P INNER JOIN CALL_DURATION CD 
ON (P.PHONE_NUMBER = CD.PHONE_NUMBER)
GROUP BY `NAME`
HAVING SUM(DURATION) >= 10
ORDER BY `NAME`;

-- TASK 3 : BANK ACCOUNT TRANSACTIONS

CREATE TABLE TRANSACTIONS (
AMOUNT INTEGER NOT NULL,
TRANSACTION_DATE DATE NOT NULL
);

INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(1000, '2020-01-06');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-10, '2020-01-14');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-75, '2020-01-20');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-5, '2020-01-25');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-4, '2020-01-29');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(2000, '2020-03-10');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-75, '2020-03-12');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-20, '2020-03-15');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(40, '2020-03-15');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-50, '2020-03-17');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(200, '2020-10-10');
INSERT INTO TRANSACTIONS (AMOUNT, TRANSACTION_DATE) VALUES(-200, '2020-10-10');

SELECT * FROM TRANSACTIONS;

-- OUTPUT QUERY : 

SELECT (SUM(AMOUNT) - (12 - COUNT)*5) AS BALANCE FROM TRANSACTIONS
CROSS JOIN
(SELECT COUNT(*) AS COUNT FROM 
(SELECT SUM(AMOUNT), EXTRACT(MONTH FROM TRANSACTION_DATE) AS MONTH_NO, COUNT(*)
FROM TRANSACTIONS
WHERE AMOUNT < 0
GROUP BY MONTH_NO
HAVING COUNT(*)>=3 AND SUM(AMOUNT) <= -100
ORDER BY MONTH_NO) AS TEST) AS TEST;
























