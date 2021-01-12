create schema dw;
-- *************** SqlDBM: PostgreSQL ****************;
-- ***************************************************;


-- ************************************** "dw"."dim_customers"

CREATE TABLE "dw"."dim_customers"
(
 "customer_id"   varchar(25) NOT NULL,
 "customer_name" varchar(50) NOT NULL,
 "segment"       varchar(50) NOT NULL,
 CONSTRAINT "PK_dim_customers" PRIMARY KEY ( "customer_id" )
);



-- *************** SqlDBM: PostgreSQL ****************;
-- ***************************************************;


-- ************************************** "dw"."dim_dates"

CREATE TABLE "dw"."dim_dates"
(
 "order_date" date NOT NULL,
 CONSTRAINT "PK_dim_dates" PRIMARY KEY ( "order_date" )
);






-- *************** SqlDBM: PostgreSQL ****************;
-- ***************************************************;


-- ************************************** "dw"."dim_managers"

CREATE TABLE "dw"."dim_managers"
(
 "manager_id" smallint NOT NULL,
 "person"     varchar(50) NOT NULL,
 "region"     varchar(50) NOT NULL,
 CONSTRAINT "PK_dim_managers" PRIMARY KEY ( "manager_id" )
);


-- *************** SqlDBM: PostgreSQL ****************;
-- ***************************************************;


-- ************************************** "dw"."dim_orders"

CREATE TABLE "dw"."dim_orders"
(
 "order_id"    varchar(50) NOT NULL,
 "ship_mode"   varchar(50) NOT NULL,
 "ship_date"   date NOT NULL,
 "country"     varchar(50) NOT NULL,
 "state"       varchar(50) NOT NULL,
 "city"        varchar(50) NOT NULL,
 "postal_code" varchar(50) NULL,
 "returned"    varchar(5) NULL,
 CONSTRAINT "PK_dim_orders" PRIMARY KEY ( "order_id" )
);






-- *************** SqlDBM: PostgreSQL ****************;
-- ***************************************************;


-- ************************************** "dw"."dim_products"

CREATE TABLE "dw"."dim_products"
(
 "category"     varchar(50) NOT NULL,
 "prod_id"      int NOT NULL,
 "sub_category" varchar(50) NOT NULL,
 "product_name" varchar(255) NOT NULL,
 "product_id"   varchar(25) NOT NULL,
 CONSTRAINT "PK_dim_products" PRIMARY KEY ( "prod_id" )
);

-- *************** SqlDBM: PostgreSQL ****************;
-- ***************************************************;


-- ************************************** "dw"."facts"

CREATE TABLE "dw"."facts"
(
 "id"          int NOT NULL,
 "sales"       numeric NOT NULL,
 "quantity"    smallint NOT NULL,
 "discount"    numeric NOT NULL,
 "profit"      numeric NOT NULL,
 "customer_id" varchar(25) NOT NULL,
 "order_id"    varchar(50) NOT NULL,
 "manager_id"  smallint NOT NULL,
 "prod_id"     int NOT NULL,
 "order_date"  date NOT NULL,
 CONSTRAINT "PK_facts" PRIMARY KEY ( "id" ),
 CONSTRAINT "FK_56" FOREIGN KEY ( "customer_id" ) REFERENCES "dw"."dim_customers" ( "customer_id" ),
 CONSTRAINT "FK_67" FOREIGN KEY ( "order_id" ) REFERENCES "dw"."dim_orders" ( "order_id" ),
 CONSTRAINT "FK_74" FOREIGN KEY ( "manager_id" ) REFERENCES "dw"."dim_managers" ( "manager_id" ),
 CONSTRAINT "FK_93" FOREIGN KEY ( "prod_id" ) REFERENCES "dw"."dim_products" ( "prod_id" ),
 CONSTRAINT "FK_98" FOREIGN KEY ( "order_date" ) REFERENCES "dw"."dim_dates" ( "order_date" )
);

CREATE INDEX "fkIdx_56" ON "dw"."facts"
(
 "customer_id"
);

CREATE INDEX "fkIdx_67" ON "dw"."facts"
(
 "order_id"
);

CREATE INDEX "fkIdx_74" ON "dw"."facts"
(
 "manager_id"
);

CREATE INDEX "fkIdx_93" ON "dw"."facts"
(
 "prod_id"
);

CREATE INDEX "fkIdx_98" ON "dw"."facts"
(
 "order_date"
);