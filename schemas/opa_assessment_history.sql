create or replace table "opa_assessment_history" (
  "record_id" integer,
  "street_code" integer,
  "house_number" integer,
  "suffix" varchar(4) null,
  "unit_number" varchar(16),
  "cert_year" integer,
  "account_number" integer,
  "certified_taxable_land" integer,
  "certified_taxable_build" integer,
  "certified_exempt_land" integer,
  "certified_exempt_building" integer,
  "market_value" integer
)
