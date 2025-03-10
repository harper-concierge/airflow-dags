DROP VIEW IF EXISTS {{ schema }}.clean__partners CASCADE;
CREATE VIEW {{ schema }}.clean__partners AS
  SELECT
    p.*
  FROM {{ schema }}.partner p
;
