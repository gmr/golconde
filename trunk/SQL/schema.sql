-- Schema: "golconde"

-- DROP SCHEMA golconde;

CREATE SCHEMA golconde
  AUTHORIZATION postgres;
GRANT ALL ON SCHEMA golconde TO postgres;
GRANT USAGE ON SCHEMA golconde TO public;
COMMENT ON SCHEMA golconde IS 'Golconde Data Distribution System Schema';

-- Table: golconde.settings

-- DROP TABLE golconde.settings;

CREATE TABLE golconde.settings
(
  setting text NOT NULL, -- Setting Name
  value text NOT NULL, -- Setting value
  CONSTRAINT pksettings PRIMARY KEY (setting)
)
WITH (OIDS=FALSE);
ALTER TABLE golconde.settings OWNER TO postgres;
GRANT ALL ON TABLE golconde.settings TO postgres;
GRANT SELECT ON TABLE golconde.settings TO public;
COMMENT ON TABLE golconde.settings IS 'Stores information important for Golconde use';
COMMENT ON COLUMN golconde.settings.setting IS 'Setting Name';
COMMENT ON COLUMN golconde.settings.value IS 'Setting value';

COPY settings (setting, value) FROM stdin;
mqConnectString	tcp://localhost:61613?wireFormat=stomp
\.
