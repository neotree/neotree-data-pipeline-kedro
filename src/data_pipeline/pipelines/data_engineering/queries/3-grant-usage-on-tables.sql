DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'neotree') THEN 
	GRANT  SELECT ON ALL TABLES IN SCHEMA derived TO neotree;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'danielsilksmith') THEN
        grant usage on schema derived to danielsilksmith;
        grant usage on schema scratch to danielsilksmith;
        grant select on all tables in schema derived to danielsilksmith;
        grant select on all tables in schema scratch to danielsilksmith;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'yalisassoon') THEN
        grant usage on schema derived to yalisassoon;
        grant usage on schema scratch to yalisassoon;
        grant select on all tables in schema derived to yalisassoon;
        grant select on all tables in schema scratch to yalisassoon;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'deliwe') THEN
        grant usage on schema derived to deliwe;
        grant select on all tables in schema derived to deliwe;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'farahshair') THEN
        grant usage on schema derived to farahshair;
        grant select on all tables in schema derived to farahshair;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'louisdutoit') THEN
        grant usage on schema derived to louisdutoit;
        grant select on all tables in schema derived to louisdutoit;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_usr') THEN
        grant select on all tables in schema derived to metabase_usr;
        grant usage on schema derived to metabase_usr;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi') THEN
        grant select on all tables in schema derived to powerbi;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_gateway') THEN
        grant select on all tables in schema derived to powerbi_gateway;
    END IF;
END;
$do$;
