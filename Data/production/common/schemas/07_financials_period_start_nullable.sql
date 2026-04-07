-- Allow production financials records where period_start is unavailable.
ALTER TABLE production_financials
    ALTER COLUMN period_start DROP NOT NULL;

DO $$
DECLARE
    constraint_name text;
BEGIN
    SELECT con.conname
    INTO constraint_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = 'public'
      AND rel.relname = 'production_financials'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) LIKE '%period_end > period_start%';

    IF constraint_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE production_financials DROP CONSTRAINT %I',
            constraint_name
        );
    END IF;
END $$;

ALTER TABLE production_financials
    ADD CONSTRAINT production_financials_check
    CHECK (period_start IS NULL OR period_end > period_start);
