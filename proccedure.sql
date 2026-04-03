CREATE OR REPLACE FUNCTION is_valid_phone(p_phone TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN p_phone ~ '^\+?[78]\d{10}$';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsert_contact(
    p_first_name VARCHAR,
    p_last_name  VARCHAR,
    p_phone      VARCHAR
)
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT is_valid_phone(p_phone) THEN
        RAISE EXCEPTION 'Invalid phone number: %', p_phone;
    END IF;

    IF EXISTS (
        SELECT 1 FROM phonebook
        WHERE first_name = p_first_name AND last_name = p_last_name
    ) THEN
        UPDATE phonebook
        SET phone = p_phone
        WHERE first_name = p_first_name AND last_name = p_last_name;
    ELSE
        INSERT INTO phonebook(first_name, last_name, phone)
        VALUES (p_first_name, p_last_name, p_phone);
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE bulk_insert_contacts(
    p_names  TEXT[],
    p_phones TEXT[]
)
LANGUAGE plpgsql AS $$
DECLARE
    v_count INT;
    i       INT;
    v_first VARCHAR;
    v_last  VARCHAR;
    v_phone TEXT;
    v_parts TEXT[];
BEGIN
    v_count := array_length(p_names, 1);

    IF v_count IS NULL OR v_count <> array_length(p_phones, 1) THEN
        RAISE EXCEPTION 'Names and phones arrays must be the same length';
    END IF;

    DROP TABLE IF EXISTS invalid_contacts;
    CREATE TEMP TABLE invalid_contacts (
        full_name TEXT,
        phone     TEXT,
        reason    TEXT
    );

    FOR i IN 1..v_count LOOP
        v_parts := string_to_array(trim(p_names[i]), ' ');
        v_phone := trim(p_phones[i]);

        IF array_length(v_parts, 1) < 2 THEN
            INSERT INTO invalid_contacts VALUES (p_names[i], v_phone, 'Name must contain first and last name');
            CONTINUE;
        END IF;

        v_first := v_parts[1];
        v_last  := v_parts[2];

        IF NOT is_valid_phone(v_phone) THEN
            INSERT INTO invalid_contacts VALUES (p_names[i], v_phone, 'Invalid phone format');
            CONTINUE;
        END IF;

        IF EXISTS (
            SELECT 1 FROM phonebook
            WHERE first_name = v_first AND last_name = v_last
        ) THEN
            UPDATE phonebook
            SET phone = v_phone
            WHERE first_name = v_first AND last_name = v_last;
        ELSE
            INSERT INTO phonebook(first_name, last_name, phone)
            VALUES (v_first, v_last, v_phone);
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE PROCEDURE delete_contact(
    p_first_name VARCHAR DEFAULT NULL,
    p_last_name  VARCHAR DEFAULT NULL,
    p_phone      VARCHAR DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    v_deleted INT;
BEGIN
    IF p_first_name IS NULL AND p_last_name IS NULL AND p_phone IS NULL THEN
        RAISE EXCEPTION 'At least one parameter must be provided';
    END IF;

    IF p_phone IS NOT NULL THEN
        DELETE FROM phonebook WHERE phone = p_phone;
        GET DIAGNOSTICS v_deleted = ROW_COUNT;
        RAISE NOTICE 'Deleted % row(s) by phone %', v_deleted, p_phone;
    ELSE
        DELETE FROM phonebook
        WHERE (p_first_name IS NULL OR first_name = p_first_name)
          AND (p_last_name  IS NULL OR last_name  = p_last_name);
        GET DIAGNOSTICS v_deleted = ROW_COUNT;
        RAISE NOTICE 'Deleted % row(s) by name % %', v_deleted, p_first_name, p_last_name;
    END IF;
END;
$$;