CREATE OR REPLACE FUNCTION search_contacts(p_pattern TEXT)
RETURNS TABLE(id INT, first_name VARCHAR, last_name VARCHAR, phone VARCHAR) AS $$
BEGIN
    RETURN QUERY
        SELECT c.id, c.first_name, c.last_name, c.phone
        FROM phonebook c
        WHERE c.first_name ILIKE '%' || p_pattern || '%'
           OR c.last_name  ILIKE '%' || p_pattern || '%'
           OR c.phone      ILIKE '%' || p_pattern || '%'
        ORDER BY c.last_name, c.first_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_contacts_paginated(p_limit INT, p_offset INT)
RETURNS TABLE(id INT, first_name VARCHAR, last_name VARCHAR, phone VARCHAR) AS $$
BEGIN
    IF p_limit <= 0 THEN
        RAISE EXCEPTION 'p_limit must be a positive integer, got %', p_limit;
    END IF;
    IF p_offset < 0 THEN
        RAISE EXCEPTION 'p_offset must be >= 0, got %', p_offset;
    END IF;

    RETURN QUERY
        SELECT c.id, c.first_name, c.last_name, c.phone
        FROM phonebook c
        ORDER BY c.id
        LIMIT p_limit
        OFFSET p_offset;
END;
$$ LANGUAGE plpgsql;