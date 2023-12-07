UPDATE description_dim
SET name_vi = name_en
WHERE name_vi IS NULL OR name_vi = '' OR
    name_en IS NULL OR name_en = '';
