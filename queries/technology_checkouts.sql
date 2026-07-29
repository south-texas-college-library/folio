--metadb:function technology_checkouts

DROP FUNCTION IF EXISTS technology_checkouts;

CREATE FUNCTION technology_checkouts(
    title TEXT DEFAULT NULL,
    call_number TEXT DEFAULT NULL,
    subtype TEXT DEFAULT NULL,
    item_library TEXT DEFAULT NULL,
    po_number TEXT DEFAULT NULL
)
RETURNS TABLE(
    "A - Subtype" TEXT,
    "B - Title" TEXT,
    "C - Call Number" TEXT,
    "D - Item Library" TEXT,
    "E - Item Barcode" TEXT,
    "F - Status" TEXT,
    "G - Check Out Library" TEXT,
    "H - Due Date" TEXT,
    "I - User Barcode" TEXT,
    "J - Name" TEXT,
    "K - Phone Number" TEXT,
	"L - Email" TEXT,
    "M - PO Number" TEXT,
    "N - Staff Notes" TEXT
)
AS $$
    WITH loans AS MATERIALIZED (
        SELECT
            it.id AS item_id,
            jsonb_extract_path_text(l.jsonb, 'dueDate') AS due_date,
            jsonb_extract_path_text(u.jsonb, 'barcode') AS user_barcode,
            NULLIF(CONCAT(jsonb_extract_path_text(u.jsonb, 'personal', 'firstName'), ' ', jsonb_extract_path_text(u.jsonb, 'personal', 'lastName')), ' ') AS full_name,
            jsonb_extract_path_text(u.jsonb, 'personal', 'phone') AS phone,
            jsonb_extract_path_text(u.jsonb, 'personal', 'email') AS email,
            jsonb_extract_path_text(lc.jsonb, 'name') AS checkout_campus
        FROM folio_inventory.instance ins
        JOIN folio_inventory.holdings_record hr ON hr.instanceid = ins.id
        JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
        JOIN folio_circulation.loan l ON jsonb_extract_path_text(l.jsonb, 'itemId')::uuid = it.id
        JOIN folio_users.users u ON u.id = jsonb_extract_path_text(l.jsonb, 'userId')::uuid
        JOIN folio_inventory.location ll ON ll.id = jsonb_extract_path_text(l.jsonb, 'itemEffectiveLocationIdAtCheckOut')::uuid
        JOIN folio_inventory.loccampus lc on lc.id = jsonb_extract_path_text(ll.jsonb, 'campusId')::uuid
        WHERE jsonb_extract_path_text(l.jsonb, 'status', 'name') = 'Open'
    )
    SELECT
        insc.name AS "Subtype",
        jsonb_extract_path_text(ins.jsonb, 'title') AS "Title",
        jsonb_extract_path_text(ins.jsonb, 'callNumber') AS "Call Number",
        ll.name AS "Item Library",
        jsonb_extract_path_text(it.jsonb, 'barcode') AS "Item Barcode",
        jsonb_extract_path_text(it.jsonb, 'status', 'name') AS "Status",
        loans.checkout_campus AS "Check Out Library",
        loans.due_date::DATE::TEXT AS "Due Date",
        loans.user_barcode AS "User Barcode",
        loans.full_name AS "Name",
        loans.phone AS "Phone Number",
        loans.email AS "Email",
        jsonb_path_query_first(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}' as "PO #",
        TRANSLATE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[]"', '') as "Staff Notes"
    FROM folio_inventory.instance ins
    JOIN folio_inventory.holdings_record hr ON hr.instanceid = ins.id
    JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
    JOIN folio_inventory.location__t hl ON hl.id = hr.permanentlocationid
    JOIN folio_inventory.loclibrary__t ll ON ll.id = hl.library_id
    JOIN folio_inventory.statistical_code__t insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
    JOIN folio_inventory.material_type__t m ON m.id = jsonb_extract_path_text(it.jsonb, 'materialTypeId')::uuid
    LEFT JOIN loans ON loans.item_id = it.id
    WHERE
        (item_library = 'All' OR ll.name = item_library)       
        AND CASE
                WHEN subtype = 'Calculator'
                    THEN insc.name = 'Calculator' AND m.name = 'SEM-ITEM'
                WHEN subtype = 'Hotspot'
                    THEN insc.name = 'Hotspot' AND m.name = 'SEMEXTEND-ITEM' AND hl.name != 'Storage'
                WHEN subtype = 'Laptop'
                    THEN insc.name = 'Laptop' AND m.name = 'SEMEXTEND-ITEM' AND (po_number IS NULL OR jsonb_path_query_first(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}' = po_number)
                ELSE
                    (insc.name = 'Calculator' AND m.name = 'SEM-ITEM') 
                    OR (insc.name IN ('Laptop', 'Hotspot') AND m.name = 'SEMEXTEND-ITEM')
            END
        AND (title IS NULL OR jsonb_extract_path_text(ins.jsonb, 'title') = title)
        AND (call_number IS NULL OR jsonb_extract_path_text(ins.jsonb, 'callNumber') = call_number)
    ORDER BY
        jsonb_extract_path_text(it.jsonb, 'barcode')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;