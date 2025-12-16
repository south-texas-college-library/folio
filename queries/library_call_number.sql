--metadb:function library_call_number

DROP FUNCTION IF EXISTS library_call_number;

CREATE FUNCTION library_call_number(
    start_call_number text DEFAULT J,
    end_call_number text DEFAULT JZ9999.9999
    )
RETURNS TABLE(
    title text,
    barcode text,
    location text,
    material_type text,
    service_point text,
    loan_date text,
    return_date text,
    call_number text,
    item_status text,
    renewal_count numeric,
    cataloged_date text,
    item_checkouts integer,
    item_identifiers text,
    item_staff_note text,
    item_statistical_code text,
    instance_subjects text,
    instance_publication_date text
    )
AS $$
--CTE for checkouts
with checkouts as (
	select
		folio_inventory.item__t.id as item_id,
		folio_inventory.item__t.barcode as item_barcode,
		count(folio_circulation.loan__t.id) as item_checkouts
	from
		folio_circulation.loan__t
	left join
		folio_inventory.item__t on folio_inventory.item__t.id = folio_circulation.loan__t.item_id
	group by
		folio_inventory.item__t.id,
		folio_inventory.item__t.barcode
),
--CTE for identifiers
identifiers as (
    select
        folio_inventory.instance.id as id,
        string_agg(identifiers ->> 'value', ', ') AS item_identifiers
    from folio_inventory.instance
    left join lateral jsonb_array_elements(folio_inventory.instance.jsonb -> 'identifiers') AS identifiers on true
    group by
        folio_inventory.instance.id
),
--CTE for item notes
notes as (
    select
        folio_inventory.item.id as id,
        string_agg(item_notes ->> 'note', ', ') filter (where folio_inventory.item_note_type__t.name = 'Staff Note') as item_staff_note
    from folio_inventory.item
    left join lateral jsonb_array_elements(folio_inventory.item.jsonb -> 'notes') as item_notes on true
    join folio_inventory.item_note_type__t on folio_inventory.item_note_type__t.id = (item_notes ->> 'itemNoteTypeId')::uuid
    group by
        folio_inventory.item.id
),
--CTE for statistical codes
statisticalcodes as (
    select
        folio_inventory.item.id as id,
        folio_inventory.statistical_code__t.name as item_statistical_code
    from folio_inventory.item
    left join lateral jsonb_array_elements_text(folio_inventory.item.jsonb -> 'statisticalCodeIds') as object on true
    left join folio_inventory.statistical_code__t on folio_inventory.statistical_code__t.id = object::uuid
),
--CTE for instance subjects
subjects as (
	select
		folio_inventory.instance.id as id,
		string_agg(subjects ->> 'value', ', ') as instance_subjects
	from folio_inventory.instance
	left join lateral jsonb_array_elements(folio_inventory.instance.jsonb -> 'subjects') as subjects on true
	group by folio_inventory.instance.id
),
--CTE for publication date
publicationdate as (
	select
		folio_inventory.instance.id as id,
		string_agg(publicationdate ->> 'dateOfPublication', ', ') as instance_publication_date
	from folio_inventory.instance
	left join lateral jsonb_array_elements(folio_inventory.instance.jsonb -> 'publication') as publicationdate on true
	group by folio_inventory.instance.id
)
select
		folio_inventory.instance.jsonb ->> 'title' as title,
		folio_inventory.item__t.barcode as barcode,
		folio_inventory.location__t.name as location,
		folio_inventory.material_type__t.name as material_type,
		folio_inventory.service_point__t.name as service_point,
		to_char(folio_circulation.loan__t.loan_date::timestamptz AT TIME ZONE 'CDT', 'YYYY-MM-DD HH24:MI:SS AM') AS loan_date,
		to_char(folio_circulation.loan__t.return_date::timestamp, 'YYYY-MM-DD HH24:MI:SS AM') as return_date,
		folio_inventory.holdings_record__t.call_number,
		folio_inventory.item.jsonb -> 'status' ->> 'name' as item_status,
		folio_circulation.loan__t.renewal_count,
		folio_inventory.instance.jsonb ->> 'catalogedDate' as cataloged_date,
		checkouts.item_checkouts,
		identifiers.item_identifiers,
		notes.item_staff_note,
		statisticalcodes.item_statistical_code,
		subjects.instance_subjects,
		publicationdate.instance_publication_date
from
		folio_inventory.instance
left join
		folio_inventory.holdings_record__t on folio_inventory.holdings_record__t.instance_id = folio_inventory.instance.id
left join
		folio_inventory.item on folio_inventory.item.holdingsrecordid = folio_inventory.holdings_record__t.id
left join
		folio_inventory.item__t on folio_inventory.item__t.id = folio_inventory.item.id 
left join
		folio_inventory.location__t on folio_inventory.location__t.id = folio_inventory.item.effectivelocationid	
left join
		folio_inventory.material_type__t on folio_inventory.material_type__t.id = folio_inventory.item.materialtypeid
left join	
		folio_inventory.service_point__t on folio_inventory.service_point__t.id = folio_inventory.location__t.primary_service_point
left join 
		folio_circulation.loan__t on folio_circulation.loan__t.item_id = folio_inventory.item.id
left join
		checkouts on checkouts.item_id = folio_inventory.item__t.id
join
		identifiers on identifiers.id = folio_inventory.instance.id
join
		notes on notes.id = folio_inventory.item.id
join
		statisticalcodes on statisticalcodes.id = folio_inventory.item.id
join
		subjects on subjects.id = folio_inventory.instance.id
join
		publicationdate on publicationdate.id = folio_inventory.instance.id
where
		folio_inventory.holdings_record__t.call_number between start_call_number and end_call_number
order by call_number
;
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;
