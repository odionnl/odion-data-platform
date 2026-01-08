{{ config(materialized='view') }}

with relations as (

    select * from {{ ref('stg_ons__relations') }}

),

relations_addresses as (

    select * from {{ ref('stg_ons__relations_addresses') }}

),

addresses as (

    select * from {{ ref('stg_ons__addresses') }}

),

lst_relation_social_types as (

    select * from {{ ref('stg_ons__lst_relation_social_types') }}

),

nexus_personal_relation_types as (

    select * from {{ ref('stg_ons__nexus_personal_relation_types') }}
    
),

nexus_client_contact_relation_types as (

    select * from {{ ref('stg_ons__nexus_client_contact_relation_types') }}

),

nexus_relation_type_categories as (

    select * from {{ ref('stg_ons__nexus_relation_type_categories') }}

),

final as (

    select
        r.client_id,

        -- sociaal type (bv. vader/moeder etc.)
        rst.relatie_sociaal_type,

        -- persoonlijke relatie
        prt.persoonlijke_relatietype_id,
        prt.persoonlijke_relatietype,
        nrtc_p.relatietype_categorie as persoonlijke_relatietype_categorie,

        -- contactpersoon relatie type en categorie
        nrt.contactpersoon_relatietype_id,
        nrt.contactpersoon_relatietype,
        nrtc_c.relatietype_categorie as contactpersoon_relatietype_categorie,

        -- adresgegevens relatie
        a.straatnaam,
        a.huisnummer,
        a.plaats,
        a.gemeente,
        a.startdatum_adres,
        a.einddatum_adres

    from relations r
    left join relations_addresses ra
        on ra.relatie_id = r.relatie_id
    left join addresses a
        on a.adres_id = ra.adres_id

    -- sociale relatie (type)
    left join lst_relation_social_types rst
        on r.persoonlijke_relatietype_id = rst.relatie_sociaal_type_id

    -- persoonlijke relaties (type & categorie)
    left join nexus_personal_relation_types prt
        on prt.persoonlijke_relatietype_id = r.persoonlijke_relatietype_id
    left join nexus_relation_type_categories nrtc_p
        on nrtc_p.relatietype_categorie_id = prt.persoonlijke_relatietype_categorie_id
    
    -- contactpersoon relaties (type & categorie)
    left join nexus_client_contact_relation_types nrt
        on nrt.contactpersoon_relatietype_id = r.contactpersoon_relatietype_id
    left join nexus_relation_type_categories nrtc_c
        on nrtc_c.relatietype_categorie_id = nrt.contactpersoon_relatietype_categorie_id
)

select * from final
