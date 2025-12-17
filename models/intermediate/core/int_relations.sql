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
        rst.relatie_sociaal_type as relatie,

        -- "volgorde" proxy (zoals in je originele query: objectId)
        nrt.client_contact_relatie_type_id as relatie_volgorde,

        nrt.relatie_type as relatie_type,
        nrtc.relatie_type_categorie as relatie_categorie,

        -- adresgegevens relatie
        a.straatnaam       as relatie_straatnaam,
        a.huisnummer   as relatie_huisnummer,
        a.woonplaats         as relatie_woonplaats,
        a.gemeente as relatie_gemeente

    from relations r
    left join lst_relation_social_types rst
        on r.persoonlijke_relatie_id = rst.relatie_sociaal_type_id
    left join relations_addresses ra
        on ra.relatie_id = r.relatie_id
    left join addresses a
        on a.adres_id = ra.adres_id
    left join nexus_client_contact_relation_types nrt
        on nrt.client_contact_relatie_type_id = r.client_contact_relatie_id
    left join nexus_relation_type_categories nrtc
        on nrtc.relatie_type_categorie_id = nrt.relatie_type_categorie_id

    where a.straatnaam is not null
      and a.straatnaam != ''
      and a.startdatum_adres < getdate()
      and (a.einddatum_adres is null or a.einddatum_adres > getdate())

)

select * from final
