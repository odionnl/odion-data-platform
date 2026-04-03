select
    client_id,
    {{ ons_administratie_url('client_id') }}              as url_ons_administratie,
    {{ ons_dossier_url('care_plan', 'client_id') }}       as url_ons_zorgplan,
    {{ ons_dossier_url('reports', 'client_id') }}         as url_ons_rapportages,
    {{ ons_dossier_url('medical/overview', 'client_id') }} as url_ons_medicatie,
    {{ ons_dossier_url('calendar', 'client_id') }}        as url_ons_agenda

from {{ ref('stg_onsdb__clients') }}
