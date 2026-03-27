{#
  get_locatiecluster: bepaalt het cluster van een locatie op basis van naam en hiërarchiepositie.

  CC-specifiek — de clusternamen en naampatronen hieronder zijn afgestemd op de
  locatiestructuur van deze organisatie. Pas dit aan voor andere organisaties.

  Parameters:
    locatienaam  — kolom met de naam van de locatie zelf
    niveau2      — tweede niveau in de hiërarchie (bijv. afdeling/programma)
    niveau3      — derde niveau in de hiërarchie (bijv. zorgsoort)
#}
{% macro get_locatiecluster(locatienaam, niveau2, niveau3) %}
    case
        when lower(isnull({{ locatienaam }}, '')) like '%externe aanbieder%'
          or isnull({{ niveau2 }}, '') like '%Externe aanbieder%'
          then N'Externe aanbieder'
        when isnull({{ niveau2 }}, '') like '%Aanmeldingen%'
          then N'Aanmeldingen'
        when lower(isnull({{ locatienaam }}, '')) like '%wachtl%'
          or isnull({{ niveau2 }}, '') like '%Wachtlijst%'
          then N'Wachtlijsten'
        when isnull({{ niveau2 }}, '') like '%Archief%'
          then N'Archief'
        when lower(isnull({{ locatienaam }}, '')) like '%in de wijk%'
          or isnull({{ niveau3 }}, '') like '%Ambulant%'
          then N'Ambulant'
        when lower(isnull({{ locatienaam }}, '')) like '%dagbesteding%'
          or isnull({{ niveau3 }}, '') like '%Dagbesteding%'
          then N'Dagbesteding'
        when lower(isnull({{ locatienaam }}, '')) like '%logeren%'
          or isnull({{ niveau3 }}, '') like '%Logeren%'
          then N'Logeren'
        when isnull({{ niveau3 }}, '') like '%Wonen%'
          then N'Wonen'
        when isnull({{ niveau3 }}, '') like '%Behandeling%'
          then N'Behandeling'
        when isnull({{ niveau3 }}, '') like '%Kind en gezin%'
          then N'Kind en gezin'
        when isnull({{ niveau3 }}, '') like '%Multidisciplinair Team%'
          then N'Multidisciplinair Team'
        when isnull({{ niveau2 }}, '') like '%Overige%'
          then N'Overige'
        else N'Overige'
    end
{% endmacro %}
