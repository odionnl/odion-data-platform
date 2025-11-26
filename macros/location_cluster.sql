{% macro get_location_cluster(locatienaam, niveau2, niveau3) %}
    case
        -- Externe aanbieder
        when lower(isnull({{ locatienaam }}, '')) like '%externe aanbieder%'
          or isnull({{ niveau2 }}, '') like '%Externe aanbieder%'
          then N'Externe aanbieder'

        -- Aanmeldingen
        when isnull({{ niveau2 }}, '') like '%Aanmeldingen%'
          then N'Aanmeldingen'

        -- Wachtlijsten
        when lower(isnull({{ locatienaam }}, '')) like '%wachtl%'
          or isnull({{ niveau2 }}, '') like '%Wachtlijst%'
          then N'Wachtlijsten'

        -- Archief
        when isnull({{ niveau2 }}, '') like '%Archief%'
          then N'Archief'

        -- Overige (algemene categorie)
        when isnull({{ niveau2 }}, '') like '%Overige%'
          then N'Overige'

        -- Ambulant
        when lower(isnull({{ locatienaam }}, '')) like '%in de wijk%'
          or isnull({{ niveau3 }}, '') like '%Ambulant%'
          then N'Ambulant'

        -- Dagbesteding
        when lower(isnull({{ locatienaam }}, '')) like '%dagbesteding%'
          or isnull({{ niveau3 }}, '') like '%Dagbesteding%'
          then N'Dagbesteding'

        -- Logeren
        when lower(isnull({{ locatienaam }}, '')) like '%logeren%'
          or isnull({{ niveau3 }}, '') like '%Logeren%'
          then N'Logeren'

        -- Wonen
        when isnull({{ niveau3 }}, '') like '%Wonen%'
          then N'Wonen'

        -- Behandeling
        when isnull({{ niveau3 }}, '') like '%Behandeling%'
          then N'Behandeling'

        -- Kind en gezin
        when isnull({{ niveau3 }}, '') like '%Kind en gezin%'
          then N'Kind en gezin'

        -- Multidisciplinair Team
        when isnull({{ niveau3 }}, '') like '%Multidisciplinair Team%'
          then N'Multidisciplinair Team'

        else N'Overige'
    end
{% endmacro %}
