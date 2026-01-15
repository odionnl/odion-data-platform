select top (1000) *
from {{ source('ortec', 'dim_employee') }}
