# Ontwikkeltools en componenten

- **Python 3.12 (via pyenv-win)** — per gebruiker. Bron: [pyenv-win](https://github.com/pyenv-win/pyenv-win), [python.org](https://www.python.org/downloads/windows/)
- **Poetry** (laatste versie) — per gebruiker. Bron: [python-poetry.org](https://python-poetry.org/docs/#installation)
- **Git** (laatste versie) — systeembreed. Bron: [git-scm.com](https://git-scm.com/download/win)
- **ODBC Driver 17 for SQL Server** (laatste versie, x64) — systeembreed. Bron: [Microsoft](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

Componenten die per gebruiker worden geïnstalleerd, moeten ook worden ingericht voor serviceaccounts die
geplande taken draaien.

Python 3.13 en hoger worden nog niet door alle packages ondersteund (waaronder `dbt-sqlserver`); installeer
nieuwere versies alleen naast 3.12.
