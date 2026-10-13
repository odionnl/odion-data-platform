# PreToolUse-hook: blokkeert shell-commando's die databasedata kunnen inlezen.
# Aanvulling op de deny-regels in settings.json: die matchen alleen op prefix,
# deze hook doorzoekt de volledige commandotekst.
# Exit code 2 = blokkeren; de melding op stderr gaat terug naar Claude.

$payload = [Console]::In.ReadToEnd() | ConvertFrom-Json
$command = $payload.tool_input.command
if (-not $command) { exit 0 }

$patterns = @(
    '\bdbt\s+show\b',
    '\bpyodbc\b',
    '\bpymssql\b',
    '\bsqlalchemy\b',
    '\bsqlcmd\b',
    '\bInvoke-Sqlcmd\b',
    '\bbcp\s',
    '\bSystem\.Data\.SqlClient\b',
    '\bMicrosoft\.Data\.SqlClient\b'
)

foreach ($pattern in $patterns) {
    if ($command -match $pattern) {
        [Console]::Error.WriteLine(
            "Geblokkeerd door privacy-hook (patroon '$pattern'): databasedata inlezen is niet toegestaan. " +
            "Zie CLAUDE.md. Lees alleen code/YAML, of vraag de gebruiker om informatie over de data. " +
            "Zoek je naar dit woord in code? Gebruik dan de Grep-tool in plaats van een shell-commando."
        )
        exit 2
    }
}

exit 0
