## Aanmaken van de Linked Server (SQL Server → Azure SQL)

Om data uit de Azure SQL-database (Ortec) beschikbaar te maken in het SQL Server DWH, is een **Linked Server** aangemaakt op de DWH SQL Server.

Deze Linked Server maakt het mogelijk om Azure SQL-tabellen te benaderen alsof ze lokaal zijn, bijvoorbeeld via `OPENQUERY`.

---

### Stap 1: Controleren van beschikbare providers

Op de DWH SQL Server is eerst gecontroleerd welke OLE DB providers beschikbaar zijn.

```sql
EXEC master.dbo.sp_enum_oledb_providers;
```

De provider **`SQLNCLI11` (SQL Server Native Client 11.0)** was beschikbaar en is gebruikt.  
De modernere provider `MSOLEDBSQL` was niet geïnstalleerd.

---

### Stap 2: Linked Server aanmaken

De Linked Server is aangemaakt met de volgende configuratie:

```sql
EXEC master.dbo.sp_addlinkedserver
  @server     = N'ORTEC_BDP',                          -- naam van de linked server
  @srvproduct = N'',
  @provider   = N'SQLNCLI11',
  @datasrc    = N'<azure_sql_server>';
```

**Toelichting:**
- `@server`: logische naam van de Linked Server (vrij te kiezen)
- `@provider`: OLE DB provider (`SQLNCLI11`)
- `@datasrc`: hostname van de Azure SQL Server

---

### Stap 3: Login mapping instellen (SQL-authenticatie)

Azure SQL ondersteunt **geen Windows-logins**, daarom is SQL-authenticatie gebruikt.

```sql
EXEC master.dbo.sp_addlinkedsrvlogin
  @rmtsrvname  = N'ORTEC_BDP',
  @useself     = N'false',
  @locallogin  = NULL,
  @rmtuser     = N'<azure_sql_user>',
  @rmtpassword = N'<azure_sql_password>';
```

> De gebruikte Azure SQL login heeft alleen **leesrechten** op de benodigde tabellen.

---

### Stap 4: Serveropties instellen

Om correct gebruik vanuit het DWH mogelijk te maken, zijn de volgende opties geactiveerd:

```sql
EXEC master.dbo.sp_serveroption @server=N'ORTEC_BDP', @optname=N'data access', @optvalue=N'true';
EXEC master.dbo.sp_serveroption @server=N'ORTEC_BDP', @optname=N'rpc out',     @optvalue=N'true';
```

---

### Stap 5: Verbinding testen

De verbinding is getest met:

```sql
EXEC master.dbo.sp_testlinkedserver N'ORTEC_BDP';
```

En door een testquery uit te voeren:

```sql
SELECT TOP (10) *
FROM OPENQUERY(
  ORTEC_BDP,
  'SELECT * FROM bi_support.DIM_EMPLOYEE'
);
```

Na succesvolle uitvoering is de Linked Server operationeel.

---

### Resultaat

- De Linked Server `ORTEC_BDP` is zichtbaar in SSMS onder  
  **Server Objects → Linked Servers**
- Tabellen uit Azure SQL zijn browsebaar en querybaar
- Deze Linked Server wordt gebruikt door views in schema `ext_ortec`, die vervolgens door dbt worden gebruikt
