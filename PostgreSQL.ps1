#
# PostgreSQL.ps1 - IDM System PowerShell Script for PostgreSQL Server.
#
# Any IDM System PowerShell Script is dot-sourced in a separate PowerShell context, after
# dot-sourcing the IDM Generic PowerShell Script '../Generic.ps1'.
#


$Log_MaskableKeys = @(
    'password'
)


#
# System functions
#

function Idm-SystemInfo {
    param (
        # Operations
        [switch] $Connection,
        [switch] $TestConnection,
        [switch] $Configuration,
        # Parameters
        [string] $ConnectionParams
    )

    Log info "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            
            @{
                name = 'Hostname'
                type = 'textbox'
                label = 'Hostname'
                tooltip = 'Hostname of PostgreSQL SQL server'
                value = ''
            }
            @{
                name = 'Port'
                type = 'textbox'
                label = 'Port'
                tooltip = 'Port of PostgreSQL SQL server'
                value = ''
            }
            @{
                name = 'Driver'
                type = 'textbox'
                label = 'Driver'
                tooltip = 'DriverName used for PostgreSQL SQL server'
                value = '{PostgreSQL ODBC Driver(UNICODE)}'
            }
            @{
                name = 'Database'
                type = 'textbox'
                label = 'Database'
                tooltip = 'Database of PostgreSQL SQL server'
                value = ''
            }
            @{
                name = 'Username'
                type = 'textbox'
                label = 'Username'
                label_indent = $true
                tooltip = 'User account name to access PostgreSQL SQL server'
                value = ''
            }
            @{
                name = 'Password'
                type = 'textbox'
                password = $true
                label = 'Password'
                label_indent = $true
                tooltip = 'User account password to access PostgreSQL SQL server'
                value = ''
            }
            @{
                name = 'nr_of_sessions'
                type = 'textbox'
                label = 'Max. number of simultaneous sessions'
                tooltip = ''
                value = 5
            }
            @{
                name = 'sessions_idle_timeout'
                type = 'textbox'
                label = 'Session cleanup idle time (minutes)'
                tooltip = '0 disables session cleanup'
                value = 30
            }
        )
    }

    if ($TestConnection) {
        Open-PostgreSQLSqlConnection $ConnectionParams
    }

    if ($Configuration) {
        @()
    }

    Log info "Done"
}


function Idm-OnUnload {
    Close-PostgreSQLSqlConnection
}


#
# CRUD functions
#

$ColumnsInfoCache = @{}

$SqlInfoCache = @{}


function Fill-SqlInfoCache {
    param (
        [switch] $Force
    )

    if (!$Force -and $Global:SqlInfoCache.Ts -and ((Get-Date) - $Global:SqlInfoCache.Ts).TotalMilliseconds -le [Int32]600000) {
        return
    }

    # Refresh cache
    $sql_command = New-PostgreSQLSqlCommand "
    SELECT
        AX.OWNER || '.' || AX.OBJECT_NAME AS full_object_name,
        CASE
            WHEN AO.TABLE_TYPE = 'BASE TABLE' THEN 'Table'
            WHEN AO.TABLE_TYPE = 'VIEW' THEN 'View'
            ELSE 'Other'
        END AS object_type,
        ATC.COLUMN_NAME,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM pg_attribute pa
                WHERE pa.attrelid = (
                        SELECT c.oid
                        FROM pg_class c
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = ATC.table_schema AND c.relname = ATC.table_name
                    )
                    AND pa.attname = ATC.column_name
                    AND pa.attnum > 0
                    AND pa.attgenerated = 'v'
            ) THEN 1
            ELSE 0
        END AS is_computed,
        CASE WHEN ATC.IS_NULLABLE = 'NO' AND ATC.COLUMN_DEFAULT IS NULL THEN 0 ELSE 1 END AS is_nullable
    FROM
        (
            SELECT
                table_schema AS OWNER,
                table_name AS OBJECT_NAME
            FROM
                information_schema.tables
            WHERE
                table_type = 'BASE TABLE'
            UNION
            SELECT
                table_schema AS OWNER,
                table_name AS OBJECT_NAME
            FROM
                information_schema.views
        ) AX
        INNER JOIN information_schema.tables AO ON AX.OWNER = AO.table_schema AND AX.OBJECT_NAME = AO.table_name
        INNER JOIN information_schema.columns ATC ON AX.OWNER = ATC.table_schema AND AX.OBJECT_NAME = ATC.table_name
    WHERE
        AX.OWNER NOT IN ('information_schema')
        AND AX.OWNER NOT LIKE 'pg_%'
    ORDER BY
        full_object_name, ATC.ORDINAL_POSITION;
    "

    $objects = New-Object System.Collections.ArrayList
    $object = @{}

    # Process in one pass
    Invoke-PostgreSQLSqlCommand $sql_command | ForEach-Object {
        if ($_.full_object_name -ne $object.full_name) {
            if ($object.full_name -ne $null) {
                $objects.Add($object) | Out-Null
            }

            $object = @{
                full_name = $_.full_object_name
                type      = $_.object_type
                columns   = New-Object System.Collections.ArrayList
            }
        }

        $object.columns.Add(@{
            name           = $_.column_name
            is_primary_key = $_.is_primary_key
            is_identity    = $_.is_identity
            is_computed    = $_.is_computed
            is_nullable    = $_.is_nullable
        }) | Out-Null
    }

    if ($object.full_name -ne $null) {
        $objects.Add($object) | Out-Null
    }

    Dispose-PostgreSQLSqlCommand $sql_command

    $Global:SqlInfoCache.Objects = $objects
    $Global:SqlInfoCache.Ts = Get-Date
}


function Idm-Dispatcher {
    param (
        # Optional Class/Operation
        [string] $Class,
        [string] $Operation,
        # Mode
        [switch] $GetMeta,
        # Parameters
        [string] $SystemParams,
        [string] $FunctionParams
    )

    Log info "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-PostgreSQLSqlConnection $SystemParams

            Fill-SqlInfoCache -Force

            #
            # Output list of supported operations per table/view (named Class)
            #

            @(
                foreach ($object in $Global:SqlInfoCache.Objects) {
                    $primary_keys = $object.columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name }

                    if ($object.type -ne 'Table') {
                        # Non-tables only support 'Read'
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = 'R'
                        }
                    }
                    else {
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Create'
                        }

                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = "CR$(if ($primary_keys) { 'UD' } else { '' })"
                        }

                        if ($primary_keys) {
                            # Only supported if primary keys are present
                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Update'
                            }

                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Delete'
                            }
                        }
                    }
                }
            )

        }
        else {
            # Purposely no-operation.
        }

    }
    else {

        if ($GetMeta) {
            #
            # Get meta data
            #

            Open-PostgreSQLSqlConnection $SystemParams

            Fill-SqlInfoCache

            $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

            switch ($Operation) {
                'Create' {
                    @{
                        semantics = 'create'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_identity -or $_.is_computed) { 'prohibited' } elseif (! $_.is_nullable) { 'mandatory' } else { 'optional' }
                                }
                            }
                        )
                    }
                    break
                }

                'Read' {
                    @(
                        @{
                            name = 'where_clause'
                            type = 'textbox'
                            label = 'Filter (SQL where-clause)'
                            tooltip = 'Applied SQL where-clause'
                            value = ''
                        }
                        @{
                            name = 'selected_columns'
                            type = 'grid'
                            label = 'Include columns'
                            tooltip = 'Selected columns'
                            table = @{
                                rows = @($columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        config = @(
                                            if ($_.is_primary_key) { 'Primary key' }
                                            if ($_.is_identity)    { 'Generated' }
                                            if ($_.is_computed)    { 'Computed' }
                                            if ($_.is_nullable)    { 'Nullable' }
                                        ) -join ' | '
                                    }
                                })
                                settings_grid = @{
                                    selection = 'multiple'
                                    key_column = 'name'
                                    checkbox = $true
                                    filter = $true
                                    columns = @(
                                        @{
                                            name = 'name'
                                            display_name = 'Name'
                                        }
                                        @{
                                            name = 'config'
                                            display_name = 'Configuration'
                                        }
                                    )
                                }
                            }
                            value = @($columns | ForEach-Object { $_.name })
                        }
                    )
                    break
                }

                'Update' {
                    @{
                        semantics = 'update'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
                                    allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }

                'Delete' {
                    @{
                        semantics = 'delete'
                        parameters = @(
                            $columns | ForEach-Object {
                                if ($_.is_primary_key) {
                                    @{
                                        name = $_.name
                                        allowance = 'mandatory'
                                    }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }
            }

        }
        else {
            #
            # Execute function
            #

            Open-PostgreSQLSqlConnection $SystemParams

            if (! $Global:ColumnsInfoCache[$Class]) {
                Fill-SqlInfoCache

                $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

                $Global:ColumnsInfoCache[$Class] = @{
                    primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                    identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
                }
            }

            $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            # Replace $null by [System.DBNull]::Value
            $keys_with_null_value = @()
            foreach ($key in $function_params.Keys) { if ($function_params[$key] -eq $null) { $keys_with_null_value += $key } }
            foreach ($key in $keys_with_null_value) { $function_params[$key] = [System.DBNull]::Value }

            $sql_command1 = New-PostgreSQLSqlCommand

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { """$_""" }) -join ', ' }

            switch ($Operation) {
                'Create' {
                    if ($identity_col) {
                        $sql_command1.CommandText = "
                            BEGIN
                                DBMS_OUTPUT.ENABLE;
                                DECLARE nim_identity_col $($Class).""$identity_col""%TYPE;
                                BEGIN
                                    INSERT INTO $Class (
                                        " + (@($function_params.Keys | ForEach-Object { """$_""" }) -join ', ') + "
                                    )
                                    VALUES (
                                        $(@($function_params.Keys | ForEach-Object { AddParam-PostgreSQLSqlCommand $sql_command1 $function_params[$_] }) -join ', ')
                                    )
                                    RETURNING
                                        ""$identity_col""
                                    INTO
                                        nim_identity_col;
                                    DBMS_OUTPUT.PUT_LINE(nim_identity_col);
                                    DBMS_OUTPUT.GET_LINE(:buffer, :status);
                                END;
                            END;
                        "

                        $p_buffer = New-Object PostgreSQL.ManagedDataAccess.Client.PostgreSQLParameter(":buffer", [System.Data.SqlDbType]::VarChar2, 32767, "", [System.Data.ParameterDirection]::Output)
                        $p_status = New-Object PostgreSQL.ManagedDataAccess.Client.PostgreSQLParameter(":status", [System.Data.SqlDbType]::Decimal,             [System.Data.ParameterDirection]::Output)

                        $sql_command1.Parameters.Add($p_buffer) | Out-Null
                        $sql_command1.Parameters.Add($p_status) | Out-Null

                        $deparam_command = DeParam-PostgreSQLSqlCommand $sql_command1

                        LogIO info 'INSERT' -In -Command $deparam_command

                        Invoke-PostgreSQLSqlCommand $sql_command1 $deparam_command

                        if ($p_status.Value.ToInt32() -ne 0) {
                            $message = "Status $($p_status.Value.ToInt32()) returned by command: $deparam_command"
                            Log error "Failed: $message"
                            Write-Error $message
                        }

                        $sql_command2 = New-PostgreSQLSqlCommand

                        $filter = """$identity_col"" = $(AddParam-PostgreSQLSqlCommand $sql_command2 $p_buffer.Value)"
                    }
                    else {
                        $sql_command1.CommandText = "
                            INSERT INTO $Class (
                                " + (@($function_params.Keys | ForEach-Object { """$_""" }) -join ', ') + "
                            )
                            VALUES (
                                $(@($function_params.Keys | ForEach-Object { AddParam-PostgreSQLSqlCommand $sql_command1 $function_params[$_] }) -join ', ')
                            )
                        "

                        $deparam_command = DeParam-PostgreSQLSqlCommand $sql_command1

                        LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                        Invoke-PostgreSQLSqlCommand $sql_command1 $deparam_command

                        $sql_command2 = New-PostgreSQLSqlCommand

                        $filter = if ($primary_keys) {
                            @($primary_keys | ForEach-Object { """$_"" = $(AddParam-PostgreSQLSqlCommand $sql_command2 $function_params[$_])" }) -join ' AND '
                        }
                        else {
                            @($function_params.Keys | ForEach-Object { """$_"" = $(AddParam-PostgreSQLSqlCommand $sql_command2 $function_params[$_])" }) -join ' AND '
                        }
                    }

                    # Do not process
                    $sql_command1.CommandText = ""

                    $sql_command2.CommandText = "
                        SELECT
                            $projection
                        FROM
                            $Class
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "

                    $deparam_command = DeParam-PostgreSQLSqlCommand $sql_command2

                    # Log output
                    $rv = Invoke-PostgreSQLSqlCommand $sql_command2 $deparam_command | ForEach-Object { $_ }
                    LogIO info 'INSERT' -Out $rv

                    $rv

                    Dispose-PostgreSQLSqlCommand $sql_command2
                    break
                }

                'Read' {
                    $filter = if ($function_params['where_clause'].length -eq 0) { '' } else { " WHERE $($function_params['where_clause'])" }

                    $sql_command1.CommandText = "
                        SELECT
                            $projection
                        FROM
                            $Class$filter
                    "
                    break
                }

                'Update' {
                    $filter = @($primary_keys | ForEach-Object { """$_"" = $(AddParam-PostgreSQLSqlCommand $sql_command1 $function_params[$_])" }) -join ' AND '

                    $sql_command1.CommandText = "
                        UPDATE
                            $Class
                        SET
                            " + (@($function_params.Keys | ForEach-Object { if ($_ -notin $primary_keys) { """$_"" = $(AddParam-PostgreSQLSqlCommand $sql_command1 $function_params[$_])" } }) -join ', ') + "
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "

                    $deparam_command = DeParam-PostgreSQLSqlCommand $sql_command1

                    LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                    Invoke-PostgreSQLSqlCommand $sql_command1 $deparam_command

                    $sql_command2 = New-PostgreSQLSqlCommand

                    $filter = @($primary_keys | ForEach-Object { """$_"" = $(AddParam-PostgreSQLSqlCommand $sql_command2 $function_params[$_])" }) -join ' AND '

                    # Do not process
                    $sql_command1.CommandText = ""

                    $sql_command2.CommandText = "
                        SELECT
                            " + (@($function_params.Keys | ForEach-Object { """$_""" }) -join ', ') + "
                        FROM
                            $Class
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "

                    $deparam_command = DeParam-PostgreSQLSqlCommand $sql_command2

                    # Log output
                    $rv = Invoke-PostgreSQLSqlCommand $sql_command2 $deparam_command | ForEach-Object { $_ }
                    LogIO info 'UPDATE' -Out $rv

                    $rv

                    Dispose-PostgreSQLSqlCommand $sql_command2
                    break
                }

                'Delete' {
                    $filter = @($primary_keys | ForEach-Object { """$_"" = $(AddParam-PostgreSQLSqlCommand $sql_command1 $function_params[$_])" }) -join ' AND '

                    $sql_command1.CommandText = "
                        DELETE
                            $Class
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "
                    break
                }
            }

            if ($sql_command1.CommandText) {
                $deparam_command = DeParam-PostgreSQLSqlCommand $sql_command1

                LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                if ($Operation -eq 'Read') {
                    # Streamed output
                    Invoke-PostgreSQLSqlCommand $sql_command1 $deparam_command
                }
                else {
                    # Log output
                    $rv = Invoke-PostgreSQLSqlCommand $sql_command1 $deparam_command | ForEach-Object { $_ }
                    LogIO info ($deparam_command -split ' ')[0] -Out $rv

                    $rv
                }
            }

            Dispose-PostgreSQLSqlCommand $sql_command1

        }

    }

    Log info "Done"
}


#
# Helper functions
#

function New-PostgreSQLSqlCommand {
    param (
        [string] $CommandText
    )

    $sql_command = New-Object System.Data.Odbc.OdbcCommand($CommandText, $Global:PostgreSQLSqlConnection)

    return $sql_command
}


function Dispose-PostgreSQLSqlCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand
    )

    $SqlCommand.Dispose()
}


function AddParam-PostgreSQLSqlCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand,
        $Param
    )

    $param_name = ":param$($SqlCommand.Parameters.Count)_"
    $param_value = if ($Param -isnot [system.array]) { $Param } else { $Param | ConvertTo-Json -Compress -Depth 32 }

    $SqlCommand.Parameters.Add($param_name, $param_value) | Out-Null

    return $param_name
}


function DeParam-PostgreSQLSqlCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand
    )

    $deparam_command = $SqlCommand.CommandText

    foreach ($p in $SqlCommand.Parameters) {
        if ($p.Direction -eq [System.Data.ParameterDirection]::Output) {
            continue
        }

        $value_txt = 
            if ($p.Value -eq [System.DBNull]::Value) {
                'NULL'
            }
            else {
                switch ($p.SqlDbType) {
                    { $_ -in @(
                        [System.Data.SqlDbType]::Char
                        [System.Data.SqlDbType]::Date
                        [System.Data.SqlDbType]::NChar
                        [System.Data.SqlDbType]::NVarChar
                        [System.Data.SqlDbType]::NVarChar2
                        [System.Data.SqlDbType]::TimeStamp
                        [System.Data.SqlDbType]::TimeStampLTZ
                        [System.Data.SqlDbType]::TimeStampTZ
                        [System.Data.SqlDbType]::VarChar
                        [System.Data.SqlDbType]::VarChar2
                        [System.Data.SqlDbType]::XmlType
                    )} {
                        "'" + $p.Value.ToString().Replace("'", "''") + "'"
                        break
                    }
        
                    default {
                        $p.Value.ToString().Replace("'", "''")
                        break
                    }
                }
            }

        $deparam_command = $deparam_command.Replace($p.ParameterName, $value_txt)
    }

    # Make one single line
    @($deparam_command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
}


function Invoke-PostgreSQLSqlCommand {
    param (
        [System.Data.Odbc.OdbcCommand] $SqlCommand,
        [string] $DeParamCommand
    )

    # Streaming
    function Invoke-PostgreSQLSqlCommand-ExecuteReader {
        param (
            [System.Data.Odbc.OdbcCommand] $SqlCommand
        )

        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            $hash_table = [ordered]@{}

            foreach ($column_name in $column_names) {
                $hash_table[$column_name] = ""
            }

            $obj = New-Object -TypeName PSObject -Property $hash_table

            # Read data
            while ($data_reader.Read()) {
                foreach ($column_name in $column_names) {
                    $obj.$column_name = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
                }

                # Output data
                $obj
            }

        }

        $data_reader.Close()
    }

    if (! $DeParamCommand) {
        $DeParamCommand = DeParam-PostgreSQLSqlCommand $SqlCommand
    }

    Log debug $DeParamCommand

    try {
        Invoke-PostgreSQLSqlCommand-ExecuteReader $SqlCommand
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }

    Log debug "Done"
}


function Open-PostgreSQLSqlConnection {
    param (
        [string] $ConnectionParams
    )

    $connection_params = ConvertFrom-Json2 $ConnectionParams

    $connectionString = "Driver=$($connection_params.Driver);Server=$($connection_params.Hostname);Port=$($connection_params.Port);Database=$($connection_params.Database);Uid=$($connection_params.Username);Pwd=$($connection_params.Password)"

    if ($Global:PostgreSQLSqlConnection -and $connection_string -ne $Global:PostgreSQLSqlConnectionString) {
        Log info "PostgreSQLSqlConnection connection parameters changed"
        Close-PostgreSQLSqlConnection
    }

    if ($Global:PostgreSQLSqlConnection -and $Global:PostgreSQLSqlConnection.State -ne 'Open') {
        Log warn "PostgreSQLSqlConnection State is '$($Global:PostgreSQLSqlConnection.State)'"
        Disconnect-PostgreSQL -Connection $Global:PostgreSQLSqlConnection
    }

    if ($Global:PostgreSQLSqlConnection) {
        #Log debug "Reusing PostgreSQLSqlConnection"
    }
    else {
        Log info "Opening PostgreSQLSqlConnection '$connection_string'"

        try {
            $connection = New-Object System.Data.Odbc.OdbcConnection
            $connection.ConnectionString = $connectionString
            $connection.Open()

            $Global:PostgreSQLSqlConnection       = $connection
            $Global:PostgreSQLSqlConnectionString = $connection_string

            $Global:ColumnsInfoCache = @{}
            $Global:SqlInfoCache = @{}
        }
        catch {
            Log error "Failed: $_"
            Write-Error $_
        }

        Log info "Done"
    }
}


function Close-PostgreSQLSqlConnection {
    if ($Global:PostgreSQLSqlConnection) {
        Log info "Closing PostgreSQLSqlConnection"

        try {
            $Global:PostgreSQLSqlConnection.Close()
            $Global:PostgreSQLSqlConnection = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log info "Done"
    }
}
