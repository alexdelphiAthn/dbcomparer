[CmdletBinding()]
param(
  [Parameter(Mandatory)]
  [string] $DBComparerPath,

  [Parameter(Mandatory)]
  [string] $SourceClientPath,

  [Parameter(Mandatory)]
  [string] $TargetClientPath,

  [string] $SourceHostName = '127.0.0.1',
  [int] $SourcePort = 3306,
  [string] $SourceUserName = 'root',
  [Parameter(Mandatory)]
  [string] $SourcePassword,

  [string] $TargetHostName = '127.0.0.1',
  [int] $TargetPort = 3306,
  [string] $TargetUserName = 'root',
  [Parameter(Mandatory)]
  [string] $TargetPassword
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-MySql {
  param(
    [string] $ClientPath,
    [string] $ServerName,
    [int] $Port,
    [string] $UserName,
    [string] $Password,
    [string] $Database,
    [string] $Sql
  )

  $clientArguments = @(
    '--protocol=tcp'
    "--host=$ServerName"
    "--port=$Port"
    "--user=$UserName"
    "--password=$Password"
    '--batch'
    '--skip-column-names'
  )
  if ($Database) {
    $clientArguments += "--database=$Database"
  }
  $clientArguments += "--execute=$Sql"

  $clientOutput = & $ClientPath @clientArguments 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw ($clientOutput -join [Environment]::NewLine)
  }
  return $clientOutput
}

function Invoke-DBComparer {
  param(
    [string] $SourceDatabase,
    [string] $TargetDatabase,
    [string] $OutputPath,
    [switch] $NoDelete
  )

  $sourceConnection = '{0}:{1}\{2}' -f
    $SourceHostName, $SourcePort, $SourceDatabase
  $targetConnection = '{0}:{1}\{2}' -f
    $TargetHostName, $TargetPort, $TargetDatabase
  $sourceCredentials = '{0}\{1}' -f $SourceUserName, $SourcePassword
  $targetCredentials = '{0}\{1}' -f $TargetUserName, $TargetPassword

  $comparerArguments = @(
    $sourceConnection
    $sourceCredentials
    $targetConnection
    $targetCredentials
    '--mariadb10'
    "--output=$OutputPath"
    '--encoding=utf8nobom'
  )
  if ($NoDelete) {
    $comparerArguments += '--nodelete'
  }

  & $DBComparerPath @comparerArguments
  if ($LASTEXITCODE -ne 0) {
    throw "DBComparer terminó con código $LASTEXITCODE."
  }
}

function Assert-Contains {
  param(
    [string] $Text,
    [string] $Expected
  )

  if (-not $Text.Contains($Expected)) {
    throw "No se encontró en el SQL generado: $Expected"
  }
}

$suffix = '{0}_{1}' -f (Get-Date -Format 'yyyyMMddHHmmss'), $PID
$sourceDatabase = "dbcomparer_check_src_$suffix"
$targetDatabase = "dbcomparer_check_dst_$suffix"
$temporaryBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$temporaryPath = Join-Path $temporaryBase "dbcomparer_check_$suffix"
$noDeleteScript = Join-Path $temporaryPath 'comparacion_nodelete.sql'
$firstScript = Join-Path $temporaryPath 'primera_comparacion.sql'
$secondScript = Join-Path $temporaryPath 'segunda_comparacion.sql'

New-Item -ItemType Directory -Path $temporaryPath | Out-Null

try {
  $sourceSql = @"
CREATE DATABASE $sourceDatabase CHARACTER SET utf8mb4
  COLLATE utf8mb4_spanish_ci;
CREATE TABLE $sourceDatabase.tabla_nueva (
  id INT NOT NULL,
  modo VARCHAR(20) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT CHK_NUEVA_MODO CHECK (modo IN ('A','B')),
  CONSTRAINT CHK_COMPARTIDO CHECK (id > 0)
);
CREATE TABLE $sourceDatabase.tabla_existente (
  id INT NOT NULL,
  valor INT NOT NULL,
  etiqueta VARCHAR(20) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT CHK_VALOR CHECK (valor >= 0),
  CONSTRAINT CHK_ETIQUETA CHECK (etiqueta <> 'x'),
  CONSTRAINT CHK_COMPARTIDO CHECK (id >= 0)
);
"@
  Invoke-MySql $SourceClientPath $SourceHostName $SourcePort `
    $SourceUserName $SourcePassword '' $sourceSql | Out-Null

  $targetSql = @"
CREATE DATABASE $targetDatabase CHARACTER SET utf8mb4
  COLLATE utf8mb4_spanish_ci;
CREATE TABLE $targetDatabase.tabla_existente (
  id INT NOT NULL,
  valor INT NOT NULL,
  etiqueta VARCHAR(20) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT CHK_VALOR CHECK (valor > 0),
  CONSTRAINT CHK_SOBRANTE CHECK (id > 0),
  CONSTRAINT CHK_COMPARTIDO CHECK (id >= 0)
);
"@
  Invoke-MySql $TargetClientPath $TargetHostName $TargetPort `
    $TargetUserName $TargetPassword '' $targetSql | Out-Null

  Invoke-DBComparer $sourceDatabase $targetDatabase $noDeleteScript -NoDelete
  $noDeleteSql = Get-Content -Raw -LiteralPath $noDeleteScript
  if ($noDeleteSql.Contains('DROP CONSTRAINT')) {
    throw '--nodelete generó un DROP CONSTRAINT.'
  }
  Assert-Contains $noDeleteSql `
    'Restricción CHECK distinta conservada por --nodelete: '

  Invoke-DBComparer $sourceDatabase $targetDatabase $firstScript
  $firstSql = Get-Content -Raw -LiteralPath $firstScript
  Assert-Contains $firstSql 'CHK_NUEVA_MODO'
  Assert-Contains $firstSql 'CHK_ETIQUETA'
  Assert-Contains $firstSql 'CHK_VALOR'
  Assert-Contains $firstSql 'CHK_SOBRANTE'
  Assert-Contains $firstSql 'CHK_COMPARTIDO'
  Assert-Contains $firstSql "constraint_type = 'CHECK'"

  $sourceCommand = 'source ' + ($firstScript -replace '\\', '/')
  Invoke-MySql $TargetClientPath $TargetHostName $TargetPort `
    $TargetUserName $TargetPassword $targetDatabase $sourceCommand | Out-Null

  Invoke-DBComparer $sourceDatabase $targetDatabase $secondScript
  $secondSql = Get-Content -Raw -LiteralPath $secondScript
  if ($secondSql.Contains("constraint_type = 'CHECK'")) {
    throw 'La segunda comparación volvió a generar cambios de CHECK.'
  }

  Invoke-MySql $TargetClientPath $TargetHostName $TargetPort `
    $TargetUserName $TargetPassword $targetDatabase $sourceCommand | Out-Null

  $invalidValueWasRejected = $false
  try {
    Invoke-MySql $TargetClientPath $TargetHostName $TargetPort `
      $TargetUserName $TargetPassword $targetDatabase `
      "INSERT INTO tabla_nueva (id, modo) VALUES (1, 'C');" | Out-Null
  }
  catch {
    $invalidValueWasRejected = $true
  }
  if (-not $invalidValueWasRejected) {
    throw 'MariaDB 10.2 no aplicó la restricción CHECK generada.'
  }

  Write-Output 'Prueba de restricciones CHECK completada correctamente.'
}
finally {
  try {
    Invoke-MySql $SourceClientPath $SourceHostName $SourcePort `
      $SourceUserName $SourcePassword '' `
      "DROP DATABASE IF EXISTS $sourceDatabase;" | Out-Null
  }
  catch {
    Write-Warning "No se pudo eliminar ${sourceDatabase}: $($_.Exception.Message)"
  }
  try {
    Invoke-MySql $TargetClientPath $TargetHostName $TargetPort `
      $TargetUserName $TargetPassword '' `
      "DROP DATABASE IF EXISTS $targetDatabase;" | Out-Null
  }
  catch {
    Write-Warning "No se pudo eliminar ${targetDatabase}: $($_.Exception.Message)"
  }

  $resolvedTemporaryPath = [IO.Path]::GetFullPath($temporaryPath)
  if ($resolvedTemporaryPath.StartsWith(
      $temporaryBase, [StringComparison]::OrdinalIgnoreCase)) {
    Remove-Item -LiteralPath $resolvedTemporaryPath -Recurse -Force
  }
}
