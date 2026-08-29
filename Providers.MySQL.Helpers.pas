unit Providers.MySQL.Helpers;

interface
uses Core.Helpers, Core.Interfaces, Core.Types, System.SysUtils, System.StrUtils,
  System.Classes, Data.DB, Uni;

function NormalizeMariaDB10SQLText(const SQL: string): string;

type
  TMySQLHelpers = class(TDBHelpers, ICheckConstraintHelpers)
  private
    FMariaDB10Compat: Boolean;
    function IsGeneratedColumn(const Col: TColumnInfo): Boolean;
    function GeneratedStorageClause(const Col: TColumnInfo): string;
    function NormalizeMariaDB10SQL(const SQL: string): string;
    function NormalizeCheckConstraintClause(const Clause: string): string;
    function QuoteDynamicSQL(const SQL: string): string;
  public
    constructor Create(const MariaDB10Compat: Boolean = False);
    function QuoteIdentifier(const Identifier: string): string; override;
    function GenerateColumnDefinition(const Col: TColumnInfo): string; override;
    function GenerateIndexDefinition(const TableName: string;
                                     const Idx: TIndexInfo): string; override;
    function CheckConstraintsAreEqual(const Check1,
      Check2: TCheckConstraintInfo): Boolean;
    function GenerateAddCheckConstraintSQL(const TableName: string;
      const CheckConstraint: TCheckConstraintInfo): string;
    function GenerateDropCheckConstraintSQL(const TableName,
      ConstraintName: string): string;
    function NormalizeType(const AType: string): string; override;
    function TriggersAreEqual(const Trg1, Trg2: TTriggerInfo): Boolean; override;
    function GenerateCreateTableSQL(const Table: TTableInfo;
                                    const Indexes: TArray<TIndexInfo>): string; override;
    function GenerateAddColumnSQL(const TableName:string;
                                  const ColumnInfo:TColumnInfo): string; override;
    function GenerateDropColumnSQL(const TableName, ColumnName:string): string; override;
    function GenerateModifyColumnSQL(const TableName:string;
                                     const ColumnInfo:TColumnInfo): string; override;
    function GenerateUpdateSQL(const TableName: string;
                                  const SetClause, WhereClause: string): string; override;
    function GenerateDropIndexSQL(const TableName,
                                        IndexName:string): string; override;
    function GenerateDropTableSQL(const TableName:String): string; override;
    function GenerateDropTrigger(const Trigger:string):string; override;
    function GenerateDropProcedure(const Proc:string):string; override;
    function GenerateDropFunction(const FuncName: string): string; override;
    function GenerateDropView(const View:string):string; override;
    function ValueToSQL(const Field: TField): string; override;
    function GenerateCreateProcedureSQL(const Body: string): string; override;
    function GenerateCreateFunctionSQL(const Body: string): string; override;
    function GenerateCreateSequence(const SeqName: string): string; override;
    function GenerateDropSequence(const SeqName: string): string; override;
    function GenerateCreateTriggerSQL(const Body: string): string;
    function GenerateCreateViewSQL(const Body: string): string;
    function GenerateDeleteSQL(const TableName, WhereClause: string): string; override;
    function GenerateInsertSQL(const TableName: string;
                           Fields, Values: TStringList;
                           const HasIdentity: Boolean = False): string; override;

  end;

implementation

// Añadir en uses: Data.DB, System.SysUtils, System.Classes, System.StrUtils

function AddUtf8mb4CastCollations(const SQL: string): string;
var
  I, J, K, Depth: Integer;
  Inner: string;
  QuoteChar: Char;
begin
  Result := SQL;
  I := 1;
  while I <= Length(Result) - 4 do
  begin
    if not SameText(Copy(Result, I, 5), 'CAST(') then
    begin
      Inc(I);
      Continue;
    end;
    J := I + 5;
    Depth := 1;
    QuoteChar := #0;
    while (J <= Length(Result)) and (Depth > 0) do
    begin
      if QuoteChar <> #0 then
      begin
        if (Result[J] = QuoteChar) then
        begin
          if (J < Length(Result)) and (Result[J + 1] = QuoteChar) then
            Inc(J)
          else
            QuoteChar := #0;
        end
        else if (Result[J] = '\') and (J < Length(Result)) then
          Inc(J);
      end
      else
      begin
        if CharInSet(Result[J], ['''', '"', '`']) then
          QuoteChar := Result[J]
        else if Result[J] = '(' then
          Inc(Depth)
        else if Result[J] = ')' then
          Dec(Depth);
      end;
      Inc(J);
    end;
    if Depth <> 0 then
      Break;
    Dec(J);
    Inner := Trim(Copy(Result, I + 5, J - I - 5));
    if EndsText(' AS CHAR CHARSET UTF8MB4', Inner) then
    begin
      K := J + 1;
      while (K <= Length(Result)) and CharInSet(Result[K], [' ', #9, #13, #10]) do
        Inc(K);
      if not SameText(Copy(Result, K, 7), 'COLLATE') then
      begin
        Insert(' COLLATE utf8mb4_spanish_ci', Result, J + 1);
        I := J + Length(' COLLATE utf8mb4_spanish_ci') + 1;
        Continue;
      end;
    end;
    I := J + 1;
  end;
end;

function NormalizeMariaDB10SQLText(const SQL: string): string;
begin
  Result := SQL;
  Result := StringReplace(Result, 'CURRENT_TIMESTAMP()', 'CURRENT_TIMESTAMP',
    [rfReplaceAll, rfIgnoreCase]);
  Result := StringReplace(Result, 'CREATE OR REPLACE TABLE', 'CREATE TABLE',
    [rfReplaceAll, rfIgnoreCase]);
  Result := StringReplace(Result, 'utf8mb4_uca1400_ai_ci',
    'utf8mb4_spanish_ci', [rfReplaceAll, rfIgnoreCase]);
  Result := StringReplace(Result, 'utf8mb3_uca1400_ai_ci',
    'utf8_spanish_ci', [rfReplaceAll, rfIgnoreCase]);
  Result := StringReplace(Result, 'utf8mb3', 'utf8',
    [rfReplaceAll, rfIgnoreCase]);
  Result := AddUtf8mb4CastCollations(Result);
end;

constructor TMySQLHelpers.Create(const MariaDB10Compat: Boolean);
begin
  inherited Create;
  FMariaDB10Compat := MariaDB10Compat;
end;

function TMySQLHelpers.NormalizeMariaDB10SQL(const SQL: string): string;
begin
  Result := SQL;
  if not FMariaDB10Compat then
    Exit;
  Result := NormalizeMariaDB10SQLText(Result);
end;

function TMySQLHelpers.NormalizeCheckConstraintClause(
  const Clause: string): string;
var
  CurrentChar, QuoteChar: Char;
  I: Integer;
begin
  Result := '';
  QuoteChar := #0;
  I := 1;
  while I <= Length(Clause) do
  begin
    CurrentChar := Clause[I];
    if QuoteChar = #0 then
    begin
      if CharInSet(CurrentChar, ['''', '"', '`']) then
      begin
        QuoteChar := CurrentChar;
        Result := Result + CurrentChar;
      end
      else if not CharInSet(CurrentChar, [' ', #9, #10, #13]) then
        Result := Result + LowerCase(string(CurrentChar));
    end
    else
    begin
      if QuoteChar = '`' then
        Result := Result + LowerCase(string(CurrentChar))
      else
        Result := Result + CurrentChar;
      if CurrentChar = QuoteChar then
      begin
        if (I < Length(Clause)) and (Clause[I + 1] = QuoteChar) then
        begin
          Inc(I);
          if QuoteChar = '`' then
            Result := Result + LowerCase(string(Clause[I]))
          else
            Result := Result + Clause[I];
        end
        else
          QuoteChar := #0;
      end
      else if (CurrentChar = '\') and (I < Length(Clause)) then
      begin
        Inc(I);
        Result := Result + Clause[I];
      end;
    end;
    Inc(I);
  end;
end;

function TMySQLHelpers.QuoteDynamicSQL(const SQL: string): string;
begin
  Result := QuotedStr(StringReplace(SQL, '\', '\\', [rfReplaceAll]));
end;

function TMySQLHelpers.CheckConstraintsAreEqual(const Check1,
  Check2: TCheckConstraintInfo): Boolean;
begin
  Result := SameText(Check1.ConstraintName, Check2.ConstraintName) and
    (NormalizeCheckConstraintClause(Check1.CheckClause) =
     NormalizeCheckConstraintClause(Check2.CheckClause));
end;

function TMySQLHelpers.GenerateAddCheckConstraintSQL(
  const TableName: string;
  const CheckConstraint: TCheckConstraintInfo): string;
var
  Command: string;
begin
  Command := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
    ' ADD CONSTRAINT ' + QuoteIdentifier(CheckConstraint.ConstraintName) +
    ' CHECK (' + NormalizeMariaDB10SQL(CheckConstraint.CheckClause) + ')';
  if not FMariaDB10Compat then
    Result := Command + ';'
  else
    Result :=
      'SET @check_exists := (' +
      'SELECT COUNT(*) FROM information_schema.table_constraints ' +
      'WHERE constraint_schema = DATABASE() AND table_name = ' +
      QuotedStr(TableName) + ' AND constraint_name = ' +
      QuotedStr(CheckConstraint.ConstraintName) +
      ' AND constraint_type = ''CHECK'');' + sLineBreak +
      'SET @sql_check := IF(@check_exists = 0, ' +
      QuoteDynamicSQL(Command) + ', ' + QuoteDynamicSQL('SELECT 1') + ');' +
      sLineBreak +
      'PREPARE stmt_check FROM @sql_check;' + sLineBreak +
      'EXECUTE stmt_check;' + sLineBreak +
      'DEALLOCATE PREPARE stmt_check;';
end;

function TMySQLHelpers.GenerateDropCheckConstraintSQL(
  const TableName, ConstraintName: string): string;
var
  Command: string;
begin
  Command := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
    ' DROP CONSTRAINT ' + QuoteIdentifier(ConstraintName);
  if not FMariaDB10Compat then
    Result := Command + ';'
  else
    Result :=
      'SET @check_exists := (' +
      'SELECT COUNT(*) FROM information_schema.table_constraints ' +
      'WHERE constraint_schema = DATABASE() AND table_name = ' +
      QuotedStr(TableName) + ' AND constraint_name = ' +
      QuotedStr(ConstraintName) + ' AND constraint_type = ''CHECK'');' +
      sLineBreak +
      'SET @sql_check := IF(@check_exists > 0, ' +
      QuoteDynamicSQL(Command) + ', ' + QuoteDynamicSQL('SELECT 1') + ');' +
      sLineBreak +
      'PREPARE stmt_check FROM @sql_check;' + sLineBreak +
      'EXECUTE stmt_check;' + sLineBreak +
      'DEALLOCATE PREPARE stmt_check;';
end;

function TMySQLHelpers.ValueToSQL(const Field: TField): string;
  function BytesToHex(const Bytes: TBytes): string;
  var
    i: Integer;
  begin
    Result := '';
    for i := Low(Bytes) to High(Bytes) do
      Result := Result + IntToHex(Bytes[i], 2);
  end;
var
  InvariantFmt: TFormatSettings;
begin
  if Field.IsNull then
    Exit('NULL');

  // Configuración invariante: punto decimal, sin separador de miles
  InvariantFmt := TFormatSettings.Invariant;

  case Field.DataType of
    ftString, ftWideString, ftMemo, ftWideMemo, ftFmtMemo:
      begin
        // 2. Escapa la barra invertida primero
        var SafeStr := StringReplace(Field.AsString, '\', '\\', [rfReplaceAll]);

        // 3. QuotedStr se encarga de las comillas simples
        Result := QuotedStr(SafeStr);
      end;

    ftDate, ftTime, ftDateTime, ftTimeStamp:
      Result := QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Field.AsDateTime));

    ftBoolean:
      Result := IntToStr(Ord(Field.AsBoolean));

    ftBlob, ftGraphic, ftVarBytes, ftBytes:
      Result := '0x' + BytesToHex(Field.AsBytes);

    ftSmallint, ftInteger, ftWord, ftLargeint, ftAutoInc:
      Result := Field.AsString;  // Los enteros no tienen separador decimal

    ftFloat, ftCurrency, ftBCD, ftFMTBcd, ftExtended, ftSingle:
      // Usar FloatToStr con formato invariante garantiza punto decimal
      Result := FloatToStr(Field.AsFloat, InvariantFmt);

    else
      // Para cualquier otro tipo numérico no contemplado arriba,
      // intentamos con AsFloat primero; si falla, caemos a AsString
      // y reemplazamos la coma por punto como red de seguridad.
      begin
        try
          Result := FloatToStr(Field.AsFloat, InvariantFmt);
        except
          Result := StringReplace(Field.AsString, ',', '.', [rfReplaceAll]);
        end;
      end;
  end;
end;

function TMySQLHelpers.GenerateInsertSQL(const TableName: string;
                           Fields, Values: TStringList;
                           const HasIdentity: Boolean = False): string;
var
  i: Integer;
  FieldList, ValueList: string;
begin
  // Unimos manualmente para evitar que CommaText escape cosas que no debe
  FieldList := '';
  ValueList := '';
  for i := 0 to Fields.Count - 1 do
  begin
    if i > 0 then FieldList := FieldList + ', ';
    FieldList := FieldList + Fields[i];
  end;
  for i := 0 to Values.Count - 1 do
  begin
    if i > 0 then ValueList := ValueList + ', ';
    ValueList := ValueList + Values[i];
  end;
  Result := 'INSERT INTO ' + QuoteIdentifier(TableName) + ' (' +
            FieldList + ') VALUES (' + ValueList + ');';
end;

function TMySQLHelpers.GenerateUpdateSQL(const TableName: string;
  const SetClause, WhereClause: string): string;
begin
  Result := 'UPDATE ' + QuoteIdentifier(TableName) + ' SET ' + SetClause +
            ' WHERE ' + WhereClause + ';';
end;

function TMySQLHelpers.TriggersAreEqual(const Trg1,
                                                   Trg2: TTriggerInfo): Boolean;
begin
  Result := SameText(Trg1.TriggerName, Trg2.TriggerName) and
            (Trg1.EventManipulation = Trg2.EventManipulation) and
            (Trg1.ActionTiming = Trg2.ActionTiming) and
            (Trim(Trg1.ActionStatement) = Trim(Trg2.ActionStatement));
end;

function TMySQLHelpers.QuoteIdentifier(const Identifier: string): string;
begin
  Result := '`' + Identifier + '`';
end;

function TMySQLHelpers.NormalizeType(const AType: string): string;
var
  S: string;
  PStart, PEnd: Integer;
begin
  S := LowerCase(Trim(AType));
  // ESPECÍFICO MYSQL: Eliminar display width de INT(11)
  if StartsText('int', S) or StartsText('tinyint', S) then
  begin
    PStart := Pos('(', S);
    PEnd := Pos(')', S);
    if (PStart > 0) and (PEnd > PStart) then
      S := Copy(S, 1, PStart - 1) + Copy(S, PEnd + 1, MaxInt);
  end;
  Result := StringReplace(S, ' ', '', [rfReplaceAll]);
end;

function TMySQLHelpers.GenerateAddColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' ADD COLUMN ';
  if FMariaDB10Compat then
    Result := Result + 'IF NOT EXISTS ';
  Result := Result + GenerateColumnDefinition(ColumnInfo);
  if FMariaDB10Compat then
  begin
    if ColumnInfo.OrdinalPosition = 1 then
      Result := Result + ' FIRST'
    else if ColumnInfo.PreviousColumnName <> '' then
      Result := Result + ' AFTER ' +
        QuoteIdentifier(ColumnInfo.PreviousColumnName);
  end
  else if (Pos('auto_increment', LowerCase(ColumnInfo.Extra)) > 0) and
     SameText(ColumnInfo.ColumnKey, 'PRI') then
  begin
    Result := Result + ', ADD PRIMARY KEY (' + QuoteIdentifier(ColumnInfo.ColumnName) + ')';
  end;
  Result := Result + ';';
end;

function TMySQLHelpers.GenerateColumnDefinition(const Col: TColumnInfo): string;
var
  DefVal, DefaultSQL: string;
  IsGenerated: Boolean;
begin
  // 1. Definición básica: `Nombre` Tipo
  Result := '`' + Col.ColumnName + '` ' + Col.DataType;
  IsGenerated := IsGeneratedColumn(Col);
  if IsGenerated then
    Result := Result + ' GENERATED ALWAYS AS (' +
      NormalizeMariaDB10SQL(Trim(Col.GenerationExpression)) + ')' +
      GeneratedStorageClause(Col)
  // 2. Definir NULL o NOT NULL
  else if SameText(Col.IsNullable, 'NO') then
    Result := Result + ' NOT NULL'
  else
    Result := Result + ' NULL';
  if (not IsGenerated) and (Col.ColumnDefault = '<NULL>') then
  begin
    if not SameText(Col.IsNullable, 'NO') then
      Result := Result + ' DEFAULT NULL';
  end
  else if (not IsGenerated) and (Col.ColumnDefault <> '') then
  begin
    if SameText(Col.ColumnDefault, 'NULL') then
    begin
       Result := Result + ' DEFAULT NULL';
    end
    else if (Pos('CURRENT_TIMESTAMP', UpperCase(Col.ColumnDefault)) > 0) or
            (Pos('NOW()', UpperCase(Col.ColumnDefault)) > 0) then
    begin
      DefaultSQL := NormalizeMariaDB10SQL(Col.ColumnDefault);
      Result := Result + ' DEFAULT ' + DefaultSQL;
    end
    else
    begin
      DefVal := Col.ColumnDefault;
      if (Length(DefVal) >= 2) and (DefVal[1] = '''') and
         (DefVal[Length(DefVal)] = '''') then
        DefVal := Copy(DefVal, 2, Length(DefVal) - 2);
      Result := Result + ' DEFAULT ' + QuotedStr(DefVal);
    end;
  end;
  if (not IsGenerated) and
     (Pos('auto_increment', LowerCase(Col.Extra)) > 0) then
    Result := Result + ' AUTO_INCREMENT';
  if (not IsGenerated) and
     (Pos('on update', LowerCase(Col.Extra)) > 0) then
    Result := Result + ' ON UPDATE CURRENT_TIMESTAMP';
  if not SameText(Col.ColumnComment, '') then
    Result := Result + ' COMMENT ' + QuotedStr(Col.ColumnComment);
end;

function TMySQLHelpers.GeneratedStorageClause(const Col: TColumnInfo): string;
begin
  Result := '';
  if ContainsText(Col.Extra, 'stored') then
    Result := ' STORED'
  else if ContainsText(Col.Extra, 'virtual') then
    Result := ' VIRTUAL';
end;

function TMySQLHelpers.IsGeneratedColumn(const Col: TColumnInfo): Boolean;
begin
  Result := Trim(Col.GenerationExpression) <> '';
end;

function TMySQLHelpers.GenerateCreateProcedureSQL(const Body: string): string;
begin
  Result := 'DELIMITER ;;' + sLineBreak +
            TrimRight(NormalizeMariaDB10SQL(Body)) + ' ;;' + sLineBreak +
            'DELIMITER ;';
end;

function TMySQLHelpers.GenerateCreateFunctionSQL(const Body: string): string;
begin
  Result := 'DELIMITER ;;' + sLineBreak +
            TrimRight(NormalizeMariaDB10SQL(Body)) + ' ;;' + sLineBreak +
            'DELIMITER ;';
end;

function TMySQLHelpers.GenerateCreateTriggerSQL(const Body: string): string;
begin
  Result := 'DELIMITER ;;' + sLineBreak +
            TrimRight(NormalizeMariaDB10SQL(Body)) + ' ;;' + sLineBreak +
            'DELIMITER ;';
end;

function TMySQLHelpers.GenerateCreateViewSQL(const Body: string): string;
begin
  // Las vistas no necesitan cambiar el delimitador
  Result := Body + ';';
end;

function TMySQLHelpers.GenerateCreateTableSQL(const Table: TTableInfo;
  const Indexes: TArray<TIndexInfo>): string;
var
  i, SeparatorPos: Integer;
  Definitions, PKList: TStringList;
  EngineName, TableCollation, CharacterSetName: string;

  function IndexColumnList(const Idx: TIndexInfo): string;
  var
    j: Integer;
  begin
    Result := '';
    for j := 0 to High(Idx.Columns) do
    begin
      if j > 0 then
        Result := Result + ', ';
      Result := Result + QuoteIdentifier(Idx.Columns[j].ColumnName);
    end;
  end;

begin
  Definitions := TStringList.Create;
  PKList := TStringList.Create;
  try
    for i := 0 to Table.Columns.Count - 1 do
    begin
      Definitions.Add(GenerateColumnDefinition(Table.Columns[i]));
      if SameText(Table.Columns[i].ColumnKey, 'PRI') then
        PKList.Add(QuoteIdentifier(Table.Columns[i].ColumnName));
    end;

    if FMariaDB10Compat then
    begin
      for i := 0 to High(Indexes) do
      begin
        if Indexes[i].IsPrimary then
          Definitions.Add('PRIMARY KEY (' + IndexColumnList(Indexes[i]) + ')')
        else if Indexes[i].IsUnique then
          Definitions.Add('UNIQUE KEY ' + QuoteIdentifier(Indexes[i].IndexName) +
            ' (' + IndexColumnList(Indexes[i]) + ')')
        else
          Definitions.Add('KEY ' + QuoteIdentifier(Indexes[i].IndexName) +
            ' (' + IndexColumnList(Indexes[i]) + ')');
      end;
    end;
    if (not FMariaDB10Compat) and (PKList.Count > 0) then
      Definitions.Add('PRIMARY KEY (' + PKList.CommaText + ')');

    if FMariaDB10Compat then
      Result := 'CREATE TABLE IF NOT EXISTS '
    else
      Result := 'CREATE TABLE ';
    Result := Result + QuoteIdentifier(Table.TableName) + ' (' + sLineBreak;
    for i := 0 to Definitions.Count - 1 do
    begin
      Result := Result + '  ' + Definitions[i];
      if i < Definitions.Count - 1 then
        Result := Result + ',';
      Result := Result + sLineBreak;
    end;
    Result := Result + ')';
    if FMariaDB10Compat then
    begin
      EngineName := Table.Engine;
      if EngineName = '' then
        EngineName := 'InnoDB';
      TableCollation := NormalizeMariaDB10SQL(Table.TableCollation);
      if TableCollation = '' then
        TableCollation := 'utf8mb4_spanish_ci';
      SeparatorPos := Pos('_', TableCollation);
      if SeparatorPos > 0 then
        CharacterSetName := Copy(TableCollation, 1, SeparatorPos - 1)
      else
        CharacterSetName := 'utf8mb4';
      Result := Result + ' ENGINE=' + EngineName + ' DEFAULT CHARSET=' +
        CharacterSetName + ' COLLATE=' + TableCollation;
    end;
    Result := Result + ';';
  finally
    Definitions.Free;
    PKList.Free;
  end;
end;

function TMySQLHelpers.GenerateCreateSequence(const SeqName: string): string;
begin
  Result := '';
end;

function TMySQLHelpers.GenerateDropSequence(const SeqName: string): string;
begin
  Result := '';
end;

function TMySQLHelpers.GenerateDeleteSQL(const TableName,
  WhereClause: string): string;
begin
  Result := 'DELETE FROM ' + QuoteIdentifier(TableName) +
            ' WHERE ' + WhereClause + ';';
end;

function TMySQLHelpers.GenerateDropColumnSQL(const TableName,
  ColumnName: string): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' DROP COLUMN ' + QuoteIdentifier(ColumnName) + ';';
end;

function TMySQLHelpers.GenerateDropFunction(const FuncName: string): string;
begin
  Result:= 'DROP FUNCTION IF EXISTS ' + QuoteIdentifier(FuncName) + ';';
end;

function TMySQLHelpers.GenerateDropTableSQL(const TableName:String): string;
begin
  Result := 'DROP TABLE IF EXISTS ' + QuoteIdentifier(TableName) + ';';
end;

function TMySQLHelpers.GenerateDropTrigger(const Trigger: string): string;
begin
  Result := 'DROP TRIGGER IF EXISTS ' + QuoteIdentifier(Trigger) + ';';
end;

function TMySQLHelpers.GenerateDropView(const View: string): string;
begin
  Result := 'DROP VIEW IF EXISTS ' + QuoteIdentifier(View) + ';';
end;

function TMySQLHelpers.GenerateDropIndexSQL(const TableName, IndexName: string): string;
begin
  if SameText(IndexName, 'PRIMARY') then
  begin
    Result :=
      'SET @pk_exists := (SELECT COUNT(*) FROM information_schema.table_constraints ' +
      'WHERE table_schema = DATABASE() AND table_name = ' + QuotedStr(TableName) +
      ' AND constraint_type = ''PRIMARY KEY'');' + sLineBreak +
      'SET @sql_drop := IF(@pk_exists > 0, ' +
      '''ALTER TABLE ' + QuoteIdentifier(TableName) + ' DROP PRIMARY KEY'', ' +
      '''SELECT "No Primary Key to drop"'');' + sLineBreak +
      'PREPARE stmt FROM @sql_drop;' + sLineBreak +
      'EXECUTE stmt;' + sLineBreak +
      'DEALLOCATE PREPARE stmt;';
  end
  else
  begin
    Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
              ' DROP INDEX ' + QuoteIdentifier(IndexName) + ';';
  end;
end;

function TMySQLHelpers.GenerateDropProcedure(const Proc: string): string;
begin
  Result:= 'DROP PROCEDURE IF EXISTS ' + QuoteIdentifier(Proc) + ';';
end;

function TMySQLHelpers.GenerateIndexDefinition(const TableName: string;
                                               const Idx: TIndexInfo): string;
var
  i: Integer;
  ColNames: string;
begin
  // Construir la lista de columnas: `col1`, `col2`...
  ColNames := '';
  for i := 0 to High(Idx.Columns) do
  begin
    if i > 0 then
      ColNames := ColNames + ', ';
    ColNames := ColNames + QuoteIdentifier(Idx.Columns[i].ColumnName);
  end;
  // Lógica principal
  if Idx.IsPrimary then
  begin
    Result :=
      '-- Descomenta para limpieza de duplicados previa a la creación de PK' + sLineBreak +
      '-- DELETE FROM ' + QuoteIdentifier(TableName) + ' WHERE ' +
      '-- (' + ColNames + ') IN (' +
        '-- SELECT ' + ColNames + ' FROM (' +
          '-- SELECT ' + ColNames +
          '-- FROM ' + QuoteIdentifier(TableName) +
          '-- GROUP BY ' + ColNames +
          '-- HAVING COUNT(*) > 1' +
        '-- ) AS c' +
      '-- );' + sLineBreak + sLineBreak +
      '-- Comprobación dinámica para evitar error si el ADD COLUMN ya creó la PK' + sLineBreak +
      'SET @pk_exists := (SELECT COUNT(*) FROM information_schema.table_constraints ' +
      'WHERE table_schema = DATABASE() AND table_name = ' + QuotedStr(TableName) +
      ' AND constraint_type = ''PRIMARY KEY'');' + sLineBreak +
      'SET @sql_add := IF(@pk_exists = 0, ' +
      '''ALTER TABLE ' + QuoteIdentifier(TableName) + ' ADD PRIMARY KEY (' + ColNames + ')'', ' +
      '''SELECT "Primary Key ya existe"'');' + sLineBreak +
      'PREPARE stmt FROM @sql_add;' + sLineBreak +
      'EXECUTE stmt;' + sLineBreak +
      'DEALLOCATE PREPARE stmt;';
  end
  else if Idx.IsUnique then
  begin
    // Aplicamos la misma lógica para índices únicos
    Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
              ' ADD UNIQUE INDEX ';
    if FMariaDB10Compat then
      Result := Result + 'IF NOT EXISTS ';
    Result := Result + QuoteIdentifier(Idx.IndexName) +
              ' (' + ColNames + ');';
  end
  else
  begin
    // Índices normales (no requieren unicidad)
    Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
              ' ADD INDEX ';
    if FMariaDB10Compat then
      Result := Result + 'IF NOT EXISTS ';
    Result := Result + QuoteIdentifier(Idx.IndexName) +
              ' (' + ColNames + ');';
  end;
end;

function TMySQLHelpers.GenerateModifyColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
var
  SanitizeSQL: string;
  MaxLen: Integer;
begin
  SanitizeSQL := '';
  MaxLen := StrToIntDef(ColumnInfo.CharMaxLength, 0);
  if not IsGeneratedColumn(ColumnInfo) and
     (MaxLen > 0) and
     (ContainsText(ColumnInfo.DataType, 'char') or
      ContainsText(ColumnInfo.DataType, 'text')) then
  begin
     SanitizeSQL := SanitizeSQL +
       'UPDATE ' + QuoteIdentifier(TableName) +
       ' SET ' + QuoteIdentifier(ColumnInfo.ColumnName) +
       ' = LEFT(' + QuoteIdentifier(ColumnInfo.ColumnName) + ', ' +
                    IntToStr(MaxLen) + ')' +
       ' WHERE LENGTH(' + QuoteIdentifier(ColumnInfo.ColumnName) + ') > ' +
                          IntToStr(MaxLen) + ';' +
       sLineBreak;
  end;
  if not IsGeneratedColumn(ColumnInfo) and
     SameText(ColumnInfo.IsNullable, 'NO') then
  begin
     SanitizeSQL := SanitizeSQL +
       'UPDATE ' + QuoteIdentifier(TableName) +
       ' SET ' + QuoteIdentifier(ColumnInfo.ColumnName) + ' = ''''' +
       ' WHERE ' + QuoteIdentifier(ColumnInfo.ColumnName) + ' IS NULL;' +
       sLineBreak;
  end;
  Result := SanitizeSQL +
            'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' MODIFY COLUMN ' + GenerateColumnDefinition(ColumnInfo);
  if FMariaDB10Compat then
  begin
    if ColumnInfo.OrdinalPosition = 1 then
      Result := Result + ' FIRST'
    else if ColumnInfo.PreviousColumnName <> '' then
      Result := Result + ' AFTER ' +
        QuoteIdentifier(ColumnInfo.PreviousColumnName);
  end;
  Result := Result + ';';
end;

end.



