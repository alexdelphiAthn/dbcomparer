unit Providers.Oracle.Helpers;

interface
uses Core.Helpers, Core.Types, System.SysUtils, System.StrUtils,
  System.Classes, Data.DB, Uni;
type
  TOracleHelpers = class(TDBHelpers)
  public
    function QuoteIdentifier(const Identifier: string): string; override;
    function GenerateColumnDefinition(const Col: TColumnInfo): string; override;
    function GenerateIndexDefinition(const TableName: string;
                                     const Idx: TIndexInfo): string; override;
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
    function ValueToSQL(const Field: TField): string;
    function GenerateCreateProcedureSQL(const Body: string): string; override;
    function GenerateCreateFunctionSQL(const Body: string): string; override;
    function GenerateDeleteSQL(const TableName, WhereClause: string): string; override;
    function GenerateInsertSQL(const TableName: string; Fields,
                                                        Values: TStringList): string; override;
    function GenerateCreateSequenceSQL(const SequenceName: string): string;
    function GenerateDropSequenceSQL(const SequenceName: string): string;
  end;

implementation

function TOracleHelpers.ValueToSQL(const Field: TField): string;
  function BytesToHex(const Bytes: TBytes): string;
  var
    i: Integer;
  begin
    Result := '';
    for i := Low(Bytes) to High(Bytes) do
      Result := Result + IntToHex(Bytes[i], 2);
  end;
begin
  if Field.IsNull then
    Exit('NULL');

  case Field.DataType of
    ftString, ftWideString, ftMemo, ftWideMemo, ftFmtMemo:
      Result := QuotedStr(StringReplace(Field.AsString, '''', '''''', [rfReplaceAll]));
    ftDate:
      Result := 'TO_DATE(' + QuotedStr(FormatDateTime('yyyy-mm-dd', Field.AsDateTime)) + 
                ', ''YYYY-MM-DD'')';
    ftTime:
      Result := 'TO_DATE(' + QuotedStr(FormatDateTime('hh:nn:ss', Field.AsDateTime)) + 
                ', ''HH24:MI:SS'')';
    ftDateTime, ftTimeStamp:
      Result := 'TO_TIMESTAMP(' + QuotedStr(FormatDateTime('yyyy-mm-dd hh:nn:ss', Field.AsDateTime)) + 
                ', ''YYYY-MM-DD HH24:MI:SS'')';
    ftBoolean:
      if Field.AsBoolean then Result := '1' else Result := '0';
    ftBlob, ftGraphic, ftVarBytes, ftBytes:
      Result := 'HEXTORAW(''' + BytesToHex(Field.AsBytes) + ''')';
    else
      Result := Field.AsString;
  end;
end;

function TOracleHelpers.GenerateInsertSQL(const TableName: string;
  Fields, Values: TStringList): string;
var
  i: Integer;
  FieldList, ValueList: string;
begin
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

function TOracleHelpers.GenerateUpdateSQL(const TableName: string;
  const SetClause, WhereClause: string): string;
begin
  Result := 'UPDATE ' + QuoteIdentifier(TableName) + ' SET ' + SetClause +
            ' WHERE ' + WhereClause + ';';
end;

function TOracleHelpers.TriggersAreEqual(const Trg1,
                                                   Trg2: TTriggerInfo): Boolean;
begin
  Result := SameText(Trg1.TriggerName, Trg2.TriggerName) and
            (Trg1.EventManipulation = Trg2.EventManipulation) and
            (Trg1.ActionTiming = Trg2.ActionTiming) and
            (Trim(Trg1.ActionStatement) = Trim(Trg2.ActionStatement));
end;

function TOracleHelpers.QuoteIdentifier(const Identifier: string): string;
begin
  // Oracle usa comillas dobles para case-sensitive
  // Sin comillas convierte a UPPERCASE automáticamente
  Result := '"' + Identifier + '"';
end;

function TOracleHelpers.NormalizeType(const AType: string): string;
var
  S: string;
begin
  S := UpperCase(Trim(AType));
  S := StringReplace(S, ' ', '', [rfReplaceAll]);
  
  // Normalizar aliases de Oracle
  if StartsText('NUMBER(', S) then
  begin
    // NUMBER sin escala es equivalente a NUMBER(38)
    // Mantener como está
  end
  else if S = 'NUMBER' then
    S := 'NUMBER'
  else if S = 'INTEGER' then
    S := 'NUMBER(38)'
  else if S = 'INT' then
    S := 'NUMBER(38)'
  else if StartsText('VARCHAR2', S) then
    S := StringReplace(S, 'VARCHAR2', 'VARCHAR2', [])
  else if StartsText('NVARCHAR2', S) then
    S := StringReplace(S, 'NVARCHAR2', 'NVARCHAR2', []);
  
  Result := S;
end;

function TOracleHelpers.GenerateAddColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' ADD ' + GenerateColumnDefinition(ColumnInfo) + ';';
end;

function TOracleHelpers.GenerateColumnDefinition(const Col: TColumnInfo): string;
var
  DefVal: string;
begin
  Result := QuoteIdentifier(Col.ColumnName) + ' ' + Col.DataType;
  
  // Default value (debe ir antes de NULL/NOT NULL en Oracle)
  if (not SameText(Col.ColumnDefault, '')) and
     (not SameText(Col.ColumnDefault, 'NULL')) then
  begin
    DefVal := Col.ColumnDefault;
    
    // Funciones comunes de Oracle
    if (Pos('SYSDATE', UpperCase(DefVal)) > 0) or
       (Pos('SYSTIMESTAMP', UpperCase(DefVal)) > 0) or
       (Pos('CURRENT_TIMESTAMP', UpperCase(DefVal)) > 0) or
       (Pos('SYS_GUID()', UpperCase(DefVal)) > 0) or
       (Pos('.NEXTVAL', UpperCase(DefVal)) > 0) then
    begin
      Result := Result + ' DEFAULT ' + DefVal;
    end
    else
    begin
      // Eliminar comillas si ya las tiene
      if (Length(DefVal) >= 2) and (DefVal[1] = '''') and
         (DefVal[Length(DefVal)] = '''') then
        DefVal := Copy(DefVal, 2, Length(DefVal) - 2);
      Result := Result + ' DEFAULT ' + QuotedStr(DefVal);
    end;
  end;
  
  // Nullability
  if SameText(Col.IsNullable, 'NO') then
    Result := Result + ' NOT NULL';
  // Oracle no requiere NULL explícito, es el default
  
  // Oracle no tiene AUTO_INCREMENT, se usa SEQUENCE + TRIGGER
  // o IDENTITY columns (Oracle 12c+)
end;

function TOracleHelpers.GenerateCreateProcedureSQL(const Body: string): string;
begin
  // Oracle puede usar el cuerpo directamente
  // El terminador "/" se agrega al final
  Result := Body + sLineBreak + '/';
end;

function TOracleHelpers.GenerateCreateFunctionSQL(const Body: string): string;
begin
  // Oracle usa "/" como terminador para bloques PL/SQL
  Result := Body + sLineBreak + '/';
end;

function TOracleHelpers.GenerateCreateTableSQL(const Table: TTableInfo;
  const Indexes: TArray<TIndexInfo>): string;
var
  i: Integer;
  PKList: TStringList;
  ColDef: string;
begin
  Result := 'CREATE TABLE ' + QuoteIdentifier(Table.TableName) + ' (' + sLineBreak;
  PKList := TStringList.Create;
  try
    for i := 0 to Table.Columns.Count - 1 do
    begin
      ColDef := '  ' + GenerateColumnDefinition(Table.Columns[i]);
      // Detectar Primary Key
      if SameText(Table.Columns[i].ColumnKey, 'PRI') then
        PKList.Add(QuoteIdentifier(Table.Columns[i].ColumnName));
      if (i < Table.Columns.Count - 1) or (PKList.Count > 0) then
        ColDef := ColDef + ',';
      Result := Result + ColDef + sLineBreak;
    end;
    // Agregar Primary Key constraint
    if PKList.Count > 0 then
    begin
      Result := Result + '  CONSTRAINT ' + QuoteIdentifier('PK_' + Table.TableName) + 
                ' PRIMARY KEY (' + PKList.CommaText + ')' + sLineBreak;
    end;
    Result := Result + ');';
  finally
    PKList.Free;
  end;
end;

function TOracleHelpers.GenerateDeleteSQL(const TableName,
  WhereClause: string): string;
begin
  Result := 'DELETE FROM ' + QuoteIdentifier(TableName) +
            ' WHERE ' + WhereClause + ';';
end;

function TOracleHelpers.GenerateDropColumnSQL(const TableName,
  ColumnName: string): string;
begin
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' DROP COLUMN ' + QuoteIdentifier(ColumnName) + ';';
end;

function TOracleHelpers.GenerateDropFunction(const FuncName: string): string;
begin
  Result := 'DROP FUNCTION ' + QuoteIdentifier(FuncName) + ';';
end;

function TOracleHelpers.GenerateDropTableSQL(const TableName:String): string;
begin
  Result := 'DROP TABLE ' + QuoteIdentifier(TableName) + ' CASCADE CONSTRAINTS;';
end;

function TOracleHelpers.GenerateDropTrigger(const Trigger: string): string;
begin
  Result := 'DROP TRIGGER ' + QuoteIdentifier(Trigger) + ';';
end;

function TOracleHelpers.GenerateDropView(const View: string): string;
begin
  Result := 'DROP VIEW ' + QuoteIdentifier(View) + ';';
end;

function TOracleHelpers.GenerateDropIndexSQL(const TableName,
  IndexName: string): string;
begin
  // Oracle no necesita especificar la tabla para DROP INDEX
  // Constraints PRIMARY KEY se eliminan con ALTER TABLE
  if StartsText('PK_', UpperCase(IndexName)) then
    Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
              ' DROP CONSTRAINT ' + QuoteIdentifier(IndexName) + ';'
  else
    Result := 'DROP INDEX ' + QuoteIdentifier(IndexName) + ';';
end;

function TOracleHelpers.GenerateDropProcedure(const Proc: string): string;
begin
  Result := 'DROP PROCEDURE ' + QuoteIdentifier(Proc) + ';';
end;

function TOracleHelpers.GenerateIndexDefinition(const TableName: string;
                                                     const Idx: TIndexInfo): string;
var
  i: Integer;
  ColNames: string;
begin
  ColNames := '';
  for i := 0 to High(Idx.Columns) do
  begin
    if i > 0 then
      ColNames := ColNames + ', ';
    ColNames := ColNames + QuoteIdentifier(Idx.Columns[i].ColumnName);
  end;
  
  if Idx.IsPrimary then
    Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
              ' ADD CONSTRAINT ' + QuoteIdentifier('PK_' + TableName) +
              ' PRIMARY KEY (' + ColNames + ');'
  else if Idx.IsUnique then
    Result := 'CREATE UNIQUE INDEX ' + QuoteIdentifier(Idx.IndexName) +
              ' ON ' + QuoteIdentifier(TableName) +
              ' (' + ColNames + ');'
  else
    Result := 'CREATE INDEX ' + QuoteIdentifier(Idx.IndexName) +
              ' ON ' + QuoteIdentifier(TableName) +
              ' (' + ColNames + ');';
end;

function TOracleHelpers.GenerateModifyColumnSQL(const TableName: string;
  const ColumnInfo: TColumnInfo): string;
begin
  // Oracle usa ALTER TABLE MODIFY para cambiar columnas
  Result := 'ALTER TABLE ' + QuoteIdentifier(TableName) +
            ' MODIFY ' + GenerateColumnDefinition(ColumnInfo) + ';';
end;

function TOracleHelpers.GenerateCreateSequenceSQL(const SequenceName: string): string;
begin
  Result := 'CREATE SEQUENCE ' + QuoteIdentifier(SequenceName) + 
            ' START WITH 1 INCREMENT BY 1;';
end;

function TOracleHelpers.GenerateDropSequenceSQL(const SequenceName: string): string;
begin
  Result := 'DROP SEQUENCE ' + QuoteIdentifier(SequenceName) + ';';
end;

end.