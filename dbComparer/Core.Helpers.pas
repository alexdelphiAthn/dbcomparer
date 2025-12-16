unit Core.Helpers;

interface

uses
  Core.Types, System.SysUtils, System.StrUtils;

type
  TDBHelpers = class
  public
    // Comparación
    class function ColumnsAreEqual(const Col1, Col2: TColumnInfo): Boolean;
    class function IndexesAreEqual(const Idx1, Idx2: TIndexInfo): Boolean;
    class function TriggersAreEqual(const Trg1, Trg2: TTriggerInfo): Boolean;

    // Generación SQL
    class function GenerateColumnDefinition(const Col: TColumnInfo): string;
    class function GenerateIndexDefinition(const TableName: string;
                                           const Idx: TIndexInfo): string;

    // Normalización
    class function NormalizeType(const AType: string): string;
    class function NormalizeExtra(const AExtra: string): string;
  end;

implementation

class function TDBHelpers.NormalizeType(const AType: string): string;
var
  S: string;
  PStart, PEnd: Integer;
begin
  S := LowerCase(Trim(AType));

  // Eliminar el tamaño de display de enteros (MySQL 8.0.17+)
  if StartsText('int', S) or StartsText('tinyint', S) or
     StartsText('smallint', S) or StartsText('mediumint', S) or
     StartsText('bigint', S) then
  begin
    PStart := Pos('(', S);
    PEnd := Pos(')', S);
    if (PStart > 0) and (PEnd > PStart) then
      S := Copy(S, 1, PStart - 1) + Copy(S, PEnd + 1, MaxInt);
  end;

  Result := StringReplace(S, ' ', '', [rfReplaceAll]);
end;

class function TDBHelpers.NormalizeExtra(const AExtra: string): string;
begin
  Result := LowerCase(Trim(AExtra));
end;

class function TDBHelpers.ColumnsAreEqual(const Col1, Col2: TColumnInfo): Boolean;
var
  Typ1, Typ2, Null1, Null2, Key1, Key2, Extra1, Extra2, Def1, Def2: string;
  IsAutoInc: Boolean;
begin
  Typ1 := NormalizeType(Col1.DataType);
  Typ2 := NormalizeType(Col2.DataType);
  Null1 := LowerCase(Trim(Col1.IsNullable));
  Null2 := LowerCase(Trim(Col2.IsNullable));
  Key1 := LowerCase(Trim(Col1.ColumnKey));
  Key2 := LowerCase(Trim(Col2.ColumnKey));
  Extra1 := NormalizeExtra(Col1.Extra);
  Extra2 := NormalizeExtra(Col2.Extra);
  Def1 := Trim(Col1.ColumnDefault);
  Def2 := Trim(Col2.ColumnDefault);

  Result := (Typ1 = Typ2) and (Null1 = Null2) and
            (Key1 = Key2) and (Extra1 = Extra2);

  if Result then
  begin
    IsAutoInc := (Pos('auto_increment', Extra1) > 0) or
                 (Pos('auto_increment', Extra2) > 0);
    if not IsAutoInc then
    begin
      if SameText(Def1, 'NULL') then Def1 := '';
      if SameText(Def2, 'NULL') then Def2 := '';
      if not SameText(Def1, Def2) then
        Result := False;
    end;
  end;

  if Result then
    Result := SameText(Col1.ColumnComment, Col2.ColumnComment);
end;

class function TDBHelpers.IndexesAreEqual(const Idx1, Idx2: TIndexInfo): Boolean;
var
  i: Integer;
begin
  Result := SameText(Idx1.IndexName, Idx2.IndexName) and
            (Idx1.IsUnique = Idx2.IsUnique) and
            (Idx1.IsPrimary = Idx2.IsPrimary) and
            (Length(Idx1.Columns) = Length(Idx2.Columns));

  if Result then
  begin
    for i := 0 to High(Idx1.Columns) do
    begin
      if not SameText(Idx1.Columns[i].ColumnName, Idx2.Columns[i].ColumnName) or
         (Idx1.Columns[i].SeqInIndex <> Idx2.Columns[i].SeqInIndex) then
      begin
        Result := False;
        Break;
      end;
    end;
  end;
end;

class function TDBHelpers.TriggersAreEqual(const Trg1, Trg2: TTriggerInfo): Boolean;
begin
  Result := SameText(Trg1.TriggerName, Trg2.TriggerName) and
            (Trg1.EventManipulation = Trg2.EventManipulation) and
            (Trg1.ActionTiming = Trg2.ActionTiming) and
            (Trim(Trg1.ActionStatement) = Trim(Trg2.ActionStatement));
end;

class function TDBHelpers.GenerateColumnDefinition(const Col: TColumnInfo): string;
var
  DefVal: string;
begin
  Result := '`' + Col.ColumnName + '` ' + Col.DataType;

  if SameText(Col.IsNullable, 'NO') then
    Result := Result + ' NOT NULL'
  else
    Result := Result + ' NULL';

  if (not SameText(Col.ColumnDefault, '')) and
     (not SameText(Col.ColumnDefault, 'NULL')) then
  begin
    if (Pos('CURRENT_TIMESTAMP', UpperCase(Col.ColumnDefault)) > 0) or
       (Pos('NOW()', UpperCase(Col.ColumnDefault)) > 0) then
    begin
      Result := Result + ' DEFAULT ' + Col.ColumnDefault;
    end
    else
    begin
      DefVal := Col.ColumnDefault;
      // Eliminar comillas duplicadas si ya las tiene
      if (Length(DefVal) >= 2) and (DefVal[1] = '''') and
         (DefVal[Length(DefVal)] = '''') then
        DefVal := Copy(DefVal, 2, Length(DefVal) - 2);
      Result := Result + ' DEFAULT ' + QuotedStr(DefVal);
    end;
  end
  else if Col.ColumnDefault = 'NULL' then
    Result := Result + ' DEFAULT NULL';

  if Pos('auto_increment', LowerCase(Col.Extra)) > 0 then
    Result := Result + ' AUTO_INCREMENT';
  if Pos('on update', LowerCase(Col.Extra)) > 0 then
    Result := Result + ' ON UPDATE CURRENT_TIMESTAMP';

  if not SameText(Col.ColumnComment, '') then
    Result := Result + ' COMMENT ' + QuotedStr(Col.ColumnComment);
end;

class function TDBHelpers.GenerateIndexDefinition(const TableName: string;
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
    ColNames := ColNames + '`' + Idx.Columns[i].ColumnName + '`';
  end;

  if Idx.IsPrimary then
    Result := 'ALTER TABLE `' + TableName + '` ADD PRIMARY KEY (' + ColNames + ')'
  else if Idx.IsUnique then
    Result := 'ALTER TABLE `' + TableName + '` ADD UNIQUE INDEX `' +
              Idx.IndexName + '` (' + ColNames + ')'
  else
    Result := 'ALTER TABLE `' + TableName + '` ADD INDEX `' +
              Idx.IndexName + '` (' + ColNames + ')';
end;

end.
