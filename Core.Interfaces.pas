unit Core.Interfaces;

interface

uses System.Classes, Core.Types, Data.DB;

type
  // Contrato para leer la estructura de la BD
  IDBMetadataProvider = interface
    ['{026B999C-B5BD-4C33-A080-9675C56A0738}']
    function GetTables: TStringList;
    function GetTableStructure(const TableName: string): TTableInfo;
    function GetTableIndexes(const TableName: string): TArray<TIndexInfo>;
    function GetTriggers: TArray<TTriggerInfo>;
    function GetTriggerDefinition(const TriggerName: string): string;
    function GetViews:TStringList;
    function GetData(const TableName: string; const Filter: string = ''): TDataSet;
    function GetViewDefinition(const ViewName:string):string;
    function GetProcedures:TStringList;
    function GetProcedureDefinition(const ProcedureName:string):string;
    function GetFunctions:TStringList;
    function GetFunctionDefinition(const FunctionName:string):string;
    function GetSequences: TStringList;
  end;

  // Capacidades opcionales para motores que publican restricciones CHECK.
  ICheckConstraintMetadataProvider = interface
    ['{EC1B436C-152B-4AAE-BC14-5162A3B1838A}']
    function GetTableCheckConstraints(
      const TableName: string): TArray<TCheckConstraintInfo>;
  end;

  ICheckConstraintHelpers = interface
    ['{1120FDF1-A154-40AC-AC4C-31CD6A30C2FA}']
    function CheckConstraintsAreEqual(const Check1,
      Check2: TCheckConstraintInfo): Boolean;
    function GenerateAddCheckConstraintSQL(const TableName: string;
      const CheckConstraint: TCheckConstraintInfo): string;
    function GenerateDropCheckConstraintSQL(const TableName,
      ConstraintName: string): string;
  end;

  // Contrato para escribir el script
  IScriptWriter = interface
    ['{638AC4C1-4AF7-48CF-ACD9-602E3BAC1228}']
    procedure AddComment(const Text: string);
    procedure AddCommand(const SQL: string);
    function GetScript: string;
  end;

  IDBHelpers = interface
    ['{1DCC0AC2-C36B-461F-9DFA-D63CD007C660}']
    // Comparación (lógica mayormente universal)
    function ColumnsAreEqual(const Col1, Col2: TColumnInfo): Boolean;
    function IndexesAreEqual(const Idx1, Idx2: TIndexInfo): Boolean;
    // Generación SQL (específico de cada motor)
    function TriggersAreEqual(const Trg1, Trg2: TTriggerInfo): Boolean;
    function QuoteIdentifier(const Identifier: string): string;
    function GenerateColumnDefinition(const Col: TColumnInfo): string;
    function GenerateIndexDefinition(const TableName: string;
                                     const Idx: TIndexInfo): string;
    function GenerateCreateTableSQL(const Table: TTableInfo;
                                    const Indexes: TArray<TIndexInfo>): string;
    function GenerateAddColumnSQL(const TableName:string;
                                  const ColumnInfo:TColumnInfo): string;
    function GenerateDropColumnSQL(const TableName, ColumnName:string): string;
    function GenerateModifyColumnSQL(const TableName:string;
                                     const ColumnInfo:TColumnInfo): string;
    function GenerateDropIndexSQL(const TableName, IndexName:string): string;
    function GenerateDropTrigger(const Trigger:string):string;
    function GenerateDropProcedure(const Proc:string):string;
    function GenerateDropFunction(const FuncName: string): string;
    function GenerateDropView(const View:string):string;
    function GenerateDropTableSQL(const TableName:String): string;
    function GenerateInsertSQL(const TableName: string;
                           Fields, Values: TStringList;
                           const HasIdentity: Boolean = False): string;
    function GenerateUpdateSQL(const TableName: string;
                               const SetClause, WhereClause: string): string;
    function GenerateDeleteSQL(const TableName, WhereClause: string): string;
    function ValueToSQL(const Field: TField): string;
    // Normalización (específico de cada motor)
    function NormalizeType(const AType: string): string;
    function NormalizeExtra(const AExtra: string): string;
    function GenerateCreateProcedureSQL(const Body: string): string;
    function GenerateCreateFunctionSQL(const Body: string): string;
    function GenerateCreateSequence(const SeqName: string): string;
    function GenerateDropSequence(const SeqName: string): string;   
  end;

implementation

end.
