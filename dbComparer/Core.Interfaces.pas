unit Core.Interfaces;

interface

uses System.Classes, Core.Types;

type
  // Contrato para leer la estructura de la BD
  IDBMetadataProvider = interface
    ['{026B999C-B5BD-4C33-A080-9675C56A0738}'] // Presiona Ctrl+Shift+G en Delphi
    function GetTables: TStringList;
    function GetTableStructure(const TableName: string): TTableInfo;
    function GetTableIndexes(const TableName: string): TArray<TIndexInfo>;
    function GetTriggers: TArray<TTriggerInfo>;
    function GetTriggerDefinition(const TriggerName: string): string;
    function GetViews:TStringList;
    function GetViewDefinition(const ViewName:string):string;
    function GetProcedures:TStringList;
    function GetProcedureDefinition(const ProcedureName:string):string;
  end;

  // Contrato para escribir el script
  IScriptWriter = interface
    ['{638AC4C1-4AF7-48CF-ACD9-602E3BAC1228}']
    procedure AddComment(const Text: string);
    procedure AddCommand(const SQL: string);
    function GetScript: string;
  end;

implementation
end.