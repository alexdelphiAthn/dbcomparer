unit Providers.MySQL;

interface

uses
  System.Classes, Data.DB, Uni, Core.Interfaces, Core.Types, Uni, 
  MySQLUniProvider;

type
  TMySQLMetadataProvider = class(TInterfacedObject, IDBMetadataProvider)
  private
    FConn: TUniConnection;
    FDBName: string;
  public
    constructor Create(Conn: TUniConnection; const DBName: string);
    // Implementación de la interfaz
    function GetTables: TStringList;
    function GetTableStructure(const TableName: string): TTableInfo;
    // ... resto de métodos
  end;

implementation

