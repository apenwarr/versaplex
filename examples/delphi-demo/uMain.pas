unit uMain;

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,
  Dialogs, DB, ADODB, uDemoSchema, uPwData, Grids, DBGrids, StdCtrls;

type
  TDemoMain = class(TForm)
    ADOConnection1: TADOConnection;
    DataSource1: TDataSource;
    DBGrid1: TDBGrid;
    bRefresh: TButton;
    ADOQuery1: TADOQuery;
    bTestAdd: TButton;
    procedure FormCreate(Sender: TObject);
    procedure bRefreshClick(Sender: TObject);
    procedure bTestAddClick(Sender: TObject);
  private
    { Private declarations }
    schema: TDemoSchema;
  public
    { Public declarations }
  end;

var
  DemoMain: TDemoMain;

implementation

{$R *.dfm}

procedure TDemoMain.bRefreshClick(Sender: TObject);
begin
    schema.DemoList.Select(ADOQuery1);
end;

procedure TDemoMain.bTestAddClick(Sender: TObject);
begin
    schema.DemoAdd.setLastName('New last').setFirstName('New first')
            .setPuppies(12).Exec;
    schema.DemoAdd('Newer last', 'Newer first', 13).Exec;
end;

procedure TDemoMain.FormCreate(Sender: TObject);
begin
    schema := TDemoSchema.Create(TPwDatabase(ADOConnection1));
end;

end.
