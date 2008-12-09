program uDemoApp;

uses
  Forms,
  uMain in 'uMain.pas' {DemoMain},
  uDemoSchema in 'uDemoSchema.pas',
  uPwData in 'uPwData.pas';

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := true;

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TDemoMain, DemoMain);
  Application.Run;
end.
