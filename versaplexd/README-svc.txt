
versaplexd-svc.exe is used for running versaplexd as a Windows service
instead of as a normal Unix daemon.

For now, it always listens on tcp port 5561 and looks for versaplexd.ini
(and all its required DLLs) in the same directory as versaplexd-svc.exe.

You need to edit versaplexd.ini to select which database to connect to.

To install the service:

	versaplexd-svc -i
	
To uninstall the service:

	versaplexd-svc -u
	
When the service is installed, it should show up in the Windows Services
control panel, where you can start and stop it as necessary.
