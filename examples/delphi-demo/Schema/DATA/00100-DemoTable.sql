DELETE FROM [DemoTable];
SET IDENTITY_INSERT [DemoTable] ON;
INSERT INTO DemoTable ([Id],[LastName],[FirstName],[Puppies],[LastUpdated]) VALUES (1,'Pennarun','Avery',13,'2008-11-12 21:52:41.280');
INSERT INTO DemoTable ([Id],[LastName],[FirstName],[Puppies],[LastUpdated]) VALUES (2,'Smith','Bob',99,'2008-11-12 21:52:53.090');
INSERT INTO DemoTable ([Id],[LastName],[FirstName],[Puppies],[LastUpdated]) VALUES (3,'Looooooooooooooooooongname','Bill "The Fish"',122,'2008-11-12 21:53:35.673');
SET IDENTITY_INSERT [DemoTable] OFF;
