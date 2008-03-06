#!/usr/bin/python

#------------------------------------------------------------------------------
#                                  Veranda 
#                              ~--------------~
#
# Original Author: Andrei "Garoth" Thorp <garoth@gmail.com>
#
# Description: This program provides a graphical frontend to using the
#              Versaplex software. The user may provide SQL commands in the
#              editor, and this program will forward those commands over DBus
#              to Versaplexd. Versaplexd will then apply the commands to 
#              whatever database it is configured to use under the surface.
#              As such, this program eventually should be capable of providing
#              a graphical interface to many databases, using Versaplex for
#              abstraction.
#
# Notes:
#   Indentation: I use tabs only, 4 spaces per tab.
#
# Todo:
#   Add version numbers for imports
#------------------------------------------------------------------------------
import sys
import pygtk
import gtk
import gtk.glade
import gtksourceview2 as gtksourceview
import dbus
import re
import pango
from Parser import Parser
from Resulter import Resulter
from Searcher import Searcher
#------------------------------------------------------------------------------
class MainUI:
#------------------------------------------------------------------------------
	""" Generates the GUI for Veranda"""
	#------------------	
	def __init__(self):
	#------------------
		"""Initialize the program & ui"""
		# Define some instance variables
		self.name = "Veranda" 			# App name
		self.version = "0.1.0" 			# Version
		self.newNumbers = [] 			# for naming new tabs
		self.database = DBusSql() 		# SQL Access driver
		self.bottomState = False 		# False: notebookBottom closed
		self.getObject = "get object" 	# Versaplex command for get object
		self.listAll = "list all" 		# Versaplex command for list all
		self.searcher = "" 				# Becomes a searcher object later
		self.exposeEventID = 0 			# Used to disconnect a signal
		self.bindings = gtk.AccelGroup()# Keyboard bindings group

		# Import Glade's XML and load it
		self.gladeTree = gtk.glade.XML("ui.glade")
		dic = {"on_exit":(gtk.mainquit)}
		self.gladeTree.signal_autoconnect(dic)

		# Grab some of the widgets for easy access
		self.sidebar = ""
		self.resulter = Resulter()
		self.window = self.gladeTree.get_widget("window")
		self.vboxMain = self.gladeTree.get_widget("vbox-main")
		self.vpanedEditor = self.gladeTree.get_widget("vpaned-editor")
		self.vpanedPanel = self.gladeTree.get_widget("vpaned-panel")
		self.notebookTop = self.gladeTree.get_widget("notebook-top")
		self.notebookBottom = self.gladeTree.get_widget("notebook-bottom")
		self.buttonRun = self.gladeTree.get_widget("button-run")
		self.buttonNewTab = self.gladeTree.get_widget("button-newtab")
		self.buttonClose = self.gladeTree.get_widget("button-closetab")
		self.buttonNext = self.gladeTree.get_widget("button-nexttab")
		self.buttonPrevious = self.gladeTree.get_widget("button-lasttab")
		self.entrySearch = self.gladeTree.get_widget("entry-search")
		self.statusbar = self.gladeTree.get_widget("statusbar")
		                 # Statusbar context ids: * "sidebar"
						 #                        * "run query"
						 #                        * "error"
						 #                        * "success"

		# Misc Initializations
		hbox = gtk.HBox()
		hbox.show()
		runImage = gtk.Image()
		runImage.set_from_file("run.svg")
		runImage.show()
		hbox.pack_start(runImage)
		label = gtk.Label("  Run")
		label.show()
		hbox.pack_start(label)
		self.buttonRun.add(hbox)

		hbox = gtk.HBox()
		hbox.show()
		newTabImage = gtk.Image()
		newTabImage.set_from_file("new.svg")
		newTabImage.show()
		hbox.pack_start(newTabImage)
		label = gtk.Label("  New Tab")
		label.show()
		hbox.pack_start(label)
		self.buttonNewTab.add(hbox)

		hbox = gtk.HBox()
		hbox.show()
		newTabImage = gtk.Image()
		newTabImage.set_from_file("close.svg")
		newTabImage.show()
		hbox.pack_start(newTabImage)
		label = gtk.Label("  Close Current Tab")
		label.show()
		hbox.pack_start(label)
		self.buttonClose.add(hbox)

		hbox = gtk.HBox()
		hbox.show()
		newTabImage = gtk.Image()
		newTabImage.set_from_file("next.svg")
		newTabImage.show()
		hbox.pack_start(newTabImage)
		label = gtk.Label("  Next Tab")
		label.show()
		hbox.pack_start(label)
		self.buttonNext.add(hbox)

		hbox = gtk.HBox()
		hbox.show()
		newTabImage = gtk.Image()
		newTabImage.set_from_file("previous.svg")
		newTabImage.show()
		hbox.pack_start(newTabImage)
		label = gtk.Label("  Previous Tab")
		label.show()
		hbox.pack_start(label)
		self.buttonPrevious.add(hbox)

		# Open a first tab (comes with configured editor)
		self.newTab()

		# Connect events & key strokes
		self.window.connect("delete_event", gtk.main_quit)
		self.buttonRun.connect("clicked", self.runQuery)
		self.buttonNewTab.connect("clicked", self.newTab)
		self.buttonClose.connect("clicked", self.closeCurrentTab)
		self.buttonNext.connect("clicked", self.nextTab)
		self.buttonPrevious.connect("clicked", self.lastTab)
		self.entrySearch.connect("key-release-event", self.search)
		self.exposeEventID = self.window.connect("expose-event", 
												self.postStartInit)

		self.window.add_accel_group(self.bindings)
		self.buttonRun.add_accelerator("clicked", self.bindings,
							ord("r"), gtk.gdk.CONTROL_MASK, gtk.ACCEL_VISIBLE)
		self.buttonNewTab.add_accelerator("clicked", self.bindings,
							ord("t"), gtk.gdk.CONTROL_MASK, gtk.ACCEL_VISIBLE)
		self.buttonNext.add_accelerator("clicked", self.bindings,
							ord("n"), gtk.gdk.CONTROL_MASK, gtk.ACCEL_VISIBLE)
		self.buttonPrevious.add_accelerator("clicked", self.bindings,
							ord("p"), gtk.gdk.CONTROL_MASK, gtk.ACCEL_VISIBLE)
		self.buttonClose.add_accelerator("clicked", self.bindings,
							ord("w"), gtk.gdk.CONTROL_MASK, gtk.ACCEL_VISIBLE)

		# Show things
		self.window.show()

	#---------------------
	def initSidebar(self):
	#---------------------
		""" Initializes the sidebar with the tables list and configures it"""
		toList = ["table", "view", "procedure",
				"trigger", "scalarfunction", "tablefunction"]

		statusID = self.statusbar.get_context_id("sidebar")
		self.statusbar.push(statusID, "Initializing Sidebar")

		scrolls = gtk.ScrolledWindow(gtk.Adjustment(), gtk.Adjustment())
		scrolls.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_AUTOMATIC)

		treestore = gtk.TreeStore(str)
		self.sidebar = gtk.TreeView(treestore)
		cell = gtk.CellRendererText()
		column = gtk.TreeViewColumn("Database Objects", cell, text=0)
		self.sidebar.append_column(column)

		masterTable = []

		for item in toList:
			result = self.database.query(self.listAll+" "+item)
			if "Error" not in result:
				parser = Parser(result)
				table = parser.getTable()[:] #the [:] makes a clone
				table.insert(0, [item])
				masterTable.append(table)
				rows = parser.getTableIterator()
				iter = treestore.append(None, [item.title()])
				while rows.hasNext():
					treestore.append(iter, [str(rows.getNext()[0])])
			else:
				statusID = self.statusbar.get_context_id("error")
				self.statusbar.push(statusID, result)

		self.searcher = Searcher(masterTable)

		self.sidebar.connect("row-activated", self.rowClicked, masterTable)

		scrolls.add(self.sidebar)
		self.vpanedPanel.add(scrolls)
		scrolls.show()
		self.sidebar.show()

		self.statusbar.push(statusID, "Sidebar Loaded")

	#-----------------------	
	def getNewNumber(self):	
	#-----------------------
		""" Get a unique number to number a tab """
		x = 0
		while (True):
			if x in self.newNumbers:
				x = x+1
			else: 
				self.newNumbers.append(x)
				return r"   "+str(x)+r"   "
	
	#----------------------------------------
	def removeNumber(self, editor, notebook):
	#----------------------------------------
		""" If a given page has a label with an automatic
		number, remove that number from the list of numbers so that
		it can be reassigned to a new fresh tab in the future"""
		label = self.getLabelText(editor, notebook)
		label = label.split(" ")
		self.newNumbers.remove(int(label[0]))
	
	#---------------------------------------------
	def configureEditor(self, editor, textbuffer):
	#---------------------------------------------
		"""Sets up a gtksourceview with the common options I want."""
		languagemanager = gtksourceview.LanguageManager()
		textbuffer.set_language(languagemanager.get_language("sql"))
		textbuffer.set_highlight_syntax(True)
		editor.set_show_line_numbers(True)
		editor.set_wrap_mode(gtk.WRAP_WORD_CHAR)
		editor.modify_font(pango.FontDescription("monospace 10"))

	#---------------------------------------------
	def makeBottomTabMenu(self, label, resulter):
	#---------------------------------------------
		"""Returns an hbox with the title, change button, and close button
		to be put in a tab"""
		hbox = gtk.HBox()
		label = gtk.Label(r"   "+str(label)+r"   ")
		hbox.pack_start(label)
		
		changeIcon = gtk.Image()
		changeIcon.set_from_file("cycle.svg")
		buttonMode = gtk.Button(None)
		buttonMode.add(changeIcon)
		hbox.pack_start(buttonMode, False, False, 1)

		closeIcon = gtk.Image()
		closeIcon.set_from_file("close.svg")
		buttonClose = gtk.Button(None)
		buttonClose.add(closeIcon)
		hbox.pack_start(buttonClose, False, False, 1) 

		buttonClose.connect("clicked", self.closeTab, resulter)
		buttonMode.connect("clicked", self.changeMode, resulter)

		changeIcon.show()
		closeIcon.show()
		buttonMode.show()
		label.show()
		buttonClose.show()
		hbox.show()

		return hbox

	#---------------------------------------
	def showOutput(self, topEditor, result):
	#---------------------------------------
		parser = Parser(result)
		
		if self.bottomState == False:
			self.resulter.update(parser)
			self.notebookBottom.show()
			hbox = self.makeBottomTabMenu("Results", self.resulter)
			self.newTabBottom(self.resulter.getCurrentView(), hbox)
			self.bottomState = True

		else :
			index = self.notebookBottom.page_num(self.resulter.getCurrentView())
			hbox = self.notebookBottom.get_tab_label(
											self.resulter.getCurrentView())
			self.resulter.update(parser)
			self.notebookBottom.remove_page(index)
			self.notebookBottom.insert_page(self.resulter.getCurrentView(),
											hbox, index)
			self.notebookBottom.set_tab_reorderable(
										self.resulter.getCurrentView(), True)
			self.notebookBottom.set_current_page(index)
			
	#------------------------------------
	def newTabBottom(self, widget, hbox):
	#------------------------------------
		"""Creates a new tab on the bottom notebook, with "widget" in the tab
		and "hbox" as the label (not actually a gtk label)"""
		self.notebookBottom.append_page(widget, hbox)
	
	#----------------------------------------
	def getLabelText(self, editor, notebook):
	#----------------------------------------
		"""Retrieves the label number from notebook with a page which contains 
		the given editor"""
		hbox = notebook.get_tab_label(editor)
		children = hbox.get_children()
		labelText = children[0].get_text()
		labelText = labelText.strip(' ')
		return str(labelText)

	#---------------------------------
	def expandSidebar(self, sidebarList):
	#---------------------------------
		"""Will expand some of the sidebar elements to make better use
		of space"""
		expandMax = 18
		usedSoFar = 0
		for section in sidebarList:
			if len(section) + usedSoFar > expandMax:
				break
			else:
				usedSoFar += len(section)
				self.sidebar.expand_to_path((sidebarList.index(section),1))

	#------------------------------------
	def updateSidebar(self, sidebarList):
	#------------------------------------
		"""Given a new list, this will change the contents of the sidebar"""
		treestore = gtk.TreeStore(str)

		for section in sidebarList:		
			iter = treestore.append(None,[section[0][0]])
			for element in section[1:]:
				treestore.append(iter,[element[0]])

		self.sidebar.set_model(treestore)

		self.expandSidebar(sidebarList)

	#----------------------#
	#-- CALLBACK METHODS --#
	#----------------------#

	#------------------------------------------
	def postStartInit(self, widget, data=None):
	#------------------------------------------
		""" Initializes all the stuff that should only happen after the window
		is already on screen"""
		self.initSidebar()
		widget.disconnect(self.exposeEventID)
	
	#-------------------------------------
	def runQuery(self, widget, data=None):
	#-------------------------------------
		"""Uses the database abstraction (initially Dbus)
		To send the query that is in the current window"""
		scrolls = self.notebookTop.get_nth_page(self.notebookTop.
												get_current_page()) 
		if scrolls != None:
			editor = scrolls.get_children()[0]
			buffer = editor.get_buffer()
			#get all text, not including hidden chars
			query = buffer.get_text(buffer.get_start_iter(),
									buffer.get_end_iter(), False)

			contextID = self.statusbar.get_context_id("run query")
			self.statusbar.push(contextID, "Running query: "+query)

			result = self.database.query(query)
			if "Error" not in result:
				self.showOutput(editor, result)
				statusID = self.statusbar.get_context_id("success")
				self.statusbar.push(statusID, "Success.")
			else:
				statusID = self.statusbar.get_context_id("error")
				self.statusbar.push(statusID, result)
		else:
			contextID = self.statusbar.get_context_id("error")
			self.statusbar.push(contextID, "No query to run.")

	#-----------------------------------
	def search(self, widget, data=None):
	#-----------------------------------
		"""Incremental search callback. As the user types, this method
		notices and modifies the sidebar"""
		text = widget.get_text()
		sidebarList = self.searcher.find(text)
		self.updateSidebar(sidebarList)
	
	#--------------------------------------	
	def newTab(self, widget=None, data=None):
	#--------------------------------------
		"""Open a new editor tab (top). Data is an optional title for the tab."""

		scrolls = gtk.ScrolledWindow(gtk.Adjustment(), gtk.Adjustment())
		scrolls.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_AUTOMATIC)
		
		textbuffer = gtksourceview.Buffer()
		editor = gtksourceview.View(textbuffer)

		self.configureEditor(editor, textbuffer)

		hbox = gtk.HBox()
		if data == None:
			label = gtk.Label(self.getNewNumber())
		else:
			label = gtk.Label(self.getNewNumber()+str(data)+"  ")
		hbox.pack_start(label)

		closeIcon = gtk.Image()
		closeIcon.set_from_file("close.svg")
		buttonClose = gtk.Button(None)
		buttonClose.add(closeIcon)
		hbox.pack_start(buttonClose, False, False, 1) 

		buttonClose.connect("clicked", self.closeTab, scrolls)

		scrolls.add(editor)
		self.notebookTop.append_page(scrolls, hbox)
		self.notebookTop.set_tab_reorderable(scrolls, True) 

		scrolls.show()
		closeIcon.show()
		label.show()
		buttonClose.show()
		hbox.show()
		editor.show()

		# KEEP THIS LINE AT THE END OR ELSE! (hours of frustration...)
		self.notebookTop.set_current_page(-1)

		return editor

	#--------------------------------------------
	def closeTab(self, sourceWidget, targetWidget):
	#--------------------------------------------
		"""Close a tab. targetWidget points to the contents of the notebook
		tab that you want closed."""
		
		index = -1
		try:
			index = self.notebookTop.page_num(targetWidget)
		except TypeError:
			pass
		if index != -1:
			self.removeNumber(targetWidget, self.notebookTop)
			self.notebookTop.remove_page(index)
			return

		index = self.notebookBottom.page_num(targetWidget.getCurrentView())
		if index != -1:
			self.notebookBottom.remove_page(index)
			self.bottomState = False
			self.notebookBottom.queue_resize()
			self.notebookTop.queue_resize()
			return

		if index == -1:
			print "Worse Than Failure: Lost The Tab!"

	#--------------------------------------------
	def closeCurrentTab(self, widget, data=None):
	#--------------------------------------------
		"""Closes the current tab in the top editor section"""
		index = self.notebookTop.get_current_page()
		self.notebookTop.remove_page(index)

	#------------------------------------
	def nextTab(self, widget, data=None):
	#------------------------------------
		"""Changes to the previous tab"""
		index = self.notebookTop.get_current_page()
		self.notebookTop.set_current_page((index+1) % \
				                          self.notebookTop.get_n_pages())
		
	#------------------------------------
	def lastTab(self, widget, data=None):
	#------------------------------------
		"""Changes to the next tab"""
		index = self.notebookTop.get_current_page()
		self.notebookTop.set_current_page(index-1)

	#--------------------------------------
	def changeMode(self, widget, resulter):
	#--------------------------------------
		"""After a change button is clicked, this makes the notebook tab
	osscroll through the different view modes in a fixed pattern"""
		pageIndex = self.notebookBottom.page_num(resulter.getCurrentView())
		hbox = self.notebookBottom.get_tab_label(resulter.getCurrentView())
		self.notebookBottom.remove_page(pageIndex)
		self.notebookBottom.insert_page(resulter.getNextView(), hbox, pageIndex)
		self.notebookBottom.set_tab_reorderable(resulter.getCurrentView(), True)

	#-------------------------------------------------------------
	def rowClicked(self, treeview, position, column, masterTable):
	#-------------------------------------------------------------
		""" 
		Given the position coordinates and the master table (a 
		list of all data that is in the sidebar), this method opens
		a new editor tab which has code in it. The code is the source
		code to the object that was double clicked on in the sidebar.
		If the item is a table, the code is just a select statement.
		"""
		try:
			type = masterTable[position[0]][0][0]
			name = masterTable[position[0]][position[1]+1][0]
		except IndexError:
			contextID = self.statusbar.get_context_id("error")
			self.statusbar.push(contextID, 
					"Can't do anything with a category title")

			print "Can't do anything when a category title is clicked"
			return

		if type == "table":
			query = "select top 100 * from [%s]" % name

			contextID = self.statusbar.get_context_id("run query")
			self.statusbar.push(contextID, "Running query: "+query)

			result = self.database.query(query)

			if "Error" not in result:
				editor = self.newTab(None, name)
				buffer = editor.get_buffer()
				buffer.set_text(query)

				self.showOutput(editor, result)
			else:
				statusID = self.statusbar.get_context_id("error")
				self.statusbar.push(statusID, result)

		else:
			query = self.getObject + " " + type + " " + name

			contextID = self.statusbar.get_context_id("run query")
			self.statusbar.push(contextID, "Running query: "+query)

			result = self.database.query(query)

			if "Error" in result:
				statusID = self.statusbar.get_context_id("error")
				self.statusbar.push(statusID, result)
				return

			parser = Parser(result)
			data = parser.getTable()
			commands = data[0][0]

			com2 = commands[:]
			pattern1 = re.compile(r"^create", re.I)
			commands = re.sub(pattern1, r"ALTER", commands, 1)
			if commands == com2:
				pattern2 = re.compile(r"\ncreate", re.I)
				commands = re.sub(pattern2, r"\nALTER", commands, 1)

			editor = self.newTab(None, name)
			buffer = editor.get_buffer()
			buffer.set_text(commands)
	
	#--------------------------#
	#-- END CALLBACK METHODS --#
	#--------------------------#

#------------------------------------------------------------------------------
class DBusSql:
#------------------------------------------------------------------------------
	""" Provides abstraction for connecting to an SQL database via 
	DBus and the Versaplex software """
	#TODO let dbus shut down nicely
	#------------------	
	def __init__(self):
	#------------------
		""" Connects to DBus and connects to versaplex"""
		print "~-------------------------------------------~"
		print "| Setting up DBus Connection                |"
		print "| If you're using a non-standard bus,       |"
		print "| export DBUS_SESSION_BUS_ADDRESS           |"
		print "| to listen to it. For testing with WvDBus, |"
		print "| it would be something like                |"
		print "| 'tcp:host=localhost,port=5432'            |"	
		print "~-------------------------------------------~"

		# FIXME Rewrite to use a config file instead of 
		# DBUS_SESSION_BUS_ADDRESS
		self.bus = dbus.SessionBus()
		# FIXME put most of this stuff in a config file
		self.versaplex = self.bus.get_object("com.versabanq.versaplex",
				                 "/com/versabanq/versaplex/db") 
		self.versaplexI = dbus.Interface(self.versaplex, dbus_interface=
				                 "com.versabanq.versaplex.db") 

	#----------------------
	def query(self, query):
	#----------------------
		""" Runs given query over dbus """

		print "Running Query:", query
		try:
			result = self.versaplexI.ExecRecordset(query)
		except dbus.exceptions.DBusException:
			# the string Error will be parsed to recognize the error.
			result = "Error: " + str(sys.exc_info()[1])
			print result

		print "Done."
		return result

mainUI=MainUI()
gtk.main()
