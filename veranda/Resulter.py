#!/usr/bin/python

#------------------------------------------------------------------------------
#                                  Veranda 
#                                 *Resulter
#                              ~--------------~
#
# Original Author: Andrei "Garoth" Thorp <garoth@gmail.com>
#
# Description: This module makes handling of my multi-widget result viewer
#              easier. By wrapping the various widgets together and having them 
#              keep information about themselves.
#
# Notes:
#   Indentation: I use tabs only, 4 spaces per tab.
#------------------------------------------------------------------------------
import sys
import pygtk
import gtk
import gtksourceview2 as gtksourceview
import time
import pango
#------------------------------------------------------------------------------
class Resulter:
#------------------------------------------------------------------------------
	#--------------------------
	def __init__(self, parser):
	#--------------------------
		"""Initializes the instance variables (and as such, the various 
		gtk widgets that would be used)"""
		# Given parser, set up with the dbus info
		self.parser = parser
		# Iterator for the data, able to go row by row
		self.iterator = self.parser.getTableIterator()
		self.titles = self.parser.getColumnTitles()
		self.message = parser.getOriginalMessage()
		self.dbusMessages = time.ctime(time.time())+"\n\n"+ str(self.message)
		self.dbusMessages = self.__formatDbusMessage__(self.dbusMessage, 100)

		# Set up these objects
		self.__initTableView__()
		self.__initDbusView__()
		self.__initTextView__()
		
		# Reorganize this if you want the program to change through the views
		# in a different order
		self.viewOrder = [self.getTableView(), 
						  self.getTextView(),
						  self.getDbusView()]
		self.currentView = self.viewOrder[0]

	#------------------
	def __init__(self):
	#------------------
		"""Initializes a blank resulter with no... results. This construct
		would be used with later calls to the update method."""
		self.dbusMessages = ""	

	#---------------------------	
	def __initTableView__(self):
	#---------------------------
		"""Generates a gtk.TreeView based table"""
		# FIXME: STUPID PANGO MARKDOWN WTH
		self.tableViewModel = gtk.ListStore(*self.parser.
												getColumnTypesAsString())

		while self.iterator.hasNext():
			item = self.iterator.getNext()
			self.tableViewModel.append(item)

		self.tableView = gtk.TreeView(self.tableViewModel)
		self.cellRenderer = gtk.CellRendererText()

		x = 0
		for title in self.titles:
			treeviewcolumn = gtk.TreeViewColumn(title, self.cellRenderer, text=x)
			treeviewcolumn.set_resizable(True)
			self.tableView.append_column(treeviewcolumn)
			x += 1

		self.tableView.show()

	#--------------------------
	def __initDbusView__(self):
	#--------------------------
		"""Generates a gtksourceview with the original dbus message inside""" 
		self.dbusBuffer = gtksourceview.Buffer()
		self.dbusView = gtksourceview.View(self.dbusBuffer)

		self.configureEditor(self.dbusView, self.dbusBuffer)
		self.dbusView.set_editable(False)

		self.dbusBuffer.set_text(self.dbusMessages)

		self.dbusView.show()

	#--------------------------
	def __initTextView__(self):
	#--------------------------
		"""Generates a textual table of the data"""
		# Well, not yet.
		self.textView = ""
		self.textBuffer = gtksourceview.Buffer()
		self.textView = gtksourceview.View(self.textBuffer)

		self.configureEditor(self.textView, self.textBuffer)
		self.textView.set_editable(False)
		self.textView.set_wrap_mode(gtk.WRAP_NONE)
		self.textView.modify_font(pango.FontDescription("monospace 10"))

		self.textBuffer.set_text(self.__formatTextTable__())

		self.textView.show()

	#-----------------------------
	def __formatTextTable__(self):
	#-----------------------------
		"""Formats the textual table to look nice"""
		output = "" 							# Final Output to send back
		numColumns = self.parser.numColumns() 	# Number of columns in table
		numRows = self.parser.numRows() 		# Number of rows in the table
		maxColWidth = 20 						# Max width of 1 column
		padding = " | "							# The space between columns
		table = self.parser.getTable() 			# the full table of values
		iterator = self.parser.getTableIterator() 	# iterator for the rows
		format = ""

		# 1) Get max widths of the columns
		widths = []
		for y in range(numColumns):
			max = len(self.titles[y])
			for x in range(numRows):
				if len(table[x][y]) > max:
					max = len(table[x][y])
					if max > maxColWidth:
						max = maxColWidth
			widths.append(max)

		# 2) Generate format string
		for width in widths:
			if width != widths[len(widths)-1]:
				format += r"%-"+str(width)+r"."+str(width)+r"s"+padding
			else:
				format += r"%-"+str(width)+r"."+str(width)+r"s"

		# 3) Print a table
		output += format % tuple(self.titles)+"\n" # not for Windows; os.linesep

		# 4) Print a divider
		totalWidth = 0
		for width in widths:totalWidth += width
		totalWidth += len(padding*(numColumns-1))
		output += "-"*totalWidth+"\n" 	#not suitable for Windows; os.linesep

		# 5) Print the body
		while iterator.hasNext():
			output += format % tuple(iterator.getNext()) + "\n"
		
		return output
	
	#--------------------------------------------
	def __formatDbusMessage__(self, text, width):
	#--------------------------------------------
		"""Slices up the Dbus Message so that hopefully gtksourceview doesn't"""
		return reduce(lambda line, word, width=width: '%s%s%s' %
				(line,
					' \n'[(len(line)-line.rfind('\n')-1
					+ len (word.split('\n', 1)[0]
						) >= width)],
					word),
				text.split(' ')
				)
		

	#-------------------------
	def __makeScrolls__(self):
	#-------------------------
		"""Generates scroll bars for widgets that want them"""
		scrolls = gtk.ScrolledWindow(gtk.Adjustment(), gtk.Adjustment())
		scrolls.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_AUTOMATIC)
		scrolls.show()
		return scrolls

	#------------------------
	def update(self, parser):
	#------------------------
		"""Updates widgets with a new dbus message.
		 * TableView replaces its old content
		 * DbusView appends text to its old content
		 * TextView replaces its old content
		Note: this will create new objects. The old won't update."""
		self.parser = parser
		self.iterator = self.parser.getTableIterator()
		self.titles = self.parser.getColumnTitles()
		self.message = parser.getOriginalMessage()
		#FIXME \n\n is not windows compatible, use os.linesep
		self.dbusMessages = time.ctime(time.time())+ "\n\n"+ str(self.message)
		self.dbusMessages = self.__formatDbusMessage__(self.dbusMessages, 100)

		# Set up these objects
		self.__initTableView__()
		self.__initDbusView__()
		self.__initTextView__()

		#set up viewing
		self.viewOrder = [self.getTableView(), 
						  self.getTextView(),
						  self.getDbusView()]
		self.currentView = self.viewOrder[0]

	#---------------------------------------------
	def configureEditor(self, editor, textbuffer):
	#---------------------------------------------
		"""Sets up a gtksourceview with the common options I want."""
		languagemanager = gtksourceview.LanguageManager()
		textbuffer.set_language(languagemanager.get_language("sql"))
		textbuffer.set_highlight_syntax(True)
		editor.set_show_line_numbers(True)
		editor.set_wrap_mode(gtk.WRAP_WORD_CHAR)

	#------------------------
	def getCurrentView(self):
	#------------------------
		"""Returns the view that was most recently retrieved"""
		return self.currentView

	#---------------------
	def getNextView(self):
	#---------------------
		"""Returns the next view that is on the self.viewOrder list"""
		self.currentView = self.viewOrder[(self.viewOrder
			                              .index(self.getCurrentView())+1)%3]
		return self.currentView

	#----------------------
	def getTableView(self):
	#----------------------
		"""Returns self.tableView widget"""
		scrolls = self.__makeScrolls__()
		scrolls.add(self.tableView)
		self.currentView = scrolls
		return scrolls

	#---------------------
	def getDbusView(self):
	#---------------------
		"""Returns self.dbusView widget"""
		scrolls = self.__makeScrolls__()
		scrolls.add(self.dbusView)
		self.currentView = scrolls
		return scrolls

	#---------------------
	def getTextView(self):
	#---------------------
		"""Returns self.textView widget"""
		scrolls = self.__makeScrolls__()
		scrolls.add(self.textView)
		self.currentView = scrolls
		return scrolls
