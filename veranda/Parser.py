#!/usr/bin/python

#------------------------------------------------------------------------------
#                                  Veranda 
#                                  *Parser
#                                 *Iterator
#                              ~--------------~
#
# Original Author: Andrei "Garoth" Thorp <garoth@gmail.com>
#
# Description: This module takes in python-formatted DBus-SQL message and 
#              wraps it in an object for easy usage. One of the aims is for 
#              this then to be easily usable with gtk's TreeView table 
#              system. 
#
# Notes:
#   Indentation: I use tabs only, 4 spaces per tab.
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class Parser:
#------------------------------------------------------------------------------
	""" An abstraction layer for DBus-SQL messages. This module makes it easy
	to extract data from messages that Versaplex replies with over dbus."""
	#---------------------------	
	def __init__(self,message):	
	#---------------------------
		"""Constructor: get the message, control the workflow"""
		self.message = message 		# The original message
		self.types = [] 			# The types of each column
		self.titles = [] 			# The titles of each column
		self.table = [] 			# The table	(columns/rows for data)

		self.__parseColumnTypes__()
		self.__parseColumnTitles__()
		self.__parseTableRows__()

	#------------------------------	
	def __parseColumnTypes__(self):
	#------------------------------
		"""Extracts the types of the columns from self.message and puts them
		into the list self.types"""
		segmentOne = self.message[1]
		signature = str(segmentOne.signature)
		signature = signature.strip("()")

		# The signature lists what each column's type is. The following
		# maps the signature characters to Python types.
		types = {
				"s":str, 	"b":bool, 	"o":object,
				"n":int, 	"i":int, 	"x":long,
				"q":int, 	"u":int, 	"t":long,
				"d":float
				}

		for char in signature:
			if char in types:
				self.types.append(types[char])
			else:
				print "Column Type unknown, defaulting to string."
				self.types.append(str) 

	#-------------------------------	
	def __parseColumnTitles__(self):
	#-------------------------------
		"""Extracts the names of the columns from self.message and puts them
		in as the first row of self.table"""
		segmentZero = self.message[0]
		for struct in segmentZero:
			self.titles.append(str(struct[1]))

	#----------------------------	
	def __parseTableRows__(self):
	#----------------------------
		"""Extracts the table rows from self.message and puts them into 
		self.table"""
		segmentOne = self.message[1]
		for struct in segmentOne:
			row = []
			for x in range(self.numColumns()):
				row.append(str(struct[x]))
			self.table.append(row)
			
		segmentTwo = self.message[2]
		y = 0
		for struct in segmentTwo:
			row = []
			for x in range(self.numColumns()):
				if struct[x]:
					self.table[y][x] = ""
			y += 1

	#--------------------	
	def numColumns(self):
	#--------------------
		"""Returns the number of columns in the data"""
		return len(self.types)

	#-----------------	
	def numRows(self):
	#-----------------
		"""Returns the number of rows of data (not the title row)"""
		return len(self.table)

	#----------------------------
	def getOriginalMessage(self):
	#----------------------------
		"""Returns the original dbus message"""
		return self.message

	#-------------------------	
	def getColumnTitles(self):
	#-------------------------
		"""Returns a list of the titles of the columns"""
		return self.titles

	#------------------------	
	def getColumnTypes(self):
	#------------------------	
		"""Returns the types of the columns (returns a list of types, like
		"str" or "int")"""
		return self.types

	#--------------------------------
	def getColumnTypesAsString(self):
	#--------------------------------
		"""This returns a list of "str" types with the number of elements equal
		to the number of columns in the table. This is useful for declaring a
		gtk.TreeView where every column will display text."""
		list = []
		for x in range(self.numColumns()):
			list.append(str)
		return list

	#------------------
	def getTable(self):
	#------------------
		"""Returns the body of the table"""
		return self.table

	#--------------------------	
	def getTableIterator(self):
	#--------------------------
		"""Returns a simple iterator for self.table"""
		return Iterator(self.table)

#------------------------------------------------------------------------------
class Iterator:
#------------------------------------------------------------------------------
	"""A simple iterator made for returning one table row at a time"""
	#------------------------	
	def __init__(self,table):
	#------------------------
		"""Starts up the instance variables"""
		self.table = table 		# The data
		self.currentRow = -1 	# The location of the iterator by row number

	#---------------------------	
	def setCurrent(self,number):
	#---------------------------
		"""Lets you set the row that the iterator is currently on. Remember,
		though that getNext() will return the next element, not the current"""
		self.currentRow = number
	
	#-----------------	
	def getNext(self):
	#-----------------
		"""Returns the next row in the table"""
		if self.hasNext():
			self.currentRow += 1
			return self.table[self.currentRow]
		else:
			raise IndexError, "Data table from Dbus has no next row"

	#-----------------
	def hasNext(self):
	#-----------------
		"""Checks if there is a next row in the table"""
		if self.currentRow+1 <= len(self.table)-1:
			return True
		else:
			return False

