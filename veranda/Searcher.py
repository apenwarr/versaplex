#!/usr/bin/python

#------------------------------------------------------------------------------
#                                  Veranda 
#                                 *Searcher
#                              ~--------------~
#
# Original Author: Andrei "Garoth" Thorp <garoth@gmail.com>
#
# Description: This module handles the search function of Veranda (for the
#              sidebar). The concept of the search bar goes as follows:
#                * As you type, it narrows down the list
#                * It will expand the categories, but only if there are few 
#                  matches there
#                * The format of each entry is category/entry (so while the
#                  text says "table_name" it will actually be Table/table_name)
#                * All search queries will be regular expressions.
#
# Notes:
#   Indentation: I use tabs only, 4 spaces per tab.
#------------------------------------------------------------------------------
import re
#------------------------------------------------------------------------------
class Searcher:
#------------------------------------------------------------------------------
	#-------------------------------
	def __init__(self, searchTable):
	#-------------------------------
		"""
		Initializes instance variables
		"""
		self.firstSearchTable = searchTable
		self.types = []

	#----------------------------------
	def __get_regex_ready_list__(self):
	#----------------------------------
		"""
		Takes a list in treeview format ("type"(newrow)"table-name") and
		converts it to regex format ("type/table-name")
		"""
		readyList = []
		for section in self.firstSearchTable:
			tmpList = []
			title = section[0][0]
			for element in section[1:]:
				tmpList.append([title+"/"+element[0]])

			if tmpList != []:
				readyList.append(tmpList)

		return readyList

	#------------------------------------------------
	def __get_treeview_ready_list__(self, regexList):
	#------------------------------------------------
		"""
		Takes a list in regex format ("type/table-name") and converts it back
		to treeview format ("type"(newrow)"table-name")
		"""
		regexList = self.__remove_empties__(regexList)

		readyList = []
		for section in regexList:
			tmpList = []
			title = section[0][0].split("/")[0] # heh
			tmpList.append([title.title()])
			for element in section:
				tmpList.append([element[0].split("/")[1]])
			readyList.append(tmpList)

		return readyList

	#-------------------------------------
	def __remove_empties__(self, newList):
	#-------------------------------------
		"""
		Removes any sections that don't have any contents
		"""
		# Unused for now
		for section in newList:
			if len(section) == 0:
				newList.remove(section)

		return newList

	#----------------------------------
	def __refine_list__(self, pattern):
	#----------------------------------
		"""
		Builds a list that contains only the items that match the pattern
		"""
		searchTable = self.__get_regex_ready_list__()
		regex = re.compile(pattern)

		newList = []
		for section in searchTable:
			tmpList = []
			for element in section:
				if re.search(regex, element[0]) != None:
					tmpList.append(element)

			if tmpList != []:
				newList.append(tmpList)

		return self.__get_treeview_ready_list__(newList)

	#----------------------
	def find(self, pattern):
	#----------------------
		"""
		Takes a search pattern (a regex) and returns a new list that has only
		the elements that match that pattern.
		"""
		list = self.__refine_list__(pattern)
		return list

	#--------------------------
	def getOriginalTable(self):
	#--------------------------
		"""
		Returns the table that the searcher was originally given.
		"""
		return self.firstSearchTable
