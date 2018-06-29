from enum import Enum

class WriterType(Enum):
	DEBUG = 1
	INFO = 2
	QUERYWRITER = 3

class Writer:
	debug = 0
	info = 0
	queryWriter = 0

	def send(text,type=WriterType.INFO,ending="\n"):
		if(type == WriterType.DEBUG and debug):
			print(text,end=ending,flush=True)
		elif(type == WriterType.INFO and info):
			print(text,end=ending,flush=True)
		elif(type == WriterType.QUERYWRITER and queryWriter):
			print(text,end=ending,flush=True)
