from enum import Enum

class WriterType(Enum):
	DEBUG = 1
	INFO = 2
	QUERYWRITER = 3

class Writer:
	debug = 0
	info = 0
	queryWriter = 0

	#Method for writing logs
	def send(self,text,type=WriterType.INFO,ending="\n"):
		if(type == WriterType.DEBUG and self.debug):
			print(text,end=ending,flush=True)
		elif(type == WriterType.INFO and self.info):
			print(text,end=ending,flush=True)
		#Need to update to write to file
		elif(type == WriterType.QUERYWRITER and self.queryWriter):
			print(text,end=ending,flush=True)
