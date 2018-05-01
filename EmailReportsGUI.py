import tkinter
from tkinter import *
import email_data_reports
    
top = tkinter.Tk()

Label(top, text='Company Id').grid(row=0, column=0)
E1 = Entry(top, bd = 5)
E1.grid(row=0, column=1)

Label(top, text='Start Date').grid(row=1, column=0)
E2 = Entry(top)
E2.grid(row=1, column=1)

Label(top, text='End Date').grid(row=2, column=0)
E3 = Entry(top)
E3.grid(row=2, column=1)

def runDomain():
    print('Running Domain Report on Company ID ', E1.get())
    email_data_reports.runReport(E1.get(),E2.get(),E3.get(),'d')
    
def runSubject():
    print('Running Subject Report on Company ID ', E1.get())
    email_data_reports.runReport(E1.get(),E2.get(),E3.get(),'s')
    
Button(top, text='Start Subject Report', command=runSubject).grid(row=3,column=0)
Button(top, text='Start Domain Report', command=runDomain).grid(row=3,column=1)

top.mainloop()
