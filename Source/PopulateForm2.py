from PrioritizationSpatial import processSpatial, uniqueProjectID
import pyodbc, pickle


#Get the answerID for a particular question, value (1=yes, 2=no)
def getAnswerID(curs, quesID, value):
   curs.execute("Select ID from Answers where QuestionID = ? and Value = ?", quesID, value)
   row = curs.fetchone()
   answerID = row[0]
   return answerID 

#Create a dictionary where key, value pair = mtpID, localID
def getLocalIDs(curs, projIDs):
   dictLocalIDs = {}
   for item in projIDs:
        curs.execute("Select ID from Projects where MTPID = ?", item)
        row = curs.fetchone()
        if row is None:
            print item
        else:
            localID = row[0]
            dictLocalIDs[item] = int(localID)
   return dictLocalIDs

        
       
def updateDatabase():
    projIDs = uniqueProjectID()
    #results = processSpatial()
   # projIDs = [4001, 4002, 4004]
    #results = {'98': [4001, 4004], '111' : [4002]}

    # Do we have pre-cached results?
    try:
        pkfile = open("results.pickle", "rb")
        results = pickle.load(pkfile)
        pkfile.close()
    except:
        results = processSpatial()
        pkfile = open("results.pickle", "wb")
        pickle.dump(results, pkfile)
        pkfile.close()

    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=SQL2008\PSRCSQL;DATABASE=shrp2c18final;UID=coe;PWD=Boarder2040')
    cursor = cnxn.cursor()
   
    #get a dictionary where key, value pair = mtpID, localID
    localIDs = getLocalIDs(cursor, projIDs)
    #print localIDs
    
    #loop through results dictionary
    for key, value in results.items():
        
        #get the questionID
        questionID = key
        print "Question # %s" %(questionID)
        #get Yes & No ID
        yesID = getAnswerID(cursor, questionID, 1)
        noID = getAnswerID(cursor, questionID, 2)
       
        #Get a set of projects that satisfy the question(yes) and a set that do not (no)
        yesSet = set(value)
        noSet = set(projIDs)-yesSet
        #print yesSet
        #print noSet 
        rows = []
        for item in yesSet:
           
            if item in localIDs:
                localID = localIDs[item]
            #print "%s, %s, %s," % (yesID, localID, questionID)
                row = [yesID,'Yes', localID, questionID]
                rows.append(row)
                #cursor.execute("Update Responses set AnswerID = ?, Text = 'Yes' where ProjectID = ? and QuestionID = ?", yesID, localID, questionID)
                #cnxn.commit()
            else: 
                print item
        for item in noSet:
            if item in localIDs:
                localID = localIDs[item]
                row = [noID,'No', localID, questionID]
                rows.append(row)
                #print "%s, %s, %s," % (noID, localID, questionID)
                #cursor.execute("Update Responses set AnswerID = ?, Text = 'No' where ProjectID = ? and QuestionID = ?", noID, localID, questionID)
                #cnxn.commit()
            else: 
                print item
        print "%s, %s, %s," % (noID, localID, questionID)
        cursor.executemany("Update Responses set AnswerID = ?, Text = ? where ProjectID = ? and QuestionID = ?", rows)
        cnxn.commit()
    cnxn.close()
updateDatabase()

