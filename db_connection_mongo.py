#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient  
def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    client = MongoClient('localhost', 27017)  # Adjust host and port as needed
    db = client['documentDB']  # Assuming the database name is documentDB
    col = db['documents']  # Assuming the collection name is documents
    return col

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    terms = docText.lower().split()

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here
    term_dict = {}
    for term in terms:
        if term in term_dict:
            term_dict[term]['count'] += 1
        else:
            term_dict[term] = {'count': 1, 'num_chars': len(term)}

    #Producing a final document as a dictionary including all the required fields
    # --> add your Python code here
    terms_list = [{'term': key, 'count': value['count'], 'num_chars': value['num_chars']} for key, value in term_dict.items()]
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": terms_list
    }
    # Insert the document
    # --> add your Python code here
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({ "_id": docId })

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    # --> add your Python code here
    index = {}

    # Retrieve all documents
    documents = col.find()

    # Iterate over each document
    for doc in documents:
        docTitle = doc['title']
        for term_info in doc['terms']:
            term = term_info['term']
            count = term_info['count']
            
            # Build the index entry for each term
            if term in index:
                index[term] += f", {docTitle}:{count}"
            else:
                index[term] = f"{docTitle}:{count}"

    # Output the inverted index sorted by term
    sorted_index = dict(sorted(index.items()))
    return sorted_index