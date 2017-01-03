# FileHandlingWithPythonAndSPSSModeler

This procedure uses Python in the backend of the IBM SPSS Modeler, interacting through the modeler API. The modeler stream itself is not included in the repo. Some of the nodes are statically built in the modeler GUI, some are created with python during the loop. Both sorts of nodes are clearly identifiable in the python code through their unique ID. 

The inability of the IBM SPSS Modeler to loop on its own (at least until V17) makes the Python backend necessary. I advice to switch to KNIME for powerful data wrangling, it has loop support and a flexible Java backend.

The procedure is divided into two parts:

- Part 1 ("mimicking the shell")
  - grabs txt files from a ftp server
  - pushes them into a pool directory
  - sorts the files according to the date engraved in their file name
  - pushes the last day's files onto another remote directory
  
- Part 2 ("helping the Modeler to loop")
  - gets the last day's files
  - parses the contents line-by-line
  - updates a db table with the file lines
 
The procedure worked for several years on a daily basis. I believe it is not usable as-is, but can serve as an idea how to handle such tasks in Python purely.
