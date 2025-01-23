# import_project
Tool to import/export Pythagora project to a different device

# how to use
**Export project:**

Put import_project.py in *\gpt-pilot\pythagora-core*

Run from the command line:

```
    python import_project.py export [name of project]
```

This will create [name_of_project].db in the *\gpt-pilot\pythagora-core* directory. Share this file with the person who wants to import the project.


**Import project:**

Put [name_of_project].db in *\gpt-pilot\pythagora-core*

Run from the command line:

```
    python import_project.py import [name of project]
```

This will import the project data to your own Pythagora. 



Note that this program only imports data from the pythagora.db file, you will also need to have the project files in your *\gpt-pilot\pythagora-core\workspace* directory.
