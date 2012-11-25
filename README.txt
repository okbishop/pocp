This is a temp readme file while learning the ins and outs of GIT
The basic idea is to use GIT as an auto versioning control tool for the POCP database
This may or may not be a good idea, the idea would be to store changes in the pocp database as they occur in time.
Say for example, 
A cron job will run a python program that downloads and updates the "all of time" pocp data base which is saved to an HDF5 file.
This is run, say, once daily, in a GIT controlled directory.  Part of the CRON job (or python script) will be to also update the
version control.  IN this case, in GIT, this will require a simple "git commit -a" command, which in turn requires an auto text input, about the commit.  My idea is to somehow do this automatically, (I'm sure this is possible), in fact it may be just a matter of "git commit -a 'text here!' "...  where the text here! could, for instance, be a datetime object, ie., datetime(2012,11,26,10,30,0,0) or the like... 

Dave Hume, 26th November, 2012.   

