#!/usr/bin/env python2.7

# To debug this, python2.7 -m pdb myscript.py

import shlex, subprocess, json

#--------------------[ command line argument helper ]-------------------------
def addArguments(parser):
    '''
     Workflow command line argument helper
    '''
    parser.add_argument('--activityName', '-n',
                        help='the name of the Simple Workflow Activity '
                        + 'than can vend work of this type')

    parser.add_argument('--activityVersion', '-v',
                        help='the version of the Simple Workflow Activity '
                        + 'than can vend work of this type',
                        default='0.1')

#--------------------[ factory for workflow instances ]----------------------  
def factory(args):
    '''
    Factory method to create a Workflow instance from command line args
    '''
    return Workflow(args.activityName, args.activityVersion, args.debug)
    
#--------------------[ Synapse ]-----------------------------
class Workflow:
    #-------------------[ Constants ]----------------------
    POLL_FOR_TASK = 'swf-pollforactivitytask --outputFormat json --activityVersions \\\'[ { \\"name\\":\\"%s\\", \\"version\\":\\"%s\\"} ]\\\' --identity commandLineTest'
    RESPOND_TASK_COMPLETED = 'swf-respondactivitytaskcompleted --outputFormat json --taskToken \'%s\' --result \'%s\''
    RESPOND_TASK_FAILED = 'swf-respondactivitytaskfailed --outputFormat json --taskToken \\\'%s\\\''
    
    #-------------------[ Constructor ]----------------------
    def __init__(self, activityName, activityVersion, debug):
        self.activityName = activityName
        self.activityVersion = activityVersion
        self.debug = debug

    def getTask(self):
        pollCmd = Workflow.POLL_FOR_TASK % (self.activityName,
                                            self.activityVersion)
        rawTask = subprocess.check_output(shlex.split("echo " + pollCmd))
        print rawTask
        # task = json.loads(rawTask)
        ### Stash task token in self
        # self.taskToken = task['token']
        self.taskToken = "fake token"
        ### Return the task parameters
        #return task['params']
        task = {}
        if('create' == self.activityName):
            task['datasetId'] = 23
            task['tcgaUrl'] = 'http://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/coad/cgcc/unc.edu/agilentg4502a_07_3/transcriptome/unc.edu_COAD.AgilentG4502A_07_3.Level_2.2.0.0.tar.gz'
        elif('download' == self.activityName):
            task['datasetId'] = 23
            task['tcgaUrl'] = 'http://tcga-data.nci.nih.gov/tcgafiles/ftp_auth/distro_ftpusers/anonymous/tumor/coad/cgcc/unc.edu/agilentg4502a_07_3/transcriptome/unc.edu_COAD.AgilentG4502A_07_3.Level_2.2.0.0.tar.gz'
            task['layerId'] = 996
        return task

    def notifyTaskCompleted(self, result):
        successCmd = Workflow.RESPOND_TASK_COMPLETED % (self.taskToken,
                                                        json.dumps(result))
        print subprocess.check_output(shlex.split("echo " + successCmd))
        
    def notifyTaskFailed(self, error):
        # TODO do something with error
        failureCmd = Workflow.RESPOND_TASK_FAILED % (self.taskToken)
        print subprocess.check_output(shlex.split("echo " + failureCmd))
        
#------- UNIT TESTS -----------------
if __name__ == '__main__':
    import unittest

    class TestWorkflow(unittest.TestCase):

        #def setUp(self):

        def test_fakeTest(self):
            self.assertEqual("foo", "foo")
    
    unittest.main()
