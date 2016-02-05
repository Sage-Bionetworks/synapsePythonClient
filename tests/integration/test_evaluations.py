# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import six
from builtins import str

import tempfile, time, os, re, sys, filecmp, shutil, requests, json
import uuid, random, base64
from datetime import datetime
from nose.tools import assert_raises

try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import synapseclient.client as client
import synapseclient.utils as utils
from synapseclient.exceptions import *
from synapseclient.evaluation import Evaluation
from synapseclient.entity import Project, File
from synapseclient.annotations import to_submission_status_annotations, from_submission_status_annotations, set_privacy
from synapseclient.team import Team, TeamMember

import integration
from integration import schedule_for_cleanup


def setup(module):
    print('\n')
    print('~' * 60)
    print(os.path.basename(__file__))
    print('~' * 60)
    module.syn = integration.syn
    module.project = integration.project


def test_evaluations():
    # Create an Evaluation
    name = 'Test Evaluation %s' % str(uuid.uuid4())
    ev = Evaluation(name=name, description='Evaluation for testing', 
                    contentSource=project['id'], status='CLOSED')
    ev = syn.store(ev)

    try:
        
        # -- Get the Evaluation by name
        evalNamed = syn.getEvaluationByName(name)
        assert ev['contentSource'] == evalNamed['contentSource']
        assert ev['createdOn'] == evalNamed['createdOn']
        assert ev['description'] == evalNamed['description']
        assert ev['etag'] == evalNamed['etag']
        assert ev['id'] == evalNamed['id']
        assert ev['name'] == evalNamed['name']
        assert ev['ownerId'] == evalNamed['ownerId']
        assert ev['status'] == evalNamed['status']
        
        # -- Get the Evaluation by project
        evalProj = syn.getEvaluationByContentSource(project)
        evalProj = next(evalProj)
        assert ev['contentSource'] == evalProj['contentSource']
        assert ev['createdOn'] == evalProj['createdOn']
        assert ev['description'] == evalProj['description']
        assert ev['etag'] == evalProj['etag']
        assert ev['id'] == evalProj['id']
        assert ev['name'] == evalProj['name']
        assert ev['ownerId'] == evalProj['ownerId']
        assert ev['status'] == evalProj['status']
        
        # Update the Evaluation
        ev['status'] = 'OPEN'
        ev = syn.store(ev, createOrUpdate=True)
        assert ev.status == 'OPEN'

        # # Add the current user as a participant
        myOwnerId = int(syn.getUserProfile()['ownerId'])
        syn._allowParticipation(ev, myOwnerId)

        # AUTHENTICATED_USERS = 273948
        # PUBLIC = 273949
        syn.setPermissions(ev, 273948, accessType=['READ'])
        syn.setPermissions(ev, 273949, accessType=['READ'])

        # test getPermissions
        permissions = syn.getPermissions(ev, 273949)
        assert ['READ'] == permissions

        permissions = syn.getPermissions(ev, syn.getUserProfile()['ownerId'])
        assert [p in permissions for p in ['READ', 'CREATE', 'DELETE', 'UPDATE', 'CHANGE_PERMISSIONS', 'READ_PRIVATE_SUBMISSION']]

        # Test getSubmissions with no Submissions (SYNR-453)
        submissions = syn.getSubmissions(ev)
        assert len(list(submissions)) == 0

        # -- Get a Submission attachment belonging to another user (SYNR-541) --
        # See if the configuration contains test authentication
        try:
            config = configparser.ConfigParser()
            config.read(client.CONFIG_FILE)
            other_user = {}
            other_user['username'] = config.get('test-authentication', 'username')
            other_user['password'] = config.get('test-authentication', 'password')
            print("Testing SYNR-541")

            # Login as the test user
            testSyn = client.Synapse(skip_checks=True)
            testSyn.login(email=other_user['username'], password=other_user['password'])
            testOwnerId = int(testSyn.getUserProfile()['ownerId'])

            # Make a project
            other_project = Project(name=str(uuid.uuid4()))
            other_project = testSyn.createEntity(other_project)

            # Give the test user permission to read and join the evaluation
            syn._allowParticipation(ev, testOwnerId)

            # Make a file to submit
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                filename = f.name
                f.write(str(random.gauss(0,1)) + '\n')

            f = File(filename, parentId=other_project.id,
                     name='Submission 999',
                     description ="Haha!  I'm inaccessible...")
            entity = testSyn.store(f)

            ## test submission by evaluation ID
            submission = testSyn.submit(ev.id, entity, submitterAlias="My Nickname")

            # Mess up the cached file so that syn._getWithEntityBundle must download again
            os.utime(filename, (0, 0))

            # Grab the Submission as the original user
            fetched = syn.getSubmission(submission['id'])
            assert os.path.exists(fetched['filePath'])

            # make sure the fetched file is the same as the original (PLFM-2666)
            assert filecmp.cmp(filename, fetched['filePath'])


        except configparser.Error:
            print('Skipping test for SYNR-541: No [test-authentication] in %s' % client.CONFIG_FILE)

        # Increase this to fully test paging by getEvaluationSubmissions
        # not to be less than 2
        num_of_submissions = 2

        # Create a bunch of Entities and submit them for scoring
        print("Creating Submissions")
        for i in range(num_of_submissions):
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                filename = f.name
                f.write(str(random.gauss(0,1)) + '\n')

            f = File(filename, parentId=project.id, name='entry-%02d' % i,
                     description='An entry for testing evaluation')
            entity=syn.store(f)
            syn.submit(ev, entity, name='Submission %02d' % i, submitterAlias='My Team')

        # Score the submissions
        submissions = syn.getSubmissions(ev, limit=num_of_submissions-1)
        print("Scoring Submissions")
        for submission in submissions:
            assert re.match('Submission \d+', submission['name'])
            status = syn.getSubmissionStatus(submission)
            status.score = random.random()
            if submission['name'] == 'Submission 01':
                status.status = 'INVALID'
                status.report = 'Uh-oh, something went wrong!'
            else:
                status.status = 'SCORED'
                status.report = 'a fabulous effort!'
            syn.store(status)

        # Annotate the submissions
        print("Annotating Submissions")
        bogosity = {}
        submissions = syn.getSubmissions(ev)
        b = 123
        for submission, status in syn.getSubmissionBundles(ev):
            bogosity[submission.id] = b
            a = dict(foo='bar', bogosity=b)
            b += 123
            status['annotations'] = to_submission_status_annotations(a)
            set_privacy(status['annotations'], key='bogosity', is_private=False)
            syn.store(status)

        # Test that the annotations stuck
        for submission, status in syn.getSubmissionBundles(ev):
            a = from_submission_status_annotations(status.annotations)
            assert a['foo'] == 'bar'
            assert a['bogosity'] == bogosity[submission.id]
            for kvp in status.annotations['longAnnos']:
                if kvp['key'] == 'bogosity':
                    assert kvp['isPrivate'] == False

        # test query by submission annotations
        # These queries run against an eventually consistent index table which is
        # populated by an asynchronous worker. Thus, the queries may remain out
        # of sync for some unbounded, but assumed to be short time.
        attempts = 2
        while attempts > 0:
            try:
                print("Querying for submissions")
                results = syn.restGET("/evaluation/submission/query?query=SELECT+*+FROM+evaluation_%s" % ev.id)
                print(results)
                assert results[u'totalNumberOfResults'] == num_of_submissions+1

                results = syn.restGET("/evaluation/submission/query?query=SELECT+*+FROM+evaluation_%s where bogosity > 200" % ev.id)
                print(results)
                assert results[u'totalNumberOfResults'] == num_of_submissions
            except AssertionError as ex1:
                print("failed query: ", ex1)
                attempts -= 1
                if attempts > 0: print("retrying...")
                time.sleep(2)
            else:
                attempts = 0

        ## Test that we can retrieve submissions with a specific status
        invalid_submissions = list(syn.getSubmissions(ev, status='INVALID'))
        assert len(invalid_submissions) == 1, len(invalid_submissions)
        assert invalid_submissions[0]['name'] == 'Submission 01'

    finally:
        # Clean up
        syn.delete(ev)
        if 'testSyn' in locals():
            if 'other_project' in locals():
                # Clean up, since the current user can't access this project
                # This also removes references to the submitted object :)
                testSyn.delete(other_project)
            if 'team' in locals():
                ## remove team
                testSyn.delete(team)

    ## Just deleted it. Shouldn't be able to get it.
    assert_raises(SynapseHTTPError, syn.getEvaluation, ev)


def test_teams():
    name = "My Uniquely Named Team " + str(uuid.uuid4())
    team = syn.store(Team(name=name, description="A fake team for testing..."))
    schedule_for_cleanup(team)

    found_team = syn.getTeam(team.id)
    assert team == found_team

    p = syn.getUserProfile()
    found = None
    for m in syn.getTeamMembers(team):
        if m.member.ownerId == p.ownerId:
            found = m
            break

    assert found is not None, "Couldn't find user {} in team".format(p.username)

    ## needs to be retried 'cause appending to the search index is asynchronous
    tries = 10
    found_team = None
    while tries > 0:
        try:
            found_team = syn.getTeam(name)
            break
        except ValueError:
            tries -= 1
            if tries > 0: time.sleep(1)
    assert team == found_team


