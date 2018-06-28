# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import tempfile
import time
import re
import uuid
import random
from nose.tools import assert_raises, assert_false

from synapseclient.exceptions import *
from synapseclient.evaluation import Evaluation
from synapseclient.entity import File
from synapseclient.annotations import to_submission_status_annotations, from_submission_status_annotations, set_privacy
from synapseclient.team import Team

import integration
from integration import schedule_for_cleanup


def setup(module):
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

        # Add the current user as a participant
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
        assert [p in permissions for p in ['READ', 'CREATE', 'DELETE', 'UPDATE', 'CHANGE_PERMISSIONS',
                                           'READ_PRIVATE_SUBMISSION']]

        # Test getSubmissions with no Submissions (SYNR-453)
        submissions = syn.getSubmissions(ev)
        assert len(list(submissions)) == 0

        # Increase this to fully test paging by getEvaluationSubmissions
        # not to be less than 2
        num_of_submissions = 2

        # Create a bunch of Entities and submit them for scoring
        for i in range(num_of_submissions):
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                filename = f.name
                f.write(str(random.gauss(0, 1)) + '\n')

            f = File(filename, parentId=project.id, name='entry-%02d' % i,
                     description='An entry for testing evaluation')
            entity = syn.store(f)
            syn.submit(ev, entity, name='Submission %02d' % i, submitterAlias='My Team')

        # Score the submissions
        submissions = syn.getSubmissions(ev, limit=num_of_submissions-1)
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
                    assert_false(kvp['isPrivate'])

        # test query by submission annotations
        # These queries run against an eventually consistent index table which is
        # populated by an asynchronous worker. Thus, the queries may remain out
        # of sync for some unbounded, but assumed to be short time.
        attempts = 2
        while attempts > 0:
            try:
                results = syn.restGET("/evaluation/submission/query?query=SELECT+*+FROM+evaluation_%s" % ev.id)
                assert len(results['rows']) == num_of_submissions+1

                results = syn.restGET(
                    "/evaluation/submission/query?query=SELECT+*+FROM+evaluation_%s where bogosity > 200" % ev.id)
                assert len(results['rows']) == num_of_submissions
            except AssertionError as ex1:
                attempts -= 1
                time.sleep(2)
            else:
                attempts = 0

        # Test that we can retrieve submissions with a specific status
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
                # remove team
                testSyn.delete(team)

    # Just deleted it. Shouldn't be able to get it.
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

    # needs to be retried 'cause appending to the search index is asynchronous
    tries = 10
    found_team = None
    while tries > 0:
        try:
            found_team = syn.getTeam(name)
            break
        except ValueError:
            tries -= 1
            if tries > 0:
                time.sleep(1)
    assert team == found_team


