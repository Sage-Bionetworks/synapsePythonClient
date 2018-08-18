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
from nose.tools import assert_raises, assert_false, assert_is_not_none, assert_true, assert_equals, assert_in

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
        assert_equals(ev['contentSource'], evalNamed['contentSource'])
        assert_equals(ev['createdOn'], evalNamed['createdOn'])
        assert_equals(ev['description'], evalNamed['description'])
        assert_equals(ev['etag'], evalNamed['etag'])
        assert_equals(ev['id'], evalNamed['id'])
        assert_equals(ev['name'], evalNamed['name'])
        assert_equals(ev['ownerId'], evalNamed['ownerId'])
        assert_equals(ev['status'], evalNamed['status'])
        
        # -- Get the Evaluation by project
        evalProj = syn.getEvaluationByContentSource(project)
        evalProj = next(evalProj)
        assert_equals(ev['contentSource'], evalProj['contentSource'])
        assert_equals(ev['createdOn'], evalProj['createdOn'])
        assert_equals(ev['description'], evalProj['description'])
        assert_equals(ev['etag'], evalProj['etag'])
        assert_equals(ev['id'], evalProj['id'])
        assert_equals(ev['name'], evalProj['name'])
        assert_equals(ev['ownerId'], evalProj['ownerId'])
        assert_equals(ev['status'], evalProj['status'])
        
        # Update the Evaluation
        ev['status'] = 'OPEN'
        ev = syn.store(ev, createOrUpdate=True)
        assert_equals(ev.status, 'OPEN')

        # Add the current user as a participant
        myOwnerId = int(syn.getUserProfile()['ownerId'])
        syn._allowParticipation(ev, myOwnerId)

        # AUTHENTICATED_USERS = 273948
        # PUBLIC = 273949
        syn.setPermissions(ev, 273948, accessType=['READ'])
        syn.setPermissions(ev, 273949, accessType=['READ'])

        # test getPermissions
        permissions = syn.getPermissions(ev, 273949)
        assert_equals(['READ'], permissions)

        permissions = syn.getPermissions(ev, syn.getUserProfile()['ownerId'])
        for p in ['READ', 'CREATE', 'DELETE', 'UPDATE', 'CHANGE_PERMISSIONS', 'READ_PRIVATE_SUBMISSION']:
            assert_in(p, permissions)

        # Test getSubmissions with no Submissions (SYNR-453)
        submissions = syn.getSubmissions(ev)
        assert_equals(len(list(submissions)), 0)

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
            assert_true(re.match('Submission \d+', submission['name']))
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
            assert_equals(a['foo'], 'bar')
            assert_equals(a['bogosity'], bogosity[submission.id])
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
                assert_equals(len(results['rows']), num_of_submissions+1)

                results = syn.restGET(
                    "/evaluation/submission/query?query=SELECT+*+FROM+evaluation_%s where bogosity > 200" % ev.id)
                assert_equals(len(results['rows']), num_of_submissions)
            except AssertionError as ex1:
                attempts -= 1
                time.sleep(2)
            else:
                attempts = 0

        # Test that we can retrieve submissions with a specific status
        invalid_submissions = list(syn.getSubmissions(ev, status='INVALID'))
        assert_equals(len(invalid_submissions), 1, len(invalid_submissions))
        assert_equals(invalid_submissions[0]['name'], 'Submission 01')

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
    assert_equals(team, found_team)

    p = syn.getUserProfile()
    found = None
    for m in syn.getTeamMembers(team):
        if m.member.ownerId == p.ownerId:
            found = m
            break

    assert_is_not_none(found, "Couldn't find user {} in team".format(p.userName))

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
    assert_equals(team, found_team)


