import uuid
import time

from nose.tools import assert_raises, assert_equals, assert_is_none, assert_is_not_none
import re
import json
from synapseclient.core.exceptions import *
from synapseclient import *
from tests import integration
from tests.integration import schedule_for_cleanup
import synapseutils
import synapseclient

def setup(module):
    module.syn = integration.syn
    module.project = integration.project


class TestCopyWiki:

    def setup(self):
        # Create a Project
        self.project_entity = syn.store(Project(name=str(uuid.uuid4())))
        filename = utils.make_bogus_data_file()
        attachname = utils.make_bogus_data_file()
        file_entity = syn.store(File(filename, parent=self.project_entity))

        schedule_for_cleanup(self.project_entity.id)
        schedule_for_cleanup(filename)
        schedule_for_cleanup(file_entity.id)

        # Create mock wiki
        md = """
        This is a test wiki
        =======================
    
        Blabber jabber blah blah boo.
        syn123
        syn456
        """

        wiki = Wiki(owner=self.project_entity, title='A Test Wiki', markdown=md,
                    attachments=[attachname])
        wiki = syn.store(wiki)

        # Create a Wiki sub-page
        subwiki = Wiki(owner=self.project_entity, title='A sub-wiki',
                       markdown='%s' % file_entity.id, parentWikiId=wiki.id)
        self.subwiki = syn.store(subwiki)

        second_md = """
        Testing internal links
        ======================
    
        [test](#!Synapse:%s/wiki/%s)
    
        %s)
        """ % (self.project_entity.id, self.subwiki.id, file_entity.id)

        sub_subwiki = Wiki(owner=self.project_entity, title='A sub-sub-wiki', markdown=second_md,
                           parentWikiId=self.subwiki.id, attachments=[attachname])
        self.sub_subwiki = syn.store(sub_subwiki)

        # Set up the second project
        self.second_project = syn.store(Project(name=str(uuid.uuid4())))
        schedule_for_cleanup(self.second_project.id)

        self.fileMapping = {'syn123': 'syn12345', 'syn456': 'syn45678'}

        self.first_headers = syn.getWikiHeaders(self.project_entity)

    def test_copy_Wiki(self):
        second_headers = synapseutils.copyWiki(syn, self.project_entity.id, self.second_project.id,
                                               entityMap=self.fileMapping)

        mapping = dict()

        # Check that all wikis were copied correctly with the correct mapping
        for index, info in enumerate(second_headers):
            mapping[self.first_headers[index]['id']] = info['id']
            assert_equals(self.first_headers[index]['title'], info['title'])
            if info.get('parentId', None) is not None:
                # Check if parent Ids are mapping correctly in the copied Wikis
                assert_equals(info['parentId'], mapping[self.first_headers[index]['parentId']])

        # Check that all wikis have the correct attachments and have correct internal synapse link/file mapping
        for index, info in enumerate(second_headers):
            # Check if markdown is the correctly mapped
            orig_wikiPage = syn.getWiki(self.project_entity, self.first_headers[index]['id'])
            new_wikiPage = syn.getWiki(self.second_project, info['id'])
            s = orig_wikiPage.markdown
            for oldWikiId in mapping.keys():
                oldProjectAndWikiId = "%s/wiki/%s" % (self.project_entity.id, oldWikiId)
                newProjectAndWikiId = "%s/wiki/%s" % (self.second_project.id, mapping[oldWikiId])
                s = re.sub(oldProjectAndWikiId, newProjectAndWikiId, s)
            for oldFileId in self.fileMapping.keys():
                s = re.sub(oldFileId, self.fileMapping[oldFileId], s)
            assert_equals(s, new_wikiPage.markdown)
            orig_attach = syn.getWikiAttachments(orig_wikiPage)
            new_attach = syn.getWikiAttachments(new_wikiPage)

            orig_file = [i['fileName'] for i in orig_attach
                         if not i['isPreview']]
            new_file = [i['fileName'] for i in new_attach
                        if  not i['isPreview']]

            # check that attachment file names are the same
            assert_equals(orig_file, new_file)

    def test_entitySubPageId_and_destinationSubPageId(self):
        # Test: entitySubPageId
        second_header = synapseutils.copyWiki(syn, self.project_entity.id, self.second_project.id,
                                              entitySubPageId=self.sub_subwiki.id, destinationSubPageId=None,
                                              updateLinks=False, updateSynIds=False, entityMap=None)
        test_ent_subpage = syn.getWiki(self.second_project.id, second_header[0]['id'])

        # Test: No internal links updated
        assert_equals(test_ent_subpage.markdown, self.sub_subwiki.markdown)
        assert_equals(test_ent_subpage.title, self.sub_subwiki.title)

        # Test: destinationSubPageId
        third_header = synapseutils.copyWiki(syn, self.project_entity.id, self.second_project.id,
                                              entitySubPageId=self.subwiki.id,
                                              destinationSubPageId=test_ent_subpage.id, updateLinks=False,
                                              updateSynIds=False, entityMap=None)
        temp = syn.getWiki(self.second_project.id, third_header[0]['id'])
        # There are issues where some title pages are blank.  This is an issue that needs to be addressed
        assert_equals(temp.title, self.subwiki.title)

        assert_equals(temp.markdown, self.subwiki.markdown)

        temp = syn.getWiki(self.second_project.id, third_header[1]['id'])
        assert_equals(temp.title, self.sub_subwiki.title)
        assert_equals(temp.markdown, self.sub_subwiki.markdown)


class TestCopyFileHandles:

    def setup(self):
        # create external file handles for https://www.synapse.org/images/logo.svg,
        project = Project('My uniquely named project 121416')
        project = syn.store(project)

        # create file entity from externalFileHandle
        external_file_handle_request_1 = {
                                           "concreteType": "org.sagebionetworks.repo.model.file.ExternalFileHandle",
                                           "externalURL": "https://www.synapse.org/images/logo.svg",
                                           "fileName": "testExternalFileHandle"
                                         }
        external_response_1 = syn.restPOST('/externalFileHandle', body=json.dumps(external_file_handle_request_1),
                                           endpoint=syn.fileHandleEndpoint)
        self.file_handle_id_1 = external_response_1['id']
        test_entity_1 = File(parent=project)
        test_entity_1.dataFileHandleId = self.file_handle_id_1
        test_entity_1 = syn.store(test_entity_1)
        self.obj_id_1 = str(test_entity_1['id'][3:])

    def test_copy_file_handles(self):
        # define inputs
        file_handles = [self.file_handle_id_1]
        associate_object_types = ["FileEntity"]
        associate_object_ids = [self.obj_id_1]
        copy_results = synapseutils.copyFileHandles(syn, file_handles, associate_object_types, associate_object_ids)
        # assert copy result contains one copy result
        assert_equals(len(copy_results), 1)

