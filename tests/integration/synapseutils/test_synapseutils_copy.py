import uuid
import time

from nose.tools import assert_raises, assert_equals
import re
import json
from synapseclient.core.exceptions import *
from synapseclient import *
from tests.integration import init_module
import synapseutils


def setup(module):
    init_module(module)


# Add Test for UPDATE
# Add test for existing provenance but the orig doesn't have provenance
def test_copy():
    """Tests the copy function"""
    # Create a Project
    project_entity = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)
    # Create two Folders in Project
    folder_entity = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    second_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    third_folder = syn.store(Folder(name=str(uuid.uuid4()), parent=project_entity))
    schedule_for_cleanup(folder_entity.id)
    schedule_for_cleanup(second_folder.id)
    schedule_for_cleanup(third_folder.id)

    # Annotations and provenance
    repo_url = 'https://github.com/Sage-Bionetworks/synapsePythonClient'
    annos = {'test': ['hello_world']}
    prov = Activity(name="test", used=repo_url)
    # Create, upload, and set annotations/provenance on a file in Folder
    filename = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    file_entity = syn.store(File(filename, parent=folder_entity))
    externalURL_entity = syn.store(File(repo_url, name='rand', parent=folder_entity, synapseStore=False))
    syn.set_annotations(Annotations(file_entity, file_entity.etag, annos))
    syn.set_annotations(Annotations(externalURL_entity, externalURL_entity.etag, annos))
    syn.setProvenance(externalURL_entity.id, prov)
    schedule_for_cleanup(file_entity.id)
    schedule_for_cleanup(externalURL_entity.id)
    # ------------------------------------
    # TEST COPY FILE
    # ------------------------------------
    output = synapseutils.copy(syn, file_entity.id, destinationId=project_entity.id)
    output_URL = synapseutils.copy(syn, externalURL_entity.id, destinationId=project_entity.id,
                                   skipCopyAnnotations=True)

    # Verify that our copied files are identical
    copied_ent = syn.get(output[file_entity.id])
    copied_URL_ent = syn.get(output_URL[externalURL_entity.id], downloadFile=False)

    copied_ent_annot = syn.get_annotations(copied_ent)
    copied_url_annot = syn.get_annotations(copied_URL_ent)
    copied_prov = syn.getProvenance(copied_ent)
    copied_url_prov = syn.getProvenance(copied_URL_ent)
    schedule_for_cleanup(copied_ent.id)
    schedule_for_cleanup(copied_URL_ent.id)

    # TEST: set_Provenance = Traceback
    assert_equals(copied_prov['used'][0]['reference']['targetId'], file_entity.id)
    assert_equals(copied_url_prov['used'][0]['reference']['targetId'], externalURL_entity.id)

    # TEST: Make sure copied files are the same
    assert_equals(copied_ent_annot, annos)
    assert_equals(copied_ent.dataFileHandleId, file_entity.dataFileHandleId)

    # TEST: Make sure copied URLs are the same
    assert_equals(copied_url_annot, {})
    assert_equals(copied_URL_ent.externalURL, repo_url)
    assert_equals(copied_URL_ent.name, 'rand')
    assert_equals(copied_URL_ent.dataFileHandleId, externalURL_entity.dataFileHandleId)

    # TEST: Throw error if file is copied to a folder/project that has a file with the same filename
    assert_raises(ValueError, synapseutils.copy, syn, project_entity.id, destinationId=project_entity.id)
    assert_raises(ValueError, synapseutils.copy, syn, file_entity.id, destinationId=project_entity.id)
    assert_raises(ValueError, synapseutils.copy, syn, file_entity.id, destinationId=third_folder.id,
                  setProvenance="gib")
    assert_raises(ValueError, synapseutils.copy, syn, file_entity.id, destinationId=file_entity.id)

    # Test: setProvenance = None
    output = synapseutils.copy(syn, file_entity.id, destinationId=second_folder.id, setProvenance=None)
    assert_raises(SynapseHTTPError, syn.getProvenance, output[file_entity.id])
    schedule_for_cleanup(output[file_entity.id])

    # Test: setProvenance = Existing
    output_URL = synapseutils.copy(syn, externalURL_entity.id, destinationId=second_folder.id, setProvenance="existing")
    output_prov = syn.getProvenance(output_URL[externalURL_entity.id])
    schedule_for_cleanup(output_URL[externalURL_entity.id])
    assert_equals(output_prov['name'], prov['name'])
    assert_equals(output_prov['used'], prov['used'])

    # ------------------------------------
    # TEST COPY LINKS
    # ------------------------------------
    second_file = utils.make_bogus_data_file()
    # schedule_for_cleanup(filename)
    second_file_entity = syn.store(File(second_file, parent=project_entity))
    link_entity = Link(second_file_entity.id, parent=folder_entity.id)
    link_entity = syn.store(link_entity)

    copied_link = synapseutils.copy(syn, link_entity.id, destinationId=second_folder.id)
    old = syn.get(link_entity.id, followLink=False)
    new = syn.get(copied_link[link_entity.id], followLink=False)
    assert_equals(old.linksTo['targetId'], new.linksTo['targetId'])

    schedule_for_cleanup(second_file_entity.id)
    schedule_for_cleanup(link_entity.id)
    schedule_for_cleanup(copied_link[link_entity.id])

    time.sleep(3)

    assert_raises(ValueError, synapseutils.copy, syn, link_entity.id, destinationId=second_folder.id)

    # ------------------------------------
    # TEST COPY TABLE
    # ------------------------------------
    second_project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(second_project.id)
    cols = [Column(name='n', columnType='DOUBLE', maximumSize=50),
            Column(name='c', columnType='STRING', maximumSize=50),
            Column(name='i', columnType='INTEGER')]
    data = [[2.1, 'foo', 10],
            [2.2, 'bar', 20],
            [2.3, 'baz', 30]]

    schema = syn.store(Schema(name='Testing', columns=cols, parent=project_entity.id))
    syn.store(RowSet(schema=schema, rows=[Row(r) for r in data]))

    table_map = synapseutils.copy(syn, schema.id, destinationId=second_project.id)
    copied_table = syn.tableQuery('select * from %s' % table_map[schema.id])
    rows = copied_table.asRowSet()['rows']
    # TEST: Check if all values are the same
    for i, row in enumerate(rows):
        assert_equals(row['values'], data[i])

    assert_raises(ValueError, synapseutils.copy, syn, schema.id, destinationId=second_project.id)

    schedule_for_cleanup(schema.id)
    schedule_for_cleanup(table_map[schema.id])

    # ------------------------------------
    # TEST COPY FOLDER
    # ------------------------------------
    mapping = synapseutils.copy(syn, folder_entity.id, destinationId=second_project.id)
    for i in mapping:
        old = syn.get(i, downloadFile=False)
        new = syn.get(mapping[i], downloadFile=False)
        assert_equals(old.name, new.name)
        assert_equals(old.annotations, new.annotations)
        assert_equals(old.concreteType, new.concreteType)

    assert_raises(ValueError, synapseutils.copy, syn, folder_entity.id, destinationId=second_project.id)
    # TEST: Throw error if excludeTypes isn't in file, link and table or isn't a list
    assert_raises(ValueError, synapseutils.copy, syn, second_folder.id, destinationId=second_project.id,
                  excludeTypes=["foo"])
    assert_raises(ValueError, synapseutils.copy, syn, second_folder.id, destinationId=second_project.id,
                  excludeTypes="file")
    # TEST: excludeType = ["file"], only the folder is created
    second = synapseutils.copy(syn, second_folder.id, destinationId=second_project.id,
                               excludeTypes=["file", "table", "link"])

    copied_folder = syn.get(second[second_folder.id])
    assert_equals(copied_folder.name, second_folder.name)
    assert_equals(len(second), 1)
    # TEST: Make sure error is thrown if foldername already exists

    assert_raises(ValueError, synapseutils.copy, syn, second_folder.id, destinationId=second_project.id)

    # ------------------------------------
    # TEST COPY PROJECT
    # ------------------------------------
    third_project = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(third_project.id)

    mapping = synapseutils.copy(syn, project_entity.id, destinationId=third_project.id)
    for i in mapping:
        old = syn.get(i, downloadFile=False)
        new = syn.get(mapping[i], downloadFile=False)
        if not isinstance(old, Project):
            assert_equals(old.name, new.name)
        assert_equals(old.annotations, new.annotations)
        assert_equals(old.concreteType, new.concreteType)

    # TEST: Can't copy project to a folder
    assert_raises(ValueError, synapseutils.copy, syn, project_entity.id, destinationId=second_folder.id)


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
                        if not i['isPreview']]

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
