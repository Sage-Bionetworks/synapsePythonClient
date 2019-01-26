import uuid
import time

from nose.tools import assert_raises, assert_equals, assert_is_none, assert_is_not_none
import re

from synapseclient.exceptions import *
from synapseclient import *
from tests import integration
from tests.integration import schedule_for_cleanup
import synapseutils


def setup(module):
    module.syn = integration.syn
    module.project = integration.project


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
    syn.setAnnotations(file_entity, annos)
    syn.setAnnotations(externalURL_entity, annos)
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

    copied_ent_annot = syn.getAnnotations(copied_ent)
    copied_url_annot = syn.getAnnotations(copied_URL_ent)
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
                         if i['concreteType'] != "org.sagebionetworks.repo.model.file.PreviewFileHandle"]
            new_file = [i['fileName'] for i in new_attach
                        if i['concreteType'] != "org.sagebionetworks.repo.model.file.PreviewFileHandle"]

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


def test_copyFileHandleAndchangeFileMetadata():
    project_entity = syn.store(Project(name=str(uuid.uuid4())))
    schedule_for_cleanup(project_entity.id)
    filename = utils.make_bogus_data_file()
    attachname = utils.make_bogus_data_file()
    schedule_for_cleanup(filename)
    schedule_for_cleanup(attachname)
    file_entity = syn.store(File(filename, parent=project_entity))
    schedule_for_cleanup(file_entity.id)
    wiki = Wiki(owner=project_entity, title='A Test Wiki', markdown="testing", 
                attachments=[attachname])
    wiki = syn.store(wiki)
    wikiattachments = syn._getFileHandle(wiki.attachmentFileHandleIds[0])
    # CHECK: Can batch copy two file handles (wiki attachments and file entity)
    copiedFileHandles = synapseutils.copyFileHandles(syn, [file_entity.dataFileHandleId,
                                                           wiki.attachmentFileHandleIds[0]],
                                                     [file_entity.concreteType.split(".")[-1], "WikiAttachment"],
                                                     [file_entity.id, wiki.id],
                                                     [file_entity.contentType, wikiattachments['contentType']],
                                                     [file_entity.name, wikiattachments['fileName']])
    for results in copiedFileHandles['copyResults']:
        assert_is_none(results.get("failureCode"), "NOT FOUND and UNAUTHORIZED failure codes.")

    files = {file_entity.name: {"contentType": file_entity['contentType'], "md5": file_entity['md5']},
             wikiattachments['fileName']: {"contentType": wikiattachments['contentType'],
                                           "md5": wikiattachments['contentMd5']}}
    for results in copiedFileHandles['copyResults']:
        i = results['newFileHandle']
        assert_is_not_none(files.get(i['fileName']), "Filename has to be the same")
        assert_equals(files[i['fileName']]['contentType'], i['contentType'], "Content type has to be the same")
        assert_equals(files[i['fileName']]['md5'], i['contentMd5'], "Md5 has to be the same")

    for results in copiedFileHandles['copyResults']:
        assert_is_none(results.get("failureCode"), "There should not be NOT FOUND and UNAUTHORIZED failure codes.")

    # CHECK: Changing content type and downloadAs
    new_entity = synapseutils.changeFileMetaData(syn, file_entity, contentType="application/x-tar",
                                                 downloadAs="newName.txt")
    schedule_for_cleanup(new_entity.id)
    assert_equals(file_entity.md5, new_entity.md5, "Md5s must be equal after copying")
    fileResult = syn._getFileHandleDownload(new_entity.dataFileHandleId, new_entity.id)
    assert_equals(fileResult['fileHandle']['fileName'], "newName.txt", "Set new file name to be newName.txt")
    assert_equals(new_entity.contentType, "application/x-tar", "Set new content type to be application/x-tar")


def test_copyFileHandles__copying_cached_file_handles():
    num_files = 3
    file_entities = []

    # upload temp files to synapse
    for i in range(num_files):
        file_path = utils.make_bogus_data_file()
        schedule_for_cleanup(file_path)
        file_entities.append(syn.store(File(file_path, name=str(uuid.uuid1()), parent=project)))

    # a bunch of setup for arguments to the function under test
    file_handles = [file_entity['_file_handle'] for file_entity in file_entities]
    file_entity_ids = [file_entity['id'] for file_entity in file_entities]
    content_types = [file_handle['contentType'] for file_handle in file_handles]
    filenames = [file_handle['fileName'] for file_handle in file_handles]

    # remove every other FileHandle from the cache (at even indicies)
    for i in range(num_files):
        if i % 2 == 0:
            syn.cache.remove(file_handles[i]["id"])

    # get the new list of file_handles
    copiedFileHandles = synapseutils.copyFileHandles(syn, file_handles, ["FileEntity"] * num_files, file_entity_ids,
                                                     content_types, filenames)
    new_file_handle_ids = [copy_result['newFileHandle']['id'] for copy_result in copiedFileHandles['copyResults']]

    # verify that the cached paths are the same
    for i in range(num_files):
        original_path = syn.cache.get(file_handles[i]['id'])
        new_path = syn.cache.get(new_file_handle_ids[i])
        if i % 2 == 0:  # since even indicies are not cached, both should be none
            assert_is_none(original_path)
            assert_is_none(new_path)
        else:  # at odd indicies, the file path should have been copied
            assert_equals(original_path, new_path)

