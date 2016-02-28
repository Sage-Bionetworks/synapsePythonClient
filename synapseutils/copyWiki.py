# from __future__ import absolute_import
# from __future__ import division
# from __future__ import print_function
# from __future__ import unicode_literals
# from builtins import str
# from builtins import input

# try:
#     import configparser
# except ImportError:
#     import ConfigParser as configparser

# import collections
# import os, sys, stat, re, json, time
# import base64, hashlib, hmac
# import six

# try:
#     from urllib.parse import urlparse
#     from urllib.parse import urlunparse
#     from urllib.parse import quote
#     from urllib.parse import unquote
# except ImportError:
#     from urlparse import urlparse
#     from urlparse import urlunparse
#     from urllib import quote
#     from urllib import unquote

# try:
#     import urllib.request, urllib.parse, urllib.error
# except ImportError:
#     import urllib

# import requests, webbrowser
# import shutil
# import zipfile
# import mimetypes
import tempfile
import re
# import warnings
# import getpass
# from collections import OrderedDict

import synapseclient
from synapseclient import Wiki


def copyProjectWiki(syn, oldOwnerId,newOwnerId,updateLinks=True,updateSynIds=True,entityMap=None):
    oldOwn = syn.get(oldOwnerId)
    oldWh = syn.getWikiHeaders(oldOwn)  
    newOwn =syn.get(newOwnerId)
    wikiIdMap =dict()
    newWikis=dict()
    for i in oldWh:
        attDir=tempfile.NamedTemporaryFile(prefix='attdir',suffix='')
        #print i['id']
        wiki = syn.getWiki(oldOwn, i.id)
        print('Got wiki %s' % i.id)
        if wiki['attachmentFileHandleIds'] == []:
            attachments = []
        elif wiki['attachmentFileHandleIds'] != []:
            uri = "/entity/%s/wiki/%s/attachmenthandles" % (wiki.ownerId, wiki.id)
            results = syn.restGET(uri)
            file_handles = {fh['id']:fh for fh in results['list']}
            ## need to download an re-upload wiki attachments, ug!
            attachments = []
            tempdir = tempfile.gettempdir()
            for fhid in wiki.attachmentFileHandleIds:
                file_info = syn._downloadWikiAttachment(wiki.ownerId, wiki, file_handles[fhid]['fileName'], destination=tempdir)
                attachments.append(file_info['path'])
        if hasattr(wiki, 'parentWikiId'):
            wNew = Wiki(owner=newOwn, title=wiki.title, markdown=wiki.markdown, attachments=attachments, parentWikiId=wikiIdMap[wiki.parentWikiId])
            wNew = syn.store(wNew)
        else:
            wNew = Wiki(owner=newOwn, title=wiki.title, markdown=wiki.markdown, attachments=attachments)
            wNew = syn.store(wNew)
            parentWikiId = wNew.id
        newWikis[wNew.id]=wNew
        wikiIdMap[wiki.id] =wNew.id

    if updateLinks:
        print("Updating internal links:\n")
        for oldWikiId in wikiIdMap.keys():
            # go through each wiki page once more:
            newWikiId=wikiIdMap[oldWikiId]
            newWiki=newWikis[newWikiId]
            print("\tUpdating internal links for Page: %s\n" % newWikiId)
            s=newWiki.markdown
            # in the markdown field, replace all occurrences of oldOwnerId/wiki/abc with newOwnerId/wiki/xyz,
            # where wikiIdMap maps abc->xyz
            # replace <oldOwnerId>/wiki/<oldWikiId> with <newOwnerId>/wiki/<newWikiId> 
            for oldWikiId2 in wikiIdMap.keys():
                oldProjectAndWikiId = "%s/wiki/%s" % (oldOwnerId, oldWikiId2)
                newProjectAndWikiId = "%s/wiki/%s" % (newOwnerId, wikiIdMap[oldWikiId2])
                s=re.sub(oldProjectAndWikiId, newProjectAndWikiId, s)
            # now replace any last references to oldOwnerId with newOwnerId
            s=re.sub(oldOwnerId, newOwnerId, s)
            newWikis[newWikiId].markdown=s

    if updateSynIds and entityMap != None:
        print("Updating Synapse references:\n")
        for oldWikiId in wikiIdMap.keys():
            # go through each wiki page once more:
            newWikiId = wikiIdMap[oldWikiId]
            newWiki = newWikis[newWikiId]
            print('Updated Synapse references for Page: %s\n' %newWikiId)
            s = newWiki.markdown

            for oldSynId in entityMap.keys():
                # go through each wiki page once more:
                newSynId = entityMap[oldSynId]
                s = re.sub(oldSynId, newSynId, s)
            print("Done updating Synpase IDs.\n")
            newWikis[newWikiId].markdown = s
    
    print("Storing new Wikis\n")
    for oldWikiId in wikiIdMap.keys():
        newWikiId = wikiIdMap[oldWikiId]
        newWikis[newWikiId] = syn.store(newWikis[newWikiId])
        print("\tStored: %s\n",newWikiId)

    newWh = syn.getWikiHeaders(newOwn)
    return(newWh)

