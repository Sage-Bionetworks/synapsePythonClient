"""
****
Wiki
****

A Wiki page requires a title, markdown and an owner object and can
alse include images.

~~~~~~~~~~~~~~~
Creating a Wiki
~~~~~~~~~~~~~~~

::

    from synapseclient import Wiki

    entity = syn.get('syn123456')

    content = \"\"\"
    # My Wiki Page

    Here is a description of my **fantastic** project!

    An attached image:
    ${image?fileName=logo.png&align=none}
    \"\"\"

    wiki = Wiki(title='My Wiki Page',
                owner=entity,
                markdown=content,
                attachments=['/path/to/logo.png'])

    wiki = syn.store(wiki)

~~~~~~~~~~~~~~~~
Embedding images
~~~~~~~~~~~~~~~~

Note that in the above example, we've **attached** a logo graphic and embedded it
in the web page.

Figures that are more than just decoration can be stored as Synapse entities
allowing versioning and provenance information to be recorded. This is a better
choice for figures with data behind them.

~~~~~~~~~~~~~~~
Updating a Wiki
~~~~~~~~~~~~~~~

::

    entity = syn.get('syn123456')
    wiki = syn.getWiki(entity)

    content = \"\"\"
    # My Wiki Page

    Here is a description of my **fantastic** project! Let's
    *emphasize* the important stuff.

    An embedded image that is also a Synapse entity:
    ${image?synapseId=syn1824434&align=None&scale=66}

    Now we can track it's provenance and keep multiple versions.
    \"\"\"

    wiki = syn.store(wiki)

~~~~~~~~~~
Wiki Class
~~~~~~~~~~

.. autoclass:: synapseclient.wiki.Wiki
   :members: __init__

~~~~~~~~~~~~
Wiki methods
~~~~~~~~~~~~

 - :py:meth:`synapseclient.Synapse.getWiki`
 - :py:meth:`synapseclient.Synapse.getWikiHeaders`
 - :py:meth:`synapseclient.Synapse.store`
 - :py:meth:`synapseclient.Synapse.delete`

"""

import sys
import json

from synapseclient.exceptions import *
from synapseclient.dict_object import DictObject
from synapseclient.utils import id_of

class Wiki(DictObject):
    """
    Represents a wiki page in Synapse with content specified in markdown.

    :param title:       Title of the Wiki
    :param owner:       Parent Entity that the Wiki will belong to
    :param markdown:    Content of the Wiki
    :param attachments: List of paths to files to attach
    :param fileHandles: List of file handle IDs representing files to be attached
    :param parentWikiId: (optional) For subpages, specify parent wiki page
    """

    __PROPERTIES = ('title', 'markdown', 'attachmentFileHandleIds', 'id', 'etag', 'createdBy', 'createdOn', 'modifiedBy', 'modifiedOn', 'parentWikiId')

    def __init__(self, **kwargs):
        # Verify that the parameters are correct
        if not 'owner' in kwargs:
            raise ValueError('Wiki constructor must have an owner specified')

        # Initialize the file handle list to be an empty list
        if 'attachmentFileHandleIds' not in kwargs:
            kwargs['attachmentFileHandleIds'] = []

        # Move the 'fileHandles' into the proper (wordier) bucket
        if 'fileHandles' in kwargs:
            for handle in kwargs['fileHandles']:
                kwargs['attachmentFileHandleIds'].append(handle)
            del kwargs['fileHandles']

        super(Wiki, self).__init__(kwargs)
        self.ownerId = id_of(self.owner)
        del self['owner']


    def json(self):
        """Returns the JSON representation of the Wiki object."""
        return json.dumps({k:v for k,v in self.iteritems()
                           if k in self.__PROPERTIES})


    def getURI(self):
        """For internal use."""

        return '/entity/%s/wiki/%s' % (self.ownerId, self.id)

    def postURI(self):
        """For internal use."""

        return '/entity/%s/wiki' % self.ownerId

    def putURI(self):
        """For internal use."""

        return '/entity/%s/wiki/%s' % (self.ownerId, self.id)

    def deleteURI(self):
        """For internal use."""

        return '/entity/%s/wiki/%s' % (self.ownerId, self.id)


class WikiAttachment(DictObject):
    """
    Represents a wiki page attachment

    """
    __PROPERTIES = ('contentType', 'fileName', 'contentMd5', 'contentSize')

    def __init__(self, **kwargs):
        super(WikiAttachment, self).__init__(**kwargs)

