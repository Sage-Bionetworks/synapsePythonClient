"""
****
Wiki
****

A Wiki page requires a title, markdown and an owner object and can
also include images.

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

Note that in the above example, we've **attached** a logo graphic and embedded it in the web page.

Figures that are more than just decoration can be stored as Synapse entities allowing versioning and provenance
information to be recorded. This is a better choice for figures with data behind them.

~~~~~~~~~~~~~~~
Updating a Wiki
~~~~~~~~~~~~~~~

::

    entity = syn.get('syn123456')
    wiki = syn.getWiki(entity)

    wiki.markdown = \"\"\"
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

import os
import json

from synapseclient.core.models.dict_object import DictObject
from synapseclient.core.utils import id_of


class Wiki(DictObject):
    """
    Represents a wiki page in Synapse with content specified in markdown.

    :param title:           Title of the Wiki
    :param owner:           Parent Entity that the Wiki will belong to
    :param markdown:        Content of the Wiki (cannot be defined if markdownFile is defined)
    :param markdownFile:    Path to file which contains the Content of Wiki (cannot be defined if markdown is defined)
    :param attachments:     List of paths to files to attach
    :param fileHandles:     List of file handle IDs representing files to be attached
    :param parentWikiId:    (optional) For sub-pages, specify parent wiki page
    """

    __PROPERTIES = ('title', 'markdown', 'attachmentFileHandleIds', 'id', 'etag', 'createdBy', 'createdOn',
                    'modifiedBy', 'modifiedOn', 'parentWikiId')

    def __init__(self, **kwargs):
        # Verify that the parameters are correct
        if 'owner' not in kwargs:
            raise ValueError('Wiki constructor must have an owner specified')

        # Initialize the file handle list to be an empty list
        if 'attachmentFileHandleIds' not in kwargs:
            kwargs['attachmentFileHandleIds'] = []

        # update the markdown
        self.update_markdown(kwargs.pop('markdown', None), kwargs.pop('markdownFile', None))

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
        return json.dumps({k: v for k, v in self.items() if k in self.__PROPERTIES})

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

    def update_markdown(self, markdown=None, markdown_file=None):
        """
        Updates the wiki's markdown. Specify only one of markdown or markdown_file
        :param markdown:        text that will become the markdown
        :param markdown_file:   path to a file. Its contents will be the markdown
        """
        if markdown and markdown_file:
            raise ValueError("Please use only one argument: markdown or markdownFile")

        if markdown_file:
            # pop the 'markdownFile' kwargs because we don't actually need it in the dictionary to upload to synapse
            markdown_path = os.path.expandvars(os.path.expanduser(markdown_file))
            if not os.path.isfile(markdown_path):
                raise ValueError(markdown_file + "is not a valid file")
            with open(markdown_path, 'r') as opened_markdown_file:
                markdown = opened_markdown_file.read()

        self['markdown'] = markdown


class WikiAttachment(DictObject):
    """
    Represents a wiki page attachment

    """
    __PROPERTIES = ('contentType', 'fileName', 'contentMd5', 'contentSize')

    def __init__(self, **kwargs):
        super(WikiAttachment, self).__init__(**kwargs)

