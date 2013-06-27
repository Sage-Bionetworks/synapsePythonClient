************
Synapse Wiki
************
A Wiki page requires a title, markdown and an owner object.  
For example::

    entity = syn.get('syn123456')

    content = """
    My Wiki Page
    ============

    A **bold** statement in markdown!

    An attached image:
    ${image?fileName=foo.png&align=none}
    """

    wiki = Wiki(title='My Wiki Page', owner=entity, markdown=content)

    ## temporary work-around
    fileHandle = syn._uploadFileToFileHandleService('/path/to/foo.png')
    wiki.attachmentFileHandleIds = [fileHandle['id']]

    wiki = syn.store(wiki)
   
~~~~
Wiki
~~~~

.. autoclass:: synapseclient.wiki.Wiki
   :members: