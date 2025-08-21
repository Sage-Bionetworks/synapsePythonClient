# PyInstaller hook for uvicorn
# This helps ensure all uvicorn modules are properly included

from PyInstaller.utils.hooks import collect_all, collect_submodules

# Collect all uvicorn modules
datas, binaries, hiddenimports = collect_all('uvicorn')

# Add specific hidden imports that are often missed
hiddenimports += [
    'uvicorn.main',
    'uvicorn.server',
    'uvicorn.config',
    'uvicorn.lifespan',
    'uvicorn.lifespan.on',
    'uvicorn.loops',
    'uvicorn.loops.auto',
    'uvicorn.loops.asyncio',
    'uvicorn.loops.uvloop',
    'uvicorn.protocols',
    'uvicorn.protocols.http',
    'uvicorn.protocols.http.auto',
    'uvicorn.protocols.http.h11_impl',
    'uvicorn.protocols.http.httptools_impl',
    'uvicorn.protocols.websockets',
    'uvicorn.protocols.websockets.auto',
    'uvicorn.protocols.websockets.websockets_impl',
    'uvicorn.protocols.websockets.wsproto_impl',
    'uvicorn.supervisors',
    'uvicorn.supervisors.basereload',
    'uvicorn.supervisors.statreload',
    'uvicorn.supervisors.watchgodreload',
    'uvicorn.supervisors.watchfilesreload',
]

# Add all submodules
hiddenimports += collect_submodules('uvicorn')
